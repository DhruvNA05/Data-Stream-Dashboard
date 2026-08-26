# Producer Script
import asyncio, logging, json, os, websockets
from datetime import datetime, timezone
from confluent_kafka import Producer

# config = {
#    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
# }

#Logging
logging.basicConfig(level = logging.INFO,
                    format = '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("producer")

coinbase_endpoint_url = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_IDS = ["BTC-USD", "ETH-USD"]
KAFKA_TOPIC = "crypto-prices"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Coinbase subscription request
COINBASE_SUBSCRIPTION_MSG = {
    "type": "subscribe",
    "product_ids": PRODUCT_IDS,
    "channels": ["ticker"]
}

# Configures the producer
def build_kafka_producer() -> Producer:
    return Producer ({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 5,
        "linger.ms": 10,
    })

# Delivery Report
def delivery_report(err, msg) -> None:
    if err is not None:
        logger.error("Delivery failed for %s: %s", msg.key(), err)
    else:
        logger.debug(
            "Sent to %s [partition %d] @ offset %d",
            msg.topic(), msg.partition(), msg.offset(),
        )

# Connect/Subscribe and Stream
async def stream_ticks(producer: Producer) -> None:
    async with websockets.connect(
        coinbase_endpoint_url, ping_interval=20, ping_timeout=20
    ) as ws:
        await ws.send(json.dumps(COINBASE_SUBSCRIPTION_MSG))
        logger.info("Subscribed to %s on ticker channel", PRODUCT_IDS)

        # Loop and receive data
        async for raw_msg in ws:
            msg = json.loads(raw_msg) # Parse JSON into Python dict
            msg_type = msg.get("type") # Get message type

            if msg_type == "subscriptions": # Confirms subscription
                logger.info("Subscription succesful %s", msg)
                continue
            if msg_type == "error": # Error
                logger.info("Subscription error %s", msg)
                continue
            if msg_type != "ticker": # Make sure we only receive from the ticker channel, if it isn't ignore it
                continue

            symbol = msg.get("product_id")
            price = msg.get("price")
            time = msg.get("time")

            logger.info("Raw ticker: symbol=%s price %s", symbol, price)

            if symbol is None or price is None:
                continue

            tick = { # JSON shape and structure use in consumer
                "symbol": symbol,
                "price": float(price),
                "time": time or datetime.now(timezone.utc).isoformat(),
            }

            # Send the message to the cluster
            producer.produce(
                KAFKA_TOPIC,
                key = symbol,
                value = json.dumps(tick),
                callback = delivery_report
            )
            producer.poll(0)
            logger.info("Produced: %s", tick)

async def main() -> None:
    producer = build_kafka_producer()
    backoff = 1
    try:
        while True:
            try:
                await stream_ticks(producer)
            except (websockets.ConnectionClosed, OSError) as e:
                logger.warning(
                    "Websocket dropped (%s), reconnecting in %ss", e, backoff
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
            else:
                backoff = 1
    finally:
        producer.flush(10)

if __name__ == "__main__":
    asyncio.run(main())