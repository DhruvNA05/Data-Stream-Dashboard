from confluent_kafka import Consumer
import json

def main():
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'coinbase_cluster',
        'auto.offset.reset': 'latest' # Start reading from the latest offset
    }

    with Consumer(consumer_config) as consumer:
        consumer.subscribe(["crypto-prices"]) # Connect to the topic

        while True:
            raw_msg = consumer.poll(1)
            if(raw_msg) is None:
                print('Waiting...')
                continue
            elif raw_msg.error() is not None:
                print('ERROR:', raw_msg.error())
                continue
            else:
                msg = json.loads(raw_msg.value().decode('utf-8')) # Interpret the JSON, first decode from utf then read
                key = msg.get('symbol')
                value = msg.get('price')
                time = msg.get('time')
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('\nTERMINATED CONSUMER')
    