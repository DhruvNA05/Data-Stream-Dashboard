from confluent_kafka import Consumer
import json

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'coinbase_cluster'
}

with Consumer(consumer_config) as consumer:
    consumer.subscribe(["crypto-prices"])

    while True:
        raw_msg = consumer.poll(1)
        if(raw_msg) is None:
            print('Waiting...')
            continue
        else:
            msg = json.loads(raw_message)
            key = msg.get('symbol')
            value = msg.get('price')
            time = msg.get('time')
        print (value)

