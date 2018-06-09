from confluent_kafka import Consumer


def main():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'ProntoTools-2',
        'enable.auto.commit': False,
        'default.topic.config': {
            'auto.offset.reset': 'earliest'
        }
    })
    consumer.subscribe(['test-topic'])

    while(True):
        message = consumer.poll(1.0)
        if message:
            print('message comming', message)
            print('message value', message.value())


if __name__ == '__main__':
    main()
