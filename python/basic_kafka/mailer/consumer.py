import json

from confluent_kafka import Consumer

from send_mail import send_mail


def main():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'send_mail',
        'default.topic.config': {
            'auto.offset.reset': 'earliest'
        }
    })
    consumer.subscribe(['sent-mail'])

    while(True):
        message = consumer.poll(1.0)
        if message:
            print('message', message.value())
            output = json.loads(message.value())
            email = output['email']
            subject = output['subject']
            text = output['text']

            send_mail(email, subject, text)



if __name__ == '__main__':
    main()
