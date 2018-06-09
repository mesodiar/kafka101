import json
import asyncio

from confluent_kafka import Producer


producer = Producer({
    'bootstrap.servers': 'localhost:9092' #if more than one node, it will have to register here
})

def delivery_callback(future):
    def __delivery_callback(err, msg):
        if err:
            future.set_exception(err)
        elif msg:
            future.set_result(msg)
    return __delivery_callback

async def produce(email: str, subject: str, text: str):
    future = asyncio.Future()
    input_data = {
        'email': email,
        'subject': subject,
        'text': text
    }
    producer.produce('sent-mail', json.dumps(input_data), key=email, callback=delivery_callback(future))
    data = await future
    #producer.flush() #don't have to flush

    print(data)

    print('Produce Step Completed')
