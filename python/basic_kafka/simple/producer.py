from confluent_kafka import Producer


def main():
    producer = Producer({
        'bootstrap.servers': 'localhost:9092' #if more than one node, it will have to register here
    })

    for i in range(10):
        producer.produce('test-topic', ('mils' + str(i))) #receive data in byete array
    producer.flush()      #asynchronous sent

if __name__ == '__main__':
    main()
    print('Completed')
