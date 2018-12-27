import csv
import socket
from confluent_kafka import Consumer, KafkaError

settings = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

c = Consumer(settings)

c.subscribe(['twitter_trend'])

file_cnt = 0

host = "localhost"
port = 7777
s = socket.socket()
s.bind((host, port))
print("Listening on port:" + str(port))

s.listen(5)

connection, client_address = s.accept()

print("Received request from: " + str(client_address))

while file_cnt < 10:

    with open('/home/nitin/projects/twitterdm/data/trends' + str(file_cnt) + '.json', 'a', newline='') as file_write:
        rec_count = 0

        try:
            while True:
                msg = c.poll(0.1)
                if msg is None:
                    continue
                elif not msg.error():
                    print('Received message: {0}'.format(msg.value()))
                    #file_write.write(msg.value().decode('UTF-8'))
                    connection.send(msg.value())
                    rec_count += 1
                    print(rec_count)
                    if rec_count > 1000:
                        break
                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
                else:
                    print('Error occured: {0}'.format(msg.error().str()))

        except KeyboardInterrupt:
            pass

        #finally:
        #    c.close()
    file_cnt += 1
