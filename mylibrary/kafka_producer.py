# 27 Dec 2018 | Kafka | Simple Producer
 
from confluent_kafka import Producer

class KafkaProducer():

    def acked(self, err, msg):
        if err is not None:
            print("Failed to deliver message: {0}: {1}".format(msg.value(), err.str()))
        else:
            print("Message produced: {0}".format(msg.value()))

    def produce_msg(self, topic, data):
        p = Producer({'bootstrap.servers': 'localhost:9092'})

        try:
            p.produce(topic, data, callback=self.acked)
            p.poll(0.5)

            #for val in range(1, 1000):
                #p.produce(topic, data.format(val), callback=acked)
                #p.produce(topic, data, callback=self.acked)
                #p.poll(0.5)

        except KeyboardInterrupt:
            pass

        p.flush(30)
