from TrafficSimulator import TrafficGenerator
from kafka import KafkaProducer
import time

# create decoder object from pcap file
pkt_obj = enumerate(TrafficGenerator('/home/cloudera-master/2018-04-05_09-36-26_Default_SPW-4.pcap'))

# create Kafka producer
producer = KafkaProducer(bootstrap_servers='cldmaster.local:9092', # batch_size=16384, linger_ms=5,
                         value_serializer=lambda v: v)

# topic must be the same for the Producer and Consumer
topic1 = "CCSDS"
topic2 = "SpaceWire"

for i in range(10):
    # iterate spw packets one by one
    packet = next(pkt_obj)[1]
    # send each pcap packet in bytes format
    if packet is not None:
        producer.send(topic1, packet)
        # producer.send(topic2, packet)
    else:
        print('None')

    # time.sleep(0.001)

producer.flush()
