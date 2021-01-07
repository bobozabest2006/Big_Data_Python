from TrafficSimulator import TrafficGenerator
from kafka import KafkaProducer

# create decoder object from pcap file
pkt_obj = enumerate(TrafficGenerator('/home/cloudera-master/2018-04-05_09-36-26_Default_SPW-4.pcap'))

# create Kafka producer
producer = KafkaProducer(bootstrap_servers='cldmaster.local:9092',  # batch_size=16384, linger_ms=5,
                         value_serializer=lambda v: v)

# topic must be the same for the Producer and Consumer
topic = "TM_RAW"

# counter = itertools.count(start=0, step=1)

for _ in range(10):
# for _ in range(863928):
    # iterate spw packets one by one
    packet = next(pkt_obj)[1]
    # send each pcap packet in bytes format
    if packet is not None:
        producer.send("TM_RAW", packet)
        # print(packet)
    else:
        print('None')

    # print(i)
    # time.sleep(0.01)

producer.flush()