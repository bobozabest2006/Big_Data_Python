import time
import os
import decimal
import arrow
import pprint
import datetime

import happybase
from influxdb import InfluxDBClient
from influxdb import SeriesHelper

from pyspark import SparkContext
from pyspark.storagelevel import StorageLevel
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# ------------------- INFLUXDB CONNECTION START --------------------
# create influxdb client
influx_client = InfluxDBClient(host='cldmaster.local', port=9998, username='root',
                               password='toor')

# get list of current databases
db_name_list = influx_client.get_list_database()


def json_body(name, timestamp_ns, port):
    utc_time = datetime.datetime.utcnow()

    body = [
        {
            "measurement": str(name),
            "time": utc_time,
            "fields": {
                "port": port,
                "timestamp": str(timestamp_ns // 10 ** 3),
                "tm": utc_time.timestamp()
            }
        }
    ]

    return body
# ------------------- INFLUXDB CONNECTION END --------------------


# ------------------- HBASE CONNECTION START --------------------
# Create connection to Hbase
connection = happybase.Connection('cldmaster.local')
# Open connection
# connection.open()

table_name = 'Details'
# Connect to table
table = connection.table(table_name)
# Create batch object to send multiple command at ones
bat = table.batch(transaction=True)

# # Empty HBase database
# for key, data in table.scan():
#     row = bat.delete(str(key.decode('utf-8')))
# bat.send()


# ------------------- HBASE CONNECTION END --------------------


# Bytes format decoder
def iso_8859_1(s):
    if s is None:
        return None
    return s.decode('ISO-8859-1')


# Decoder function for PCAP file bytes
def decodePacket(p, codec="ISO-8859-1"):
    if p is not None:
        # -------------------------------- PCAP RECORD HEADER -------------------------------- #
        # Set PCAP Record length
        len_pcap_rec = 16
        # Set PVS Header length
        len_pvs_header = 12
        # Set SPW Header length
        len_spw_header = 12

        pkt = bytes(p, encoding=codec)

        # Read PCAP Record - 16 bytes
        read_bytes = pkt[:len_pcap_rec]

        # Convert ts_sec bytes to integer
        pcap_record_ts_sec = int.from_bytes(read_bytes[:4], byteorder='little')
        # Convert ts_usec bytes to integer
        pcap_record_ts_usec = int.from_bytes(read_bytes[4:8], byteorder='little')

        # Convert incl_len bytes to integer
        pcap_record_incl_len = int.from_bytes(read_bytes[8:12], byteorder='little')
        # Convert orig_len bytes to integer
        pcap_record_orig_len = int.from_bytes(read_bytes[12:16], byteorder='little')

        # -------------------------------- PVS HEADER -------------------------------- #
        # Read PVS Header - 12 bytes
        read_bytes = pkt[len_pcap_rec: (len_pcap_rec + len_pvs_header)]
        # Convert frame_type bytes to integer
        f_type = int.from_bytes(read_bytes[:3], byteorder='little')
        # Convert port_name bytes to string
        port_name = read_bytes[4:12].decode("utf-8").partition('\0')[0]

        # -------------------------------- SPW HEADER -------------------------------- #
        # Read SPW Header - 12 bytes
        read_bytes = pkt[(len_pcap_rec + len_pvs_header): (len_pcap_rec + len_pvs_header + len_spw_header)]
        # Convert Payload length on the wire bytes to integer
        res = int.from_bytes(read_bytes[:4], byteorder='little')
        res_bit = '{0:032b}'.format(res)
        payload_len = int(res_bit[4:], 2)
        # Packet duration as signed integer
        duration_lsb = int.from_bytes(read_bytes[4:8], byteorder='little')
        duration_msb = int.from_bytes(read_bytes[11:], byteorder='little')
        # Total duration
        duration = int(str(duration_msb) + str(duration_lsb))

        # -------------------------------- SPW DATA -------------------------------- #
        # Read SPW Data
        read_bytes = pkt[(len_pcap_rec + len_pvs_header + len_spw_header):
                         (len_pcap_rec + len_pvs_header + len_spw_header + payload_len)]
        # Convert spw_data bytes to hex
        spw_data = read_bytes.hex()
        protocol_id = int(str(spw_data)[2:4], 16)
        logical_addr = int(str(spw_data)[:2], 16)

        # Packet parameters keys
        keys = ['Timestamp',
                'Timestamp_ns',
                'BytesCaptured',
                'OriginalLength',
                'FrameType',
                'InterfaceName',
                'PayloadLength',
                'Duration',
                'PID',
                'LAddr',
                'SpwData']
        # Packet parameter values
        values = [(pcap_record_ts_sec * 10 ** 9 + pcap_record_ts_usec) // 10 ** 3,
                  pcap_record_ts_sec * 10 ** 9 + pcap_record_ts_usec,
                  pcap_record_incl_len,
                  pcap_record_orig_len,
                  f_type,
                  port_name,
                  payload_len,
                  duration,
                  protocol_id,
                  logical_addr,
                  spw_data]

        # Create packet key-value pairs
        packet_data = dict(zip(keys, values))

        return packet_data

    else:
        pass


# Write to database function
def write_rows_spw(rdd):

    if len(rdd.collect()) > 0:
        rdd_list = rdd.collect()
        # pprint.pprint(rdds.collect())

        for i in range(len(rdd_list)):
            pkt = rdd_list[i]
            # pprint.pprint(pkt)

            # Add row to HBase batch
            bat.put(str(datetime.datetime.utcnow().timestamp()),
                    {'SpW:Timestamp': str(pkt['Timestamp']),
                     'SpW:PortName': str(pkt['InterfaceName']),
                     'SpW:Data': str(pkt['SpwData'])})

            # # Write to InfluxDB
            influx_client.write_points(json_body('spacewire',
                                                 (pkt['Timestamp_ns']),  # EGSE&Big-Data-URD_v4.pdf InfluxDB data model
                                                 pkt['InterfaceName']),
                                       database='details')

            # print("i={}, rdd_len={}".format(i, len(rdd_list)))
            if (i + 1) % len(rdd_list) == 0:
                # Send batch to HBase
                bat.send()


# Write to database function
def write_rows_ccsds(rdd):

    if len(rdd.collect()) > 0:
        rdd_list = rdd.collect()
        # pprint.pprint(rdds.collect())

        for i in range(len(rdd_list)):
            pkt = rdd_list[i]
            # pprint.pprint(pkt)

            # Add row to HBase batch
            bat.put(str(datetime.datetime.utcnow().timestamp()),
                    {'CCSDS:Timestamp': str(pkt['Timestamp']),
                     'CCSDS:PortName': str(pkt['InterfaceName']),
                     'CCSDS:Data': str(pkt['SpwData'])})

            # # Write to InfluxDB
            influx_client.write_points(json_body('ccsds',
                                                 (pkt['Timestamp_ns']),  # EGSE&Big-Data-URD_v4.pdf InfluxDB data model
                                                 pkt['InterfaceName']),
                                       database='details')

            # print("i={}, rdd_len={}".format(i, len(rdd_list)))
            if (i + 1) % len(rdd_list) == 0:
                # Send batch to HBase
                bat.send()


def test_func(rdd):

    if len(rdd.collect()) > 0:
        rdd_list = rdd.collect()
        # pprint.pprint(rdd_list)
        for d in rdd_list:
            a = str(arrow.get(decimal.Decimal(d['Timestamp_ns'] / 10 ** 9))).split('.')
            b = a[1].split('+')
            pprint.pprint(b[0])


if __name__ == "__main__":

    topic1 = "CCSDS"
    topic2 = "SpaceWire"
    sc = SparkContext("local[2]", "TestThis")
    ssc = StreamingContext(sc, 10)

    # Kafka Consumer client, connect to Kafka producer server
    # kvs = KafkaUtils.createStream(ssc, 'cldmaster.local:2181', 'spark-streaming-consumer', {topic1: 1, topic2: 1},
    #                               keyDecoder=iso_8859_1, valueDecoder=iso_8859_1)
    kvs = KafkaUtils.createDirectStream(ssc, topics=[topic1, topic2], valueDecoder=iso_8859_1, keyDecoder=iso_8859_1,
                                        kafkaParams={'metadata.broker.list': 'cldmaster.local:9092'})

    # RDD containing SPW packet details
    dStream1 = kvs.map(lambda x: decodePacket(x[1])). \
        filter(lambda y: len(y) > 0). \
        filter(lambda z: z['PID'] != 2)

    # Write SpaceWire rows
    dStream1.foreachRDD(write_rows_spw)

    # RDD containing CCSDS packet details
    dStream2 = kvs.map(lambda x: decodePacket(x[1])). \
        filter(lambda y: len(y) > 0). \
        filter(lambda z: z['PID'] == 2)

    # Write CCSDS rows
    dStream2.foreachRDD(write_rows_ccsds)

    # Print to console
    dStream1.count().pprint()
    dStream2.count().pprint()

    ssc.start()
    ssc.awaitTermination()
