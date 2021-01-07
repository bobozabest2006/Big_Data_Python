import pprint
import pandas as pd
import numpy as np
import time
import subprocess

import happybase
import pdhbase as pdh
from influxdb import DataFrameClient

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


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
        if protocol_id != 2:
            protocol_name = 'spacewire{}'.format(logical_addr)
        else:
            protocol_name = 'ccsds{}'.format(logical_addr)

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
                'SpwData',
                'Protocol']
        # Packet parameter values
        values = [(pcap_record_ts_sec * 10 ** 9 + pcap_record_ts_usec) // 10 ** 3,  # timestamp in microseconds
                  pcap_record_ts_sec * 10 ** 9 + pcap_record_ts_usec,
                  pcap_record_incl_len,
                  pcap_record_orig_len,
                  f_type,
                  port_name,
                  payload_len,
                  duration,
                  protocol_id,
                  logical_addr,
                  spw_data,
                  protocol_name]

        # Create packet key-value pairs
        packet_data = dict(zip(keys, values))

        return packet_data

    else:
        pass


def to_hbase(df, con, table_name, key, cf='hb'):
    """Write a pandas DataFrame object to HBase table.

    :param df: pandas DataFrame object that has to be persisted
    :type df: pd.DataFrame
    :param con: HBase connection object
    :type con: happybase.Connection
    :param table_name: HBase table name to which the DataFrame should be written
    :type table_name: str
    :param key: row key to which the dataframe should be written
    :type key: str
    :param cf: Column Family name
    :type cf: str
    """
    table = con.table(table_name)

    # column_dtype_key = key + 'columns'
    column_dtype_key = ".".join([key, 'ColumnType'])
    column_dtype_value = dict()
    for column in df.columns:
        column_dtype_value[':'.join([cf, column])] = df.dtypes[column].name

    # column_order_key = key + 'column_order'
    column_order_key = ".".join([key, 'ColumnOrder'])
    column_order_value = dict()
    for i, column_name in enumerate(df.columns.tolist()):
        # order_key = struct.pack('>q', i).decode("utf-8")
        order_key = str(i)
        column_order_value[':'.join((cf, order_key))] = column_name

    row_key_template = ".".join([key, 'Row'])
    with table.batch(transaction=True) as b:
        b.put(column_dtype_key, column_dtype_value)
        b.put(column_order_key, column_order_value)
        for row in df.iterrows():
            # row_key = row_key_template + struct.pack('>q', row[0]).decode("utf-8")
            row_key = "".join([row_key_template, str(row[0])])
            row_value = dict()
            for column, value in row[1].iteritems():
                if not pd.isnull(value):
                    row_value[':'.join((cf, column))] = str(value)
            b.put(row_key, row_value)


def influxdb_write(client, data, m_name, db_name, length=0, optional=False):

    if optional is True:
        fields = {
            "timestamp": list(data["Timestamp"]),
            "groupdate": [int(i + 100) for i in range(length)],
            "simulationdate": list(data.index.astype(np.int64)),
            "Localdate": list(data.index.astype(np.int64)),
            "eng": [int(i + 50) for i in range(length)],
            "dec": [float(i + 30.5) for i in range(length)],
            "raw": [str(i + 200) for i in range(length)],
            # tags
            "confid": [str(hex(i + 100)) for i in range(length)],
            "appid": [int(i + 10) for i in range(length)],
            "vcid": [int(i + 10) for i in range(length)],
            "subsystem": [str(i + 10) for i in range(length)],
            "puss": [int(i + 68) for i in range(length)],
            "pusss": [int(i + 596) for i in range(length)],
            "status_processed": [bool() for _ in range(length)],
            "status_invalid": [bool() for _ in range(length)],
            "status_obsolete": [bool() for _ in range(length)],
            "status_caution": [bool() for _ in range(length)],
            "status_action": [bool() for _ in range(length)],
            "status_alarm": [bool() for _ in range(length)],
            "source": [str(hex(i + 9876)) for i in range(length)],
            # "sbd": [str(hex(i + 6874)) for i in range(length)],
            "sbd": list(data["Protocol"]),
            "unit": [str(hex(i + 3652)) for i in range(length)],
            "type": [str(hex(i + 4523)) for i in range(length)]
        }

        tags = ["confid", "appid", "vcid", "subsystem", "puss", "pusss",
                "status_processed", "status_invalid", "status_obsolete",
                "status_caution", "status_action", "status_alarm",
                "source", "sbd", "unit", "type"]

    else:
        fields = {"timestamp": list(data["Timestamp"]),
                  "eng": [int(i + 50) for i in range(length)],
                  "dec": [float(i + 30.5) for i in range(length)],
                  # tags
                  "confid": "Version 1.2",
                  }

        tags = ["confid"]

    df = pd.DataFrame.from_dict(data=fields)

    df.timestamp = pd.to_datetime(df["timestamp"], unit='us', utc=True)

    df = df.set_index("timestamp")

    # pprint.pprint(df)

    start_time = time.time()
    # write dataFrame to influxDB
    client.write_points(df, measurement=m_name,
                        tag_columns=tags,
                        database=db_name)

    end_time = time.time()

    delta = end_time - start_time

    # for performance measuring only
    batch_write_time.append(delta * 10**3)
    single_rec_write_time.append(delta * 10**3 / length)
    pprint.pprint("InfDB:Write Time {} records: {:.3f} ms".format(length, delta * 10**3))
    pprint.pprint("InfDB:Write Time 1 record: {} ms".format(delta * 10**3 / length))


def process_rdd(rdd):

    if rdd.isEmpty():
        print("[NO DATA RECV]")
        if len(batch_write_time) > 0:
            # for performance measuring only
            batch_write_time.sort()
            temp = np.diff(batch_write_time)
            print("InfluxDB Batch ExecTime(ms):", sum(list(temp)) / len(list(temp)))

            # for performance measuring only
            single_rec_write_time.sort()
            temp1 = np.diff(single_rec_write_time)
            print("InfluxDB SingleRecord AvgTime(ms):", sum(list(temp1)) / len(list(temp1)))

            # # for performance measuring only
            # batch_write_time.sort()
            # temp2 = np.diff(batch_write_time)
            # print("HBase Batch ExecTime(ms):", sum(list(temp2)) / len(list(temp2)))
            #
            # # for performance measuring only
            # single_rec_write_time.sort()
            # temp3 = np.diff(single_rec_write_time)
            # print("HBase SingleRecord AvgTime(ms):", sum(list(temp3)) / len(list(temp3)))

    else:
        print("[DATA RECV]")

        df = sqlContext.jsonRDD(rdd)

        # df.printSchema()
        # get DataFrame filtered data
        # rows = df.select(df["Timestamp"], df["InterfaceName"], df["SpwData"],
        #                  df["Protocol"].alias("measurement"))#.collect()
        # rows.show()

        # get DataFrame filtered data
        rows = df.select(df["Timestamp"], df["Protocol"])

        # convert SPARK dataframe to PANDAS dataframe(influxDB compatible)
        pd_df = rows.toPandas()

        # write to influxDB
        influxdb_write(client=influx_client_df, data=pd_df, db_name='details',
                       m_name='AT1030Z', length=rdd.count())

        # write to HBase
        # hbase_write(client=hbase_client, data=pd_df, db_name='details',
        #             length=rdd.count(), m_name='AT1030Z')


if __name__ == "__main__":

    topic1 = "CCSDS"
    topic2 = "SpaceWire"

    # for performance measuring only
    batch_write_time = []
    single_rec_write_time = []

    batch_write_time1 = []
    single_rec_write_time1 = []
    # pd.set_option('display.max_columns', 20)
    # pd.set_option('display.max_colwidth', 100)

    # Create connection to influxDB
    influx_client_df = DataFrameClient(host='cldmaster.local',
                                       port=9998,
                                       username='root',
                                       password='toor')

    # Create connection to HBase
    hbase_client = happybase.Connection('cldmaster.local')

    # spark object
    sc = SparkContext("local[2]", "SparkKafkaStreaming", batchSize=5000)

    # streaming object
    ssc = StreamingContext(sc, 10)

    # dataframe object
    sqlContext = SQLContext(sc)

    # Kafka Consumer client, connect to Kafka producer server
    # kvs = KafkaUtils.createStream(ssc, 'cldmaster.local:2181', 'spark-streaming-consumer', {topic1: 1, topic2: 1},
    #                               keyDecoder=iso_8859_1, valueDecoder=iso_8859_1)
    kvs = KafkaUtils.createDirectStream(ssc, topics=[topic1],
                                        valueDecoder=iso_8859_1,
                                        keyDecoder=iso_8859_1,
                                        kafkaParams={'metadata.broker.list': 'cldmaster.local:9092'})

    def some_function(rdd):
        if rdd.isEmpty():
            print("[NO DATA RECEIVED]")

        else:
            # rdd.saveAsPickleFile("/user/test/test.pcap", batchSize=rdd.count())
            # rdd.saveAsNewAPIHadoopFile("/user/test/test_newHadoopAPI3.pcap",
            #                            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat")
            # pickleRdd1 = sc.pickleFile("/user/test/test.pcap").collect()

            # print(pickleRdd1)
            cat = subprocess.Popen(["hadoop", "fs", "-cat", "/user/test/test_newHadoopAPI3.pcap"], stdout=subprocess.PIPE)
            for line in cat.stdout:
                print(line)


    # kvs.foreachRDD(lambda rdd: print("[NO DATA RECEIVED]") if rdd.isEmpty() else rdd.saveAsPickleFile("/user/test/test.pcap", batchSize=rdd.count()))
    kvs.foreachRDD(some_function)

    # # map RDD data
    # dStream = kvs.map(lambda x: decodePacket(x[1])).filter(lambda y: len(y) > 0)
    #
    # dStream.count().pprint()
    #
    # # data model handler
    # dStream.foreachRDD(process_rdd)

    # # RDD containing SPW packet details
    # spwStream = dStream.filter(lambda a: a['PID'] != 2)
    #
    # # RDD containing CCSDS packet details
    # ccsdsStream = dStream.filter(lambda b: b['PID'] == 2)

    # # Write SpaceWire rows (using rdd)
    # spwStream.foreachRDD(write_rows_spw)

    # # Write CCSDS rows (using rdd)
    # ccsdsStream.foreachRDD(write_rows_ccsds)

    # # Write SpaceWire rows (using dataframe)
    # spwStream.foreachRDD(df_spw)

    # Write CCSDS rows (using dataframe)
    # ccsdsStream.foreachRDD(df_ccsds)

    # data model handler
    # spwStream.foreachRDD(process_rdd)
    # ccsdsStream.foreachRDD(process_rdd)

    # Print to console
    ssc.start()
    ssc.awaitTermination()
