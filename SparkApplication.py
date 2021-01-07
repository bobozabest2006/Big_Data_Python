import pprint
import pandas as pd
import numpy as np
import time
import datetime
from bitstring import BitArray
import itertools
import os

import happybase
from influxdb import DataFrameClient
from Configuration import *
from hdfs3 import HDFileSystem

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


# Bytes format decoder
def iso_8859_1(b):
    if b is None:
        return None
    return b.decode('ISO-8859-1')


# Decoder function for PCAP file bytes
def decodePacket(p, codec="ISO-8859-1"):
    if p is not None:
        # -------------------------------- PCAP RECORD HEADER -------------------------------- #
        pkt = bytes(p, encoding=codec)

        # Read PCAP Record - 16 bytes
        read_bytes = pkt[:pcap_record_header]

        # Convert ts_sec bytes to integer
        ts_sec = int.from_bytes(read_bytes[:4], byteorder='little')
        # Convert ts_usec bytes to integer
        ts_usec = int.from_bytes(read_bytes[4:8], byteorder='little')

        # Convert incl_len bytes to integer
        incl_len = int.from_bytes(read_bytes[8:12], byteorder='little')
        # Convert orig_len bytes to integer
        orig_len = int.from_bytes(read_bytes[12:16], byteorder='little')

        # -------------------------------- PVS HEADER -------------------------------- #
        # Read PVS Header - 12 bytes
        read_bytes = pkt[pcap_record_header: (pcap_record_header + pvs_header)]
        # Convert frame_type bytes to integer
        frame_type = int.from_bytes(read_bytes[:3], byteorder='little')
        # Convert direction bytes to integer
        res = int.from_bytes(read_bytes[3:4], byteorder='little')
        res_bit = '{0:032b}'.format(res)
        direction = int(res_bit[-6], 2)
        # Convert port_name bytes to string
        port_name = read_bytes[4:12].decode("utf-8").partition('\0')[0]

        # -------------------------------- SPW HEADER -------------------------------- #
        # Read SPW Header - 12 bytes
        read_bytes = pkt[(pcap_record_header + pvs_header): (pcap_record_header + pvs_header +
                                                             spw_packet_header)]
        # Convert Payload length on the wire bytes to integer
        res = int.from_bytes(read_bytes[:4], byteorder='little')
        res_bit = '{0:032b}'.format(res)
        payload_length = int(res_bit[4:], 2)
        # Packet duration as signed integer
        duration_lsb = int.from_bytes(read_bytes[4:8], byteorder='little')
        duration_msb = int.from_bytes(read_bytes[11:], byteorder='little')
        # Total duration
        duration = int(str(duration_msb) + str(duration_lsb))

        # -------------------------------- SPW DATA -------------------------------- #
        # Read SPW Data
        spw_packet_data = pkt[(pcap_record_header + pvs_header + spw_packet_header):
                         (pcap_record_header + pvs_header + spw_packet_header + payload_length)]
        # Convert payload_data bytes to hex
        payload_data = spw_packet_data.hex()
        protocol_id = int(str(payload_data)[2:4], 16)
        logical_addr = int(str(payload_data)[:2], 16)
        ext_protocol_id = int(str(payload_data)[4:6], 16)

        # -------------------------------- CCSDS DATA -------------------------------- #
        if protocol_id == 2:
            # ccsds primary header 6 bytes
            ccsds_header = spw_packet_data[4:10]
            # binary ccsds primary header
            ccsds_header_bin = BitArray(bytes=ccsds_header).bin

            ccsds_packet_ver_num = int(ccsds_header_bin[:3], 2)
            ccsds_type = int(ccsds_header_bin[3], 2)
            ccsds_sec_header = bool(ccsds_header_bin[4])
            # ccsds_sec_header = int(ccsds_header_bin[4])
            ccsds_apid = int(ccsds_header_bin[5:16], 2)
            ccsds_seq_flags = int(ccsds_header_bin[16:18], 2)
            ccsds_seq_count = int(ccsds_header_bin[18:32], 2)
            ccsds_packet_len = int(ccsds_header_bin[32:], 2)

            # ccsds data field
            ccsds_data_field = spw_packet_data[10:]
            # ccsds data field header 4 bytes
            ccsds_data_field_header = ccsds_data_field[:4]
            # binary ccsds data field header
            ccsds_data_field_header_bin = BitArray(bytes=ccsds_data_field_header).bin

            ccsds_sec_header_flag = bool(ccsds_data_field_header_bin[0])
            # ccsds_sec_header_flag = int(ccsds_data_field_header_bin[0])
            ccsds_pus_ver_num = int(ccsds_data_field_header_bin[1:4], 2)
            ccsds_ack = int(ccsds_data_field_header_bin[4:8], 2)
            ccsds_service_type = int(ccsds_data_field_header_bin[8:16], 2)
            ccsds_service_subtype = int(ccsds_data_field_header_bin[16:24], 2)
            ccsds_source_id = int(ccsds_data_field_header_bin[24:32], 2)

            # ccsds data hex
            ccsds_data_raw = str(ccsds_data_field[4:-2].hex())
            # ccsds packet error control
            ccsds_error_control = int.from_bytes(ccsds_data_field[-2:], byteorder='big')

        else:
            ccsds_packet_ver_num = UNKNOWN_TYPE
            ccsds_type = UNKNOWN_TYPE
            ccsds_sec_header = UNKNOWN_TYPE
            ccsds_apid = UNKNOWN_TYPE
            ccsds_seq_flags = UNKNOWN_TYPE
            ccsds_seq_count = UNKNOWN_TYPE
            ccsds_packet_len = UNKNOWN_TYPE
            ccsds_sec_header_flag = UNKNOWN_TYPE
            ccsds_pus_ver_num = UNKNOWN_TYPE
            ccsds_ack = UNKNOWN_TYPE,
            ccsds_service_type = UNKNOWN_TYPE
            ccsds_service_subtype = UNKNOWN_TYPE
            ccsds_source_id = UNKNOWN_TYPE
            ccsds_data_raw = UNKNOWN_TYPE
            ccsds_error_control = UNKNOWN_TYPE

        # decoded packet parameters information
        packet_data = {
            "local_timestamp": int(datetime.datetime.utcnow().timestamp() * 10**6),
            "spw_ts_usec": (ts_sec * 10 ** 9 + ts_usec) // 10 ** 3,
            "spw_ts_nsec": ts_sec * 10 ** 9 + ts_usec,
            "spw_incl_length": incl_len,
            "spw_orig_length": orig_len,
            "spw_frame_type": frame_type,
            "spw_port_name": port_name,
            "spw_payload_length": payload_length,
            "spw_duration": duration,
            "spw_protocol_id": protocol_id,
            "spw_logical_addr": logical_addr,
            "spw_data_raw": payload_data,
            "spw_direction": direction,
            "spw_ext_protocol_id": ext_protocol_id,
            "ccsds_packet_ver_num": ccsds_packet_ver_num,
            "ccsds_type": ccsds_type,
            "ccsds_sec_header": ccsds_sec_header,
            "ccsds_apid": ccsds_apid,
            "ccsds_seq_flags": ccsds_seq_flags,
            "ccsds_seq_count": ccsds_seq_count,
            "ccsds_packet_len": ccsds_packet_len,
            "ccsds_sec_header_flag": ccsds_sec_header_flag,
            "ccsds_pus_ver_num": ccsds_pus_ver_num,
            "ccsds_ack": ccsds_ack,
            "ccsds_service_type": ccsds_service_type,
            "ccsds_service_subtype": ccsds_service_subtype,
            "ccsds_source_id": ccsds_source_id,
            "ccsds_data_raw": ccsds_data_raw,
            "ccsds_error_control": ccsds_error_control,
            "raw_packet_data": pkt
        }

        # set type parameter to correct value
        if protocol_id != 2:
            if protocol_id == 1:
                packet_type = {"type": RMAP_TYPE}
                packet_data.update(packet_type)
                return packet_data

            else:
                packet_type = {"type": SPW_RAW_TYPE}
                packet_data.update(packet_type)
                return packet_data

        else:
            packet_type = {"type": TELECOMMAND_TYPE if ccsds_type == 1 else TELEMETRY_TYPE}
            packet_data.update(packet_type)
            return packet_data

    else:
        print("[NO RAW DATA AVAILABLE]")


def to_hbase(data, con, table_name, key, cf='hb', optional=False):
    """Write a pandas DataFrame object to HBase table.

    :param data: pandas DataFrame object that has to be persisted
    :type data: pd.DataFrame
    :param con: HBase connection object
    :type con: happybase.Connection
    :param table_name: HBase table name to which the DataFrame should be written
    :type table_name: str
    :param key: row key to which the dataframe should be written
    :type key: str
    :param cf: Column Family name
    :type cf: str
    :param optional: Optional fields
    :type optional: bool
    """
    # optional fields for the data model
    if optional is True:
        fields = {
            "timestamp": list(data["spw_ts_usec"]),
            "grounddate": list(data["local_timestamp"]),
            "simulationdate": list(data["local_timestamp"]),
            "localdate": list(data["local_timestamp"]),
            # "eng": list(data["spw_payload_length"]),
            # "dec": list(data["spw_payload_length"]),  # payload_length or duration
            # "raw": list(data["spw_payload_length"]),

            # tags
            "confid": revision_number,
            "appid": list(data["ccsds_apid"]),
            "vcid": 0,
            "subsystem": list(data["spw_port_name"]),
            "puss": list(data["ccsds_service_type"]),
            "pusss": list(data["ccsds_service_subtype"]),
            "status_processed": packet_status[0],
            "status_invalid": packet_status[1],
            "status_obsolete": packet_status[1],
            "status_caution": packet_status[1],
            "status_action": packet_status[1],
            "status_alarm": packet_status[1],
            "source": list(data["spw_port_name"]),
            "sbd": satellite_db_ver,
            # "unit": units[0],
            "type": list(data["type"])
        }

        # tag fields
        tags = ["confid", "appid", "vcid", "subsystem", "puss", "pusss",
                "status_processed", "status_invalid", "status_obsolete",
                "status_caution", "status_action", "status_alarm",
                "source", "sbd", "unit", "type"]

    else:
        # mandatory fields for the data model
        fields = {"timestamp": list(data["spw_ts_usec"]),
                  # "eng": list(data["spw_payload_length"]),
                  # "dec": list(data["spw_payload_length"]),

                  # tags
                  "confid": revision_number,
                  }

        # tag fields
        tags = ["confid"]

    # data values for the PAYLOADLENGTH measurement
    if cf == measurement_name[0]:
        params = {"eng": list(data["spw_payload_length"]),
                  "dec": list(data["spw_payload_length"]),
                  # "raw": list(map(hex, list(data["spw_payload_length"]))),
                  "raw_packet_data": list(data["raw_packet_data"]),
                  "unit": units[0]}
        # update fields dictionary with the parameter values
        fields.update(params)

    # data values for the DURATION measurement
    elif cf == measurement_name[1]:
        params = {"eng": list(data["spw_duration"]),
                  "dec": list(data["spw_duration"]),
                  # "raw": list(map(hex, list(data["spw_duration"]))),
                  "raw_packet_data": list(data["raw_packet_data"]),
                  "unit": units[1]}
        # update fields dictionary with the parameter values
        fields.update(params)

    # create pandas dataframe to write to influxDB
    df = pd.DataFrame.from_dict(data=fields)

    # # set timestamp as influxDB index
    df = df.set_index("timestamp")

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
            # row_key = "".join([row_key_template, str(row[0])])
            row_key = "".join([row_key_template, str(next(counter1))])
            row_value = dict()
            for column, value in row[1].iteritems():
                if not pd.isnull(value):
                    row_value[':'.join((cf, column))] = str(value)
            b.put(row_key, row_value)


def to_influxdb(client, data, m_name, database, optional=False):

    # optional fields for the data model
    if optional is True:
        fields = {
            "timestamp": list(data["spw_ts_usec"]),
            "grounddate": list(data["local_timestamp"]),
            "simulationdate": list(data["local_timestamp"]),
            "localdate": list(data["local_timestamp"]),

            # tags
            "confid": revision_number,
            "appid": list(data["ccsds_apid"]),
            "vcid": 0,
            "subsystem": list(data["spw_port_name"]),
            "puss": list(data["ccsds_service_type"]),
            "pusss": list(data["ccsds_service_subtype"]),
            "status_processed": packet_status[0],
            "status_invalid": packet_status[1],
            "status_obsolete": packet_status[1],
            "status_caution": packet_status[1],
            "status_action": packet_status[1],
            "status_alarm": packet_status[1],
            "source": list(data["spw_port_name"]),
            "sbd": satellite_db_ver,
            # "unit": units[0],
            "type": list(data["type"])
        }

        # tag fields
        tags = ["confid", "appid", "vcid", "subsystem", "puss", "pusss",
                "status_processed", "status_invalid", "status_obsolete",
                "status_caution", "status_action", "status_alarm",
                "source", "sbd", "unit", "type"]

    else:
        # mandatory fields for the data model
        fields = {"timestamp": list(data["local_timestamp"]),
                  # "eng": list(data["spw_payload_length"]),
                  # "dec": list(data["spw_payload_length"]),

                  # tags
                  "confid": revision_number,
                  }

        # tag fields
        tags = ["confid"]

    # data values for the PAYLOADLENGTH measurement
    if m_name == measurement_name[0]:
        params = {"eng": list(data["spw_payload_length"]),
                  "dec": list(data["spw_payload_length"]),
                  "raw": list(map(hex, list(data["spw_payload_length"]))),
                  "unit": units[0]}
        # update fields dictionary with the parameter values
        fields.update(params)

    # data values for the DURATION measurement
    elif m_name == measurement_name[1]:
        params = {"eng": list(data["spw_duration"]),
                  "dec": list(data["spw_duration"]),
                  "raw": list(map(hex, list(data["spw_duration"]))),
                  "unit": units[1]}
        # update fields dictionary with the parameter values
        fields.update(params)

    # create pandas dataframe to write to influxDB
    df = pd.DataFrame.from_dict(data=fields)

    # convert int timestamp value to TimestampType (influxDB indexing)
    df.timestamp = pd.to_datetime(df["timestamp"], unit='us', utc=True)

    # set timestamp as influxDB index
    df = df.set_index("timestamp")

    # pprint.pprint(df.dtypes)
    # pprint.pprint(df)

    # write dataFrame to influxDB
    client.write_points(df, measurement=m_name, tag_columns=tags, database=database)


def process_rdd(rdd):
    global hbase_time, influx_time

    if rdd.isEmpty():
        print("[NO DATA]")
        if len(influx_time) > 0 or len(hbase_time) > 0:
            print("Write to InfluxDB Time (s):", sum(influx_time))
            print("Write to HBase Time (s):", sum(hbase_time))
            # ssc.stop()

    else:
        print("[DATA RECEIVED]")

        # define spark dataframe schema
        my_schema = StructType([
            StructField("local_timestamp", LongType(), True),
            StructField("spw_ts_usec", LongType(), True),
            StructField("spw_ts_nsec", LongType(), True),
            StructField("spw_incl_length", IntegerType(), True),
            StructField("spw_orig_length", IntegerType(), True),
            StructField("spw_frame_type", IntegerType(), True),
            StructField("spw_port_name", StringType(), True),
            StructField("spw_payload_length", IntegerType(), True),
            StructField("spw_duration", LongType(), True),
            StructField("spw_protocol_id", IntegerType(), True),
            StructField("spw_logical_addr", IntegerType(), True),
            StructField("spw_data_raw", StringType(), True),
            StructField("spw_direction", IntegerType(), True),
            StructField("spw_ext_protocol_id", IntegerType(), True),
            StructField("ccsds_packet_ver_num", IntegerType(), True),
            StructField("ccsds_type", IntegerType(), True),
            StructField("ccsds_sec_header", BooleanType(), True),
            StructField("ccsds_apid", IntegerType(), True),
            StructField("ccsds_seq_flags", IntegerType(), True),
            StructField("ccsds_seq_count", IntegerType(), True),
            StructField("ccsds_packet_len", IntegerType(), True),
            StructField("ccsds_sec_header_flag", BooleanType(), True),
            StructField("ccsds_pus_ver_num", IntegerType(), True),
            StructField("ccsds_ack", IntegerType(), True),
            StructField("ccsds_service_type", IntegerType(), True),
            StructField("ccsds_service_subtype", IntegerType(), True),
            StructField("ccsds_source_id", IntegerType(), True),
            StructField("ccsds_data_raw", StringType(), True),
            StructField("ccsds_error_control", IntegerType(), True),
            StructField("type", StringType(), True),
            StructField("raw_packet_data", StringType(), True),
        ])

        # convert rdd to spark dataframe
        df = sqlContext.createDataFrame(rdd, schema=my_schema)

        # print spark dataframe
        # df.show()

        # filtered dataframe
        df = df.select(df["local_timestamp"], df["spw_ts_usec"], df["spw_port_name"],
                       df["spw_payload_length"], df["spw_duration"], df["ccsds_apid"],
                       df["ccsds_service_type"], df["ccsds_service_subtype"], df["type"],
                       df["raw_packet_data"], df["spw_data_raw"], df["ccsds_data_raw"])

        # print spark dataframe schema
        # df.printSchema()

        # print spark dataframe
        # df.show()

        # convert SPARK dataframe to PANDAS dataframe(influxDB compatible)
        pd_df = df.toPandas()

        # s_time_influx = time.time()
        # # write to influxDB measurement = PAYLOADLENGTH
        # to_influxdb(client=influx_client_df, data=pd_df, database=db_name,
        #             m_name=measurement_name[0], optional=True)
        #
        # # write to influxDB measurement = DURATION
        # to_influxdb(client=influx_client_df, data=pd_df, database=db_name,
        #             m_name=measurement_name[1], optional=True)
        # e_time_influx = time.time()
        #
        # delta_influx = e_time_influx - s_time_influx
        #
        # s_time_hbase = time.time()
        # # write to HBase measurement = PAYLOADLENGTH
        # to_hbase(pd_df, con=hbase_client, key='test', table_name='details',
        #          cf=measurement_name[0])
        # # # write to HBase measurement = DURATION
        # # to_hbase(pd_df, con=hbase_client, key='test', table_name='details',
        # #          cf=measurement_name[1])
        # e_time_hbase = time.time()
        #
        # delta_hbase = e_time_hbase - s_time_hbase

        for mes in measurement_name:
            s_time_influx = time.time()
            # write to InfluxDB
            to_influxdb(client=influx_client_df, data=pd_df, database=db_name,
                        m_name=mes, optional=True)
            e_time_influx = time.time()

            delta_influx = e_time_influx - s_time_influx

            s_time_hbase = time.time()
            # write to HBase
            to_hbase(pd_df, con=hbase_client, key=mes, table_name='details',
                     cf=mes)
            e_time_hbase = time.time()

            delta_hbase = e_time_hbase - s_time_hbase
            influx_time.append(delta_influx)
            hbase_time.append(delta_hbase)


if __name__ == "__main__":

    # performance measure
    hbase_time = []
    influx_time = []

    # counter for HBase row key
    counter1 = itertools.count(start=0, step=1)

    # topic for consumer to subscribe
    topic = "TM_RAW"

    # printing params for pandas dataframe
    # pd.set_option('display.max_columns', 50)
    # pd.set_option('display.max_colwidth', 20)

    # Create connection to influxDB
    influx_client_df = DataFrameClient(host='cldmaster.local',
                                       port=9998,
                                       username='root',
                                       password='toor')

    # Create connection to HBase
    hbase_client = happybase.Connection('cldmaster.local')

    # spark object
    sc = SparkContext("local[2]", "Spark_Kafka_Streaming_App")  # , batchSize=5000)

    # streaming object
    ssc = StreamingContext(sc, 10)

    # dataframe object
    sqlContext = SQLContext(sc)

    # Kafka Consumer client, connect to Kafka producer server
    # kvs = KafkaUtils.createStream(ssc, 'cldmaster.local:2181', 'spark-streaming-consumer', {topic1: 1, topic2: 1},
    #                               keyDecoder=iso_8859_1, valueDecoder=iso_8859_1)
    kvs = KafkaUtils.createDirectStream(ssc, topics=[topic],
                                        valueDecoder=iso_8859_1,
                                        keyDecoder=iso_8859_1,
                                        kafkaParams={'metadata.broker.list': 'cldmaster.local:9092'})

    # map RDD data database
    dStream = kvs.filter(lambda y: len(y) > 0).repartition(3).map(lambda x: decodePacket(x[1]))
    dStream.count().pprint()
    # dStream.pprint()

    # data model handler
    dStream.foreachRDD(process_rdd)

    # Print to console
    ssc.start()
    ssc.awaitTermination()

