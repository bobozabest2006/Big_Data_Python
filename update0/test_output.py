# python ExperimentsTesting.py | kafka-console-producer --broker-list cldmaster.local:9092 --topic test
# kafka-console-consumer --zookeeper cldmaster.local:2181 --topic test

import arrow
import time
import pprint
import decimal
import datetime
from TrafficSimulator import TrafficGenerator
from influxdb import InfluxDBClient
from influxdb import SeriesHelper


def decodePacket(p, codec="ISO-8859-1"):

    if p is not None:
        # -------------------------------- PCAP RECORD HEADER -------------------------------- #
        # Set PCAP Record length
        len_pcap_rec = 16
        # Set PVS Header length
        len_pvs_header = 12
        # Set SPW Header length
        len_spw_header = 12

        pkt = bytes(p.decode(codec), encoding=codec)

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
        values = [decimal.Decimal(pcap_record_ts_sec + pcap_record_ts_usec / 10**9),
                  pcap_record_ts_sec * 10**9 + pcap_record_ts_usec,
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


# create influxdb client
influx_client = InfluxDBClient(host='cldmaster.local', port=9998, username='root',
                               password='toor', database='test2')

# get list of current databases
db_name_list = influx_client.get_list_database()


class MySeriesHelper(SeriesHelper):
    """Instantiate SeriesHelper to write points to the backend"""

    class Meta:
        """Meta class stores time series helper configuration"""

        # The client should be an instance of InfluxDBClient
        client = influx_client

        # The series name must be a string. Add dependent fields/tags
        # in curly brackets
        series_name = 'SpaceWire'

        # Define all fields in this time series
        fields = ['portname', 'timestamp']

        # Define all tags for the fields
        tags = ['protocol']

        # Define the number of data points to store prior to writing
        # on the wire
        bulk_size = 30

        # autocommit must be set to True when using bulk_size
        autocommit = True


def list_db(x):
    return x['name']


def is_db(name):
    data_list = list(map(lambda z: list_db(z), db_name_list))

    if name in data_list:
        print('True')
        return True
    else:
        influx_client.create_database(name)
        print('Database created [OK]')


def ceva(obj_pkt):

    for i in range(20000):
        packet = next(obj_pkt)[1]

        new_pkt = decodePacket(packet)

        MySeriesHelper(portname=new_pkt['InterfaceName'],
                       protocol='SPW',
                       timestamp=str(new_pkt['Timestamp_ns'] // 10**3),
                       time=str(arrow.get(decimal.Decimal(new_pkt['Timestamp_ns'] / 10**9))))

        # To inspect the JSON which will be written
        # pprint.pprint(MySeriesHelper._json_body_())

        if i % 5 == 0:
            print('{} points inserted.'.format(i))

        time.sleep(0.5)

    # Manually submit data points which are not yet written
    MySeriesHelper.commit()


if __name__ == "__main__":
    # influx_client.drop_database('test2')
    # influx_client.create_database('test2')
    # influx_client.create_retention_policy('test_policy2', 'INF', '3', 'test2', default=True)
    #
    # pkt_obj = enumerate(TrafficGenerator('/home/cloudera-master/2018-04-05_09-36-26_Default_SPW-4.pcap'))
    #
    # ceva(pkt_obj)

    # for i in range(20000):
    #     packet = next(pkt_obj)[1]
    #
    #     new_pkt = decodePacket(packet)
    #
        # MySeriesHelper(portname=new_pkt['InterfaceName'],
        #                protocol='SPW',
        #                timestamp=str(new_pkt['Timestamp_ns'] // 10**3),
        #                time=str(arrow.get(decimal.Decimal(new_pkt['Timestamp_ns'] / 10**9))))
    #
    #     # To inspect the JSON which will be written
    #     # pprint.pprint(MySeriesHelper._json_body_())
    #
    #     if i % 5 == 0:
    #         print('{} points inserted.'.format(i))
    #
    #     time.sleep(0.5)

    # Manually submit data points which are not yet written
    # MySeriesHelper.commit()

    result = influx_client.query('select * from SpaceWire;')
    print(result)

    # packet = next(pkt_obj)[1]
    # new_pkt = decodePacket(packet)
    # print(arrow.get(decimal.Decimal(new_pkt['Timestamp_ns'] / 10**9)))
    # print(new_pkt['Timestamp_ns'])
