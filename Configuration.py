# Configuration parameters
measurement_name = ["payloadlength", "duration"]
db_name = 'details'
hostname = 'cldmaster.local'
application_name = "SparkKafkaStreaming"
username = 'root'
password = 'toor'
influx_port = 9998
kafka_port = 9092
kafka_topics = ["TM_RAW"]

# PCAP Record length
pcap_record_header = 16
# PVS Header length
pvs_header = 12
# SPW Header length
spw_packet_header = 12

# SPW packet parameters indexes
ts_sec_start = 0
ts_sec_end = 4

ts_usec_start = 4
ts_usec_end = 8

incl_len_start = 8
incl_len_end = 12

orig_len_start = 12
orig_len_end = 16

frame_type_start = 0
frame_type_end = 3

port_name_start = 4
port_name_end = 12

payload_length_start = 0
payload_length_end = 4

duration_lsb_start = 4
duration_lsb_end = 8

duration_msb_start = 11

logical_addr_start = 0
logical_addr_end = 2

protocol_id_start = 2
protocol_id_end = 4

# influx data model parameters
revision_number = "cb4223af4ba"
units = ["bytes", "nanoseconds"]
packet_status = [True, False]
satellite_db_ver = "1.2"

# packet type
UNKNOWN_TYPE = -1
TELECOMMAND_TYPE = "TC"
TELEMETRY_TYPE = "TM"
RMAP_TYPE = "RMAP"
SPW_RAW_TYPE = "SPW_RAW"
