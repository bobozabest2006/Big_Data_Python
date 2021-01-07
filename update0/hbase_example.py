import happybase
from collections import OrderedDict
import pprint

# Create custom Hbase class with method so read or write to database

# Path to pcap file
pcap_file = '/home/cloudera-master/2018-04-05_09-36-26_Default_SPW-4.pcap'

connection = happybase.Connection('cldmaster.local')
connection.open()
families = {
    'cf1': dict(max_versions=10),
    'cf2': dict()
}

spw_families = {
    'SpW': dict()
}

table_name = 'PacketDetails'

# if connection.is_table_enabled(table_name):
#     print('Table found [OK]')

# connection.create_table(table_name, spw_families)

table = connection.table(table_name)
bat = table.batch(transaction=True)

for key, data in table.scan():
    # row = bat.delete(str(key.decode('utf-8')))
    print(key.decode('utf-8'))

# for i in range(10):
#     row = bat.put(str(i), {'SpW:Details': 'Florin Salam {}'.format(i)})

bat.send()
