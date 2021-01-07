import happybase

connection = happybase.Connection('cldmaster.local')
connection.open()
table_name = 'PacketDetails'

# if connection.is_table_enabled(table_name):
#     print('Table found [OK]')

# connection.create_table(table_name, spw_families)

table = connection.table(table_name)

# for i in range(1, 4):
#
#     values = table.cells(row='{}'.format(i), column='SpW', versions=1)
#
#     if values:
#         print(values[1].decode("utf-8"),
#               '\n',
#               values[2].decode("utf-8"),
#               '\n',
#               values[0].decode("utf-8"),
#               '\n')

for key, data in table.scan():
    print(key,
          data[b'SpW:InterfaceName'],
          '\n',
          data[b'SpW:Timestamp'],
          '\n',
          data[b'SpW:Data'],
          '\n')
