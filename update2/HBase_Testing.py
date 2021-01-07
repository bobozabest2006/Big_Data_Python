import happybase
import numpy as np
import pandas as pd
import pdhbase as pdh
import struct
import sys

# # Create custom Hbase class with method so read or write to database
# connection = happybase.Connection('cldmaster.local')
#
# connection.open()
#
# families = {
#     'SpW': dict(),
#     'CCSDS': dict()
# }
#
# table_name = 'details'


def time_measurement(conn, tb_name):
    table = conn.table(tb_name)

    x = []

    for key, data in table.scan(include_timestamp=False):
        # print(key, data)
        x.append(float(key.decode("utf-8")) * 1000)
        # print(float(key.decode("utf-8").split(":")[2]))

    avg = np.diff(x)

    return list(avg)


def clear_db(conn):
    for name in conn.tables():
        print(name)
        conn.delete_table(name=name, disable=True)
    print(conn.tables())


def create_table(conn, name, fam=None):
    # if bytes(name, encoding="utf-8") in conn.tables():
    #     if conn.is_table_enabled(name):
    #         print("Table already exists.")
    # else:
    #     print("Creating table {}.".format(name))
    #     conn.create_table(name, fam)
    conn.create_table(name, fam)


def new_test(conn):
    print(conn.tables())

    # clear database
    clear_db(conn)

    # create empty table
    # create_table(connection, table_name, families)
    create_table(conn, 'details', {'AT1030Z': dict()})


# print("HBase AvgTime(ms):", sum(time_measurement(connection)) / len(time_measurement(connection)))

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
        column_dtype_value[':'.join((cf, column))] = df.dtypes[column].name

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


def read_hbase(con, table_name, key, cf='hb'):
    """Read a pandas DataFrame object from HBase table.

    :param con: HBase connection object
    :type con: happybase.Connection
    :param table_name: HBase table name to which the DataFrame should be read from
    :type table_name: str
    :param key: row key from which the DataFrame should be read
    :type key: str
    :param cf: Column Family name
    :type cf: str
    :return: Pandas DataFrame object read from HBase
    :rtype: pd.DataFrame
    """
    table = con.table(table_name)

    column_dtype_key = key + 'columns'
    column_dtype = table.row(column_dtype_key, columns=[cf])
    columns = {col.split(':')[1]: value for col, value in column_dtype.items()}

    column_order_key = key + 'column_order'
    column_order_dict = table.row(column_order_key, columns=[cf])
    column_order = list()
    for i in range(len(column_order_dict)):
        column_order.append(column_order_dict[':'.join((cf, struct.pack('>q', i)))])

    row_start = key + 'rows' + str(struct.pack('>q', 0))
    row_end = key + 'rows' + str(struct.pack('>q', sys.maxsize))
    rows = table.scan(row_start=row_start, row_stop=row_end)
    df = pd.DataFrame(columns=columns)
    for row in rows:
        df_row = {key.split(':')[1]: value for key, value in row[1].items()}
        df = df.append(df_row, ignore_index=True)
        print()
    for column, data_type in columns.items():
        df[column] = df[column].astype(np.dtype(data_type))

    return df


def print_values(con, table_name):

    table = con.table(table_name)

    for key, data in table.scan():
        print(key, data)


def connect(host, tb_prefix=None):
    if tb_prefix:
        connection = happybase.Connection(host, table_prefix=tb_prefix)
    else:
        connection = happybase.Connection(host, table_prefix=tb_prefix)

    connection.open()

    return connection


def write_data(conn, data, tb_name, c_fam):

    table = conn.table(tb_name, use_prefix=True)

    print("Table column families: {}".format(table.families()))

    to_hbase(df=data, con=conn, table_name=tb_name, key='df_key', cf=c_fam)


connection = None
try:
    # connection = happybase.Connection('cldmaster.local', table_prefix="")
    # connection.open()

    # clean database
    # new_test(connection)

    df = pd.DataFrame(np.random.randn(10, 5), columns=['a', 'b', 'c', 'd', 'e'])
    df['f'] = 'hello world'
    print(df)

    fields = {"timestamp": [int(i + 203) for i in range(5)],
              "eng": [int(i + 50) for i in range(5)],
              "dec": [float(i + 30.5) for i in range(5)],
              # tags
              "confid": ["Record {}".format(i) for i in range(5)],
              }
    df1 = pd.DataFrame.from_dict(data=fields)
    print(df1)

    fam = {'cf1': dict()}

    print(list(fam.keys()))

    write_data(host='cldmaster.local', c_fam=fam, data=df1,
               tb_name='test', m_name='AT1030Z')

    # to_hbase(df=df1, con=connection, table_name='details', key='df_key', cf='cf')
    # read_hbase(con=connection, table_name='sample_table', key='df_key', cf='cf')

    # print_values(con=connection, table_name='sample_table')
finally:
    if connection:
        connection.close()
