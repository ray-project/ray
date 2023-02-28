# flake8: noqa

# fmt: off
# __connect_props_begin__
import os, yaml

# set directly in code
connect_props = dict(
    user = 'MY_USER',
    password = 'MY_PASSWORD',
    database = 'MY_DATABASE'
)

# load from yaml file
CONNECT_PROPS_FILE = "connect_props.yaml"
if os.path.isfile(CONNECT_PROPS_FILE):
    with open(CONNECT_PROPS_FILE, 'r') as stream:
        connect_props = yaml.safe_load(stream)

# load from environment
def get_env_props(prefix):
    return {
        key.replace(prefix,'').lower(): value \
        for key,value in os.environ.items() if prefix in key
    }
connect_props = get_env_props('SQLITE3_')
    
# __connect_props_end__
# fmt: on

# fmt: off
# __database_support_import_begin__
import ray

# sqlite 3 connect function
from sqlite3 import connect

# postgress connect function
# from psycopg2 import connect

# mysql connect function
# from mysql.connector import connect

# __database_support_import_end__
# fmt: on

# fmt: off
# __database_support_read_write_begin__

# create some data
SAMPLE_SIZE = 100
with connect(**connect_props) as con:
    con.execute('DROP TABLE IF EXISTS source')
    con.execute('DROP TABLE IF EXISTS destination')
    con.execute('CREATE TABLE source(int_val, str_val, flt_val, bool_val)')
    con.execute('CREATE TABLE destination(int_val, str_val, flt_val, bool_val)')
    con.commit()
    data = [[int(i),str(i),float(i),i%2==0] for i in range(0,SAMPLE_SIZE)]
    con.executemany('INSERT INTO source VALUES (?, ?, ?, ?)', data)

# write data to a table
ray.data.write_dbapi2(
    connect, connect_props, 
    table='source'
)

# read all columns and rows from a table
ds = ray.data.read_dbapi2(
    connect, 
    connect_props, 
    table='source'  
)
#.-> Dataset(
#       num_blocks=14, 
#       num_rows=150000, 
#       schema={int_val: int64, str_val: string, flt_val: double, bool_val: int64}
# )
ds.take(2)
#.-> [
#       {'int_val': 0, 'str_val': '0', 'flt_val': 0.0, 'bool_val': 1},
#       {'int_val': 1, 'str_val': '1', 'flt_val': 1.0, 'bool_val': 0}
# ]

# read with a query, specifying two columns 
# and filtering rows with a where clause
ds = ray.data.read_dbapi2(
    connect, 
    connect_props, 
    query='SELECT int_val, str_val FROM source WHERE int_val > 2'
)
#.-> Dataset(
#       num_blocks=14, 
#       num_rows=150000, 
#       schema={int_val: int64}
# )
ds.take(2)
#.-> [
#       {'int_val': 3, 'str_val': '3'},
#       {'int_val': 4, 'str_val': '4'}
# ]

# write to staging tables prior to final copy to destination table
ds.write_dbapi2(
    connect, 
    connect_props, 
    table='destination',
    mode='stage'
)

# __database_support_read_write_end__
# fmt: on

# fmt: off
# __database_support_parallelism_begin__

# read from an entire table with 100 read tasks
ds = ray.data.read_dbapi2(
    connect, 
    connect_props, 
    parallelism=100,
    table='source'
)
#.-> Dataset(
#       num_blocks=14, 
#       num_rows=150000, 
#       schema={int_val: int64, str_val: string, flt_val: double, bool_val: int64}
# )

# write to a table with 20 write tasks
ds.repartition(20).write_dbapi2(
    connect, 
    connect_props, 
    table='destination'
)
# __database_support_parallelism_end__
# fmt: on

# fmt: off
# __database_support_databricks_begin__

# read databricks properties from environment
connect_props = get_env_props('DATABRICKS_')
  
# read entire customer table
ds = ray.data.read_databricks(
    connect_props, 
    table='samples.tpch.customer'
)
#.-> Dataset(
#   num_blocks=10, 
#   num_rows=10, 
#   schema={c_custkey: int64, c_name: string, c_address: string, 
#       c_nationkey: int64, c_phone: string, c_acctbal: decimal128(18, 2), 
#       c_mktsegment: string, c_comment: string
#   }
# )
ds.take(2)
#.-> [{'c_custkey': 412445,
#  'c_name': 'Customer#000412445',
#  'c_address': '0QAB3OjYnbP6mA0B,kgf',
#  'c_nationkey': 21,
#  'c_phone': '31-421-403-4333',
#  'c_acctbal': Decimal('5358.33'),
#  'c_mktsegment': 'BUILDING',
#  'c_comment': 'arefully blithely regular epi'},
# {'c_custkey': 412446,
#  'c_name': 'Customer#000412446',
#  'c_address': '5u8MSbyiC7J,7PuY4Ivaq1JRbTCMKeNVqg ',
#  'c_nationkey': 20,
#  'c_phone': '30-487-949-7942',
#  'c_acctbal': Decimal('9441.59'),
#  'c_mktsegment': 'MACHINERY',
#  'c_comment': 'sleep according to the fluffily even forges. fluffily careful packages after the ironic, silent deposi'}]

# query specific columns with a where clause
ds = ray.data.read_databricks(
    connect_props, 
    query=  'SELECT c_acctbal, c_mktsegment ' +
            'FROM samples.tpch.customer WHERE c_acctbal < %(balance)i',
    parameters= {'balance':0.0}
)
#.-> Dataset(
#       num_blocks=14, 
#       num_rows=1000, 
#       schema={C_ACCTBAL: decimal128(18, 2), C_MKTSEGMENT: string}
# )
ds.take(2)
#.-> [{'C_ACCTBAL': Decimal('-219.53'), 'C_MKTSEGMENT': 'BUILDING'},
#  {'C_ACCTBAL': Decimal('-778.23'), 'C_MKTSEGMENT': 'AUTOMOBILE'}]

# write to a destination table
ds.write_databricks(
    connect_props, 
    table='hive_metastore.default.customer2',
    stage_uri='s3://mybucket/stage'
)
# __database_support_databricks_end__
# fmt: on

# fmt: off
# __database_support_snowflake_begin__

# read snowflake properties from environment
connect_props = get_env_props('SNOWFLAKE_')
  
# read entire customer table
ds = ray.data.read_snowflake(
    connect_props, 
    table='snowflake_sample_data.tpch_sf1.customer'
)
#.-> Dataset(
#   num_blocks=10, 
#   num_rows=10, 
#   schema={C_CUSTKEY: int32, C_NAME: object, C_ADDRESS: object, 
#       C_NATIONKEY: int8, C_PHONE: object, C_ACCTBAL: float64, 
#       C_MKTSEGMENT: object, C_COMMENT: object}
#   )

ds.take(2)
#.-> [
#   {
#       'C_CUSTKEY': 30001,
#       'C_NAME': 'Customer#000030001',
#       'C_ADDRESS': 'Ui1b,3Q71CiLTJn4MbVp,,YCZARIaNTelfst',
#       'C_NATIONKEY': 4,
#       'C_PHONE': '14-526-204-4500',
#       'C_ACCTBAL': 8848.47,
#       'C_MKTSEGMENT': 'MACHINERY',
#       'C_COMMENT': 'frays wake blithely enticingly ironic asymptote'
#   },
#   {
#       'C_CUSTKEY': 30002,
#       'C_NAME': 'Customer#000030002',
#       'C_ADDRESS': 'UVBoMtILkQu1J3v',
#       'C_NATIONKEY': 11,
#       'C_PHONE': '21-340-653-9800',
#       'C_ACCTBAL': 5221.81,
#       'C_MKTSEGMENT': 'MACHINERY',
#       'C_COMMENT': 'he slyly ironic pinto beans wake slyly above the fluffily careful warthogs. even dependenci'
#   }
# ]

# query specific columns with a where clause
ds = ray.data.read_snowflake(
    connect_props, 
    query=  'SELECT c_acctbal, c_mktsegment ' +
            'FROM samples.tpch.customer WHERE c_acctbal < ?',
    params=[1000]
)
#.-> Dataset(
#       num_blocks=8, 
#       num_rows=122881, 
#       schema={C_ACCTBAL: float64, C_MKTSEGMENT: object}
# )

ds.take(2)
#.-> [
#   {'C_ACCTBAL': 8848.47, 'C_MKTSEGMENT': 'MACHINERY'},
#   {'C_ACCTBAL': 5221.81, 'C_MKTSEGMENT': 'MACHINERY'}
# ]

# create destination db and tables
from snowflake.connector import connect
with connect(**connect_props) as con:
    con.execute('create database if not exits ray_sample')
    con.commit()
    con.execute(
        'create or replace customer_copy' +
        'like snowflake_sample_data.tpch_sf1.customer'
    )

# write to a destination table
ds.write_snowflake(
    connect_props, 
    table='ray_sample.public.customer_copy'
)

# __database_support_snowflake_end__
# fmt: on