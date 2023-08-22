import configparser
import os
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

#get configs

config =configparser.ConfigParser()
ini_path = os.path.join(os.getcwd(),'config.ini')
print(ini_path)
config.read(ini_path)

#Snowflake config
sf_account = config['Snowflake']['sf_account']
sf_user = config['Snowflake']['sf_user']
sf_pass=config['Snowflake']['sf_pass']
sf_role=config['Snowflake']['sf_role']
sf_warehouse = config['Snowflake']['sf_warehouse']
sf_database =  config['Snowflake']['sf_database']
sf_schema =config['Snowflake']['sf_schema']

connection_parameters = {"account": sf_account,
                         "user": sf_user,
                         "password": sf_pass,
                         "role": sf_role,
                         "warehouse": sf_warehouse,
                         "database": sf_database,
                         "schema": sf_schema}


#Create a session
session = Session.builder.configs(connection_parameters).create()


test = session.createDataFrame([1,2,3,4], schema=["A"])
test.show()

test = session.createDataFrame([[1,2,3,4],[5,6,7,8]], schema=["A","B","C","D"])
test.show()

test1 = session.createDataFrame([1,2,3,{"a":7}], schema=["A"])
test1.show()

import pandas as pd
x = session.create_dataframe(pd.DataFrame([(1, 2, 3, 4)], columns=["a", "b", "c", "d"]))
x.show()


table_name = "SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CALL_CENTER"
df = session.table(table_name)
type(df)
print(f"type of df is {type(df)}")
df.show()

records = session.sql("SELECT *  FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CALL_CENTER limit 20")
records.collect()
print(f"""Records - : {records.collect()}""")
print(f"type of records is {type(records)}")
records.show(20)

print(f"Number of records in table is {df.count()}")
