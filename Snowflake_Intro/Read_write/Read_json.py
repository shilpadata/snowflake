import configparser
import os
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType,StructField,StringType,IntegerType,DateType,TimestampType

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

book_df = session.read.json("@my_stage/json_folder/books1.json")
book_schema = book_df.schema
print(f"\n Schema object is :{type(book_schema)}")

print(f"Number of columns : {len(book_schema.fields)}")

#Display 10 records from book dataframe
book_df.show()

#select the fields

author_df = book_df.selectExpr("$1:author")
author_df.show()

#write dataframe to Table
book_df.write.mode("overwrite").saveAsTable("book_json")

session.sql("select * from book_json limit 10 ").show()

#Create Temp table
book_df.write.mode("overwrite").saveAsTable(table_name="book_json_01",table_type="temporary")

#Flatten Json
flatten_df = session.sql("SELECT $1:author::TEXT AS author, $1:cat::TEXT AS cat, $1:genre_s::TEXT AS genre_s,  $1:id::TEXT AS id,$1:inStock::TEXT AS inStock,$1:name::TEXT AS name, $1:pages_i::INT AS pages_i,$1:price::FLOAT AS price, $1:sequence_i::INT AS sequence_i FROM book_json_01")

flatten_df.show()

