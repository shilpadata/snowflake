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

#Declare schema
schema = StructType([StructField('Firstname',StringType()),
                     StructField('Lastname',StringType()),
                     StructField('Email',StringType()),
                     StructField('Address',StringType()),
                     StructField('City',StringType()),\
                     StructField('Date',DateType())])
#Read CSV file
employee_df1 = session.read.schema(schema).csv('@my_stage/Employee/employees01.csv')
employee_df1.show()

#Reading csv file with options
employee_df3 = session.read.schema(schema).csv('@my_stage/Employee/employees02_error.csv')
employee_df3.show()
employee_df3 = session.read.options({"ON_ERROR":"CONTINUE"}).schema(schema).csv('@my_stage/Employee/employees02_error.csv')
employee_df3.show()
print(type(employee_df3))