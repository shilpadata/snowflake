from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
#replace with your connection paramters
connection_parameters = {"account": "ya07079.ap-south-1",
                         "user":"shilpa",
                         "password":"******",
                         "role":"ACCOUNTADMIN",
                         "warehouse":"COMPUTE_WH",
                         "database":"DEMO",
                         "schema":"SNOWPARK_DEMO"}
#Create a session
session = Session.builder.configs(connection_parameters).create()
tmstamp = session.sql("select current_timestamp").collect()
print(tmstamp)

print(session.sql("select current_warehouse(), current_database(), current_schema()").collect())
