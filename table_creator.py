import requests
import csv
from bs4 import BeautifulSoup as bs
from random import randint
from time import sleep
from urllib.parse import urljoin
import os
import snowflake.connector
from datetime import datetime
import uuid
import re
import pandas as pd
import time
from word2number import w2n  # Library to convert numbers to words

URL = 'https://www.espncricinfo.com/records/trophy/indian-premier-league-117'
sleep_delay = 5
#https://rforotb-ka22072.snowflakecomputing.com
# Snowflake connection parameters
snowflake_account = 'zzsqojn-oq97208'
snowflake_warehouse= 'COMPUTE_WH'
snowflake_database = 'MYCRICKET'
snowflake_schema = 'DATA2'
snowflake_user = 'comicsvibe'
snowflake_password = 'Comicsvibe123!'
snowflake_stage = 'MYSTAGE2'

# Establish Snowflake connection
conn = snowflake.connector.connect(
    account=snowflake_account,
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema,
    user=snowflake_user,
    password=snowflake_password,
    stage=snowflake_stage
)


cursor = conn.cursor()

# Function to create a table in Snowflake based on the CSV file
def create_table_from_csv(conn, csv_file):
    table_name = csv_file.split("/")[-1].split(".")[0]
    print(table_name)

    # Manually set data types for LAST_UPDATE and all other columns as 'object'
    data_types = {'LAST_UPDATE': 'object'}
    df = pd.read_csv(csv_file, nrows=1, dtype=data_types)

    # Map data types based on column values
    data_types = {
        'int64': 'NUMBER',
        'float64': 'NUMBER',
        'object': 'VARCHAR(255)',
    }

    create_table_sql = f"CREATE OR REPLACE TABLE {snowflake_database}.{snowflake_schema}.{table_name} ("

    column_counts = {}  # Dictionary to store column name occurrences

    for col_name, col_type in zip(df.columns, df.dtypes):
        if col_name == 'LAST_UPDATE':
            data_type = 'TIMESTAMP_NTZ(9)'  # For 'LAST_UPDATE' column
        else:
            # Check if the column contains only NULL values
            if df[col_name].isnull().all():
                data_type = 'NUMBER'  # Treat NULL columns as 'NUMBER' data type
            else:
                data_type = data_types.get(str(col_type), 'VARCHAR')  # Default to VARCHAR if type not found

        # Handle 'Null' values in column names
        #col_name = col_name.replace('Null', '_null_')
        
        # Replace spaces in column names with underscores
        col_name = col_name.replace(' ', '_')
        
        # Handle column names starting with a number by adding 'S_' in front
        if col_name[0].isdigit():
            col_name = 'S_' + col_name
        
        # Handle duplicate column names by appending "_1", "_2", etc.
        if col_name in column_counts:
            column_counts[col_name] += 1
            col_name = f"{col_name}_{column_counts[col_name]}"
        else:
            column_counts[col_name] = 1

        create_table_sql += f"{col_name} {data_type}, "

    create_table_sql = create_table_sql.rstrip(', ') + ")"
    print(create_table_sql)

    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        print(f"Table '{table_name}' created successfully.")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error creating table '{table_name}': {e}")
    finally:
        cursor.close()

# Use the Snowflake Python Connector to query the stage and get the list of files
sql = f"LIST @{snowflake_stage}"
cursor.execute(sql)
csv_files = [row[0] for row in cursor.fetchall()]
print(csv_files)

# Create tables based on CSV files with a 5-second delay between each table creation
for csv_file in csv_files:
    csv_file_name = csv_file.split("/")[-2]
    create_table_from_csv(conn, csv_file_name)
    time.sleep(5)  # Add a 5-second delay between table creations

# Close Snowflake connection
conn.close()