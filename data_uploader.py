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


# Create a cursor to execute SQL statements
cursor = conn.cursor()

# List the target table names in your Snowflake schema
cursor.execute(f"SHOW TABLES IN {snowflake_schema}")
table_names = [row[1] for row in cursor.fetchall()]
print(table_names)

# List the CSV files in the Snowflake stage
cursor.execute(f"LIST @{snowflake_stage}")
FILE_NAMES = [row[0] for row in cursor.fetchall()]
print(FILE_NAMES)

# Upload the CSV data from the Snowflake stage to the matching tables in the schema
for file_name in FILE_NAMES:
    # Remove the file extension from the CSV file name to compare with table names
    file_name_without_ext = file_name.split("/")[-2]
    print(file_name_without_ext)
    file_name_without_ext1 = file_name_without_ext.split(".")[0]
    print(file_name_without_ext1)
    file_name_upper = file_name_without_ext1.upper()

    # Check if the file name matches any of the table names
    if file_name_upper in table_names:
        try:
            # Use the TRUNCATE command to delete all records from the table
            truncate_query = f"TRUNCATE TABLE {snowflake_schema}.{file_name_upper}"
            cursor.execute(truncate_query)

            # Use the COPY INTO command to load data from the stage to the table
            copy_into_query = f"""
            COPY INTO {snowflake_schema}.{file_name_upper}
            FROM @{snowflake_stage}/{file_name_without_ext}
            FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE, NULL_IF = ('-'), VALIDATE_UTF8 = TRUE)
            ON_ERROR = 'CONTINUE'
            """
            print(copy_into_query)
            cursor.execute(copy_into_query)
            print(f"Data uploaded from {file_name} to {snowflake_schema}.{file_name_upper}.")
        except snowflake.connector.errors.ProgrammingError as e:
            # Catch the exception when COPY INTO reaches the end of the CSV file
            # (Error code 100080: Number of columns in file does not match that of the corresponding table)
            if e.errno == 100080:
                print(f"Finished loading data from {file_name} into {snowflake_schema}.{file_name_upper}.")
            else:
                # If it's a different ProgrammingError, raise it to handle other potential issues
                raise e

# Commit the changes
conn.commit()

# Close the cursor and connection
cursor.close()