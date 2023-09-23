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

URL = 'https://www.espncricinfo.com/records/format/one-day-internationals-2'
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


def remove_extra_chars(text):
    # Remove extra characters like double quotes from the text
    return text.replace('"', '')

def sanitize_filename(filename):
    # Replace any dashes (-) with underscores (_) in the filename
    filename = filename.replace('-', '_')

    # Add 'S_' prefix if the filename starts with a number
    if filename[0].isdigit():
        filename = f"S_{filename}"

    return filename

def generate_unique_filename(filename):
    # Generate a unique filename to avoid overwriting existing files
    if not os.path.exists(filename):
        return filename
    base, ext = os.path.splitext(filename)
    count = 1
    while os.path.exists(f"{base}_{count}{ext}"):
        count += 1
    return f"{base}_{count}{ext}"

def sanitize_column_name(col):
    # Replace '%' with 'Percentage', '/' with 'Slash', and '+' with 'Plus'
    col = col.replace('%', 'Percentage').replace('/', 'Slash').replace('+', 'Plus')

    # Convert column names with numbers to English spelling
    words = col.split()
    new_words = []
    for word in words:
        try:
            num = w2n.word_to_num(word)
            new_words.append(str(num))
        except ValueError:
            new_words.append(word)
    col = ' '.join(new_words)

    return col

def handle_duplicate_columns(data):
    # Rename columns that have the same name by appending '_1', '_2', etc.
    header = data[0]
    header_count = {col: 1 for col in header}
    for i in range(1, len(header)):
        col = header[i]
        if header.count(col) > 1:
            data[0][i] = f"{col}_{header_count[col]}"
            header_count[col] += 1
    return data

def remove_empty_columns(data):
    # Remove any empty columns from the data
    return [[col for col in row if col] for row in data]

req = requests.get(URL)
soup = bs(req.text, 'html.parser')

links = soup.find_all('a', href=True)

for link in links:
    absolute_url = urljoin(URL, link['href'])
    req = requests.get(absolute_url)
    print(f"Visiting page: {absolute_url}...")  # Print statement with the URL being visited
    soup = bs(req.text, 'html.parser')

    table = soup.find('table')
    if table:
        data = []
        rows = table.find_all('tr')
        for row in rows:
            data.append([remove_extra_chars(cell.get_text(strip=True)) for cell in row.find_all('td')])

        if data:
            # Handle duplicate column names by appending '_1', '_2', etc.
            data = handle_duplicate_columns(data)

            # Remove empty columns from the data
            data = remove_empty_columns(data)

            # Create a unique CSV filename based on the page URL
            csv_filename = f"{sanitize_filename(os.path.basename(absolute_url))}.csv"
            csv_filename = generate_unique_filename(csv_filename)

            # Generate 'uuid' and 'LAST_UPDATE' values in real-time during scraping
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data_with_new_columns = [['uuid', 'LAST_UPDATE'] + [sanitize_column_name(col) for col in data[0]]] + [[str(uuid.uuid4()), current_time] + [col for col in row] for row in data[1:]]

            # Save data to the CSV file
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(data_with_new_columns)

            # Upload the CSV file to Snowflake stage and replace the existing file
            cursor = conn.cursor()
            remove_command = f"REMOVE @{snowflake_stage}/{csv_filename}"
            cursor.execute(remove_command)
            put_command = f"PUT file://{csv_filename} @{snowflake_stage}/{csv_filename}"
            cursor.execute(put_command)
            cursor.close()

            print(f"Table data scraped and saved to '{csv_filename}' and uploaded to Snowflake stage.")
        else:
            print("No table data found on the page.")

    sleep(sleep_delay)

print("All links visited.")

# Close the Snowflake connection
conn.close()