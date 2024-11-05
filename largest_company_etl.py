#This is a python code to make an ETL pipeline for a data warehouse
#Importing library
import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import re
import numpy as np

#Data used for the project
url = "https://www.idntimes.com/business/economy/trio-hamdani/lengkap-ini-daftar-100-perusahaan-terbesar-di-indonesia?page=all"
table_attribs = ["No", "Company Name", "Revenue in Rupiah"]
table_name = 'Largest_company_in_Indonesia'
db_name = 'Company.db'
csv_path = r'./Largest_Company_Indonesia.csv'

#Creating log process
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')
        
#Function to turn the revenue into integer
def revenue_transform(revenue):
    parts = revenue.split('.')
    if len(parts) < 3:
        revenue = revenue.replace('.', '')
        return int(revenue) * 1000000 #+ ".000.000"
    else:
        revenue = revenue.replace('.', '')
        return int(revenue) * 1000 #+ ".000"


#Extract function
def extract_data(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    name_of_data = 'split-page split-page1 open has-keypoint'
    div_content = data.find('div', class_=name_of_data)
    #The data used is wraped in a div with a class of 'split-page split-page1 open has-keypoint'
    if div_content:
        paragraphs = div_content.find_all('p')
        for p in paragraphs:
            text = p.text
            if text.startswith("Berikut daftar"):
                continue
    bagian = text.split('.000')
    #Due to the text is not in a table and there is no separator, the .000 will be use as the seperator
    bagian_baru = list(filter(None, bagian))
    data = []
    for perusahaan in bagian_baru:
        match = re.match(r"(\d+)\.\s(.+)\sdengan pendapatan\sRp([\d\.]+)", perusahaan)
        if match:
            nomor = int(match.group(1))
            nama = match.group(2)
            pendapatan = revenue_transform(match.group(3))
            data.append([nomor, nama, pendapatan])
    df = pd.DataFrame(data, columns=table_attribs)
    return df


#Transform
def transform(df):
    df['Revenue in USD'] = [np.round(x / 15735, 2) for x in df['Revenue in Rupiah']]
    return df

#Load
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index=False)

#SQL query
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

#ETL process start
log_progress('Preliminaries complete. Initiating ETL process')

df = extract_data(url, table_attribs)

log_progress('Extract process complete. Initiating Tranform process')

df = transform(df)

log_progress('Data Transformation process complete. Initiating Load process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated')

load_to_db(df, sql_connection, table_name)

log_progress('Data successfully loaded to database')

query_statement = f"SELECT * FROM {table_name}"

run_query(query_statement, sql_connection)

log_progress('Process Complete')

sql_connection.close()

log_progress('Server Connection closed')
#ETL process finished
