import sqlite3
import csv
import json
import pandas as pd
from flask import Flask, render_template, jsonify
from pymongo import MongoClient
from filetime import to_datetime
from hex_to_int import is_hex
from bson import ObjectId
import threading

app = Flask(__name__)


#C:\Digital_Twin\Digital_Twin_Dashboard\HistoricalGroup6.dxpdb

# Path to SQLite database file
database_file = r'C:\Digital_Twin\Digital_Twin_Dashboard\HistoricalGroup8.dxpdb'
database_file = r'c:\Program Files (x86)\TAKEBISHI\DeviceXPlorer OPC Server 7\Bin\HistoricalGroup5.dxpdb'

conn = sqlite3.connect(database_file)
c = conn.cursor()

conn= sqlite3.connect(database_file)

def column_exists(table_name, column_name):
    # Check if the column exists in the table
    c.execute(f"PRAGMA table_info({table_name})")
    columns = c.fetchall()
    for col in columns:
        if col[1] == column_name:
            return True
    return False

def delete_merged_table():
    c.execute('DROP TABLE IF EXISTS MergedData')
    conn.commit()
    print("MergedData table deleted successfully!")
    

delete_merged_table()

def merged():
    c.execute('CREATE TABLE IF NOT EXISTS MergedData AS SELECT * FROM HistoricalData t1 INNER JOIN NodeIdKey t2 ON t1.NodeKey = t2.NodeKey')
    conn.commit()  
    print("Tables merged successfully!")
    

merged()

 # Read SQL database table into a DataFrame
df = pd.read_sql_query("SELECT * FROM MergedData", conn)

# Save DataFrame to CSV
df.to_csv('MergedData.csv', index=False)

# Read the CSV file into a DataFrame
df_merged = pd.read_csv('MergedData.csv')

# Get the number of rows
num_rows = df_merged.shape[0]

print("Number of rows in MergedData.csv:", num_rows)

client = MongoClient('mongodb://localhost:27017/')
# DB name
db = client["DigitalTwin"]



# Sort DataFrame by 'NodeId', 'ServerTimeStamp' and drop Null rows
df_dropped = df_merged.sort_values(by=['NodeId','ServerTimeStamp'], ascending=[True, False])
#df_dropped.dropna(inplace=True)

# Convert 'ServerTimeStamp' and 'SourceTimeStamp' columns to datetime
columns_to_convert = ['ServerTimeStamp', 'SourceTimeStamp']
df_dropped[columns_to_convert] = df_dropped[columns_to_convert].apply(to_datetime)

# Apply the conversion function only to hexadecimal strings in the 'value' column
df_dropped['Value'] = df_dropped['Value'].apply(lambda x: int(x, 16) if isinstance(x, str) and is_hex(x) else x)

df_dropped.to_csv('CleanedData.csv', index=False)

# Insert DataFrame records into MongoDB
db.testdata.insert_many(df_dropped.to_dict('records'))

# Read existing data from MongoDB
existing_data = list(db.MotorData.find({}, {"_id": 0}))

# Convert existing data to DataFrame
df_existing = pd.DataFrame(existing_data)

# Find new rows in the DataFrame to append to MongoDB
new_rows = df_dropped[~df_dropped.apply(tuple, axis=1).isin(df_existing.apply(tuple, axis=1))]

# Convert new rows to dictionary format
new_rows_dict = new_rows.to_dict('records')

# Insert new rows into MongoDB
if new_rows_dict:
    db.MotorData.insert_many(new_rows_dict)
    print("New rows added to MongoDB.")

# Close the connection
conn.close()

