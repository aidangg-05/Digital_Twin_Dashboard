import time
import threading
import sqlite3
import csv
import time
import json
import pymongo 
import pandas as pd
from flask import Flask, render_template, jsonify
from pymongo import MongoClient
from filetime import to_datetime
from hex_to_int import is_hex
from bson import ObjectId
import threading
from routes import routes
import re

# Function to check database connection status
def check_db_connection():
    while True:
        try:
            # Attempt to connect to MongoDB
            client = MongoClient('mongodb+srv://digitaltwin:digita1_twin@cnc.jvs9vv2.mongodb.net/')
            db = client["DigitalTwin"]
            collection = db["MotorData"]
            # If connection successful, break the loop
            break
        except Exception as e:
            print("Failed to connect to database. Retrying in 5 seconds...")
            time.sleep(5)

# Start a thread to check database connection
connection_thread = threading.Thread(target=check_db_connection)
connection_thread.daemon = True
connection_thread.start()

# Wait for database connection to be established
connection_thread.join()

#C:\Digital_Twin\Digital_Twin_Dashboard\HistoricalGroup6.dxpdb

# Path to SQLite database file
#database_file = r'C:\Digital_Twin\Digital_Twin_Dashboard\HistoricalGroup8.dxpdb'
database_file = r'c:\Program Files (x86)\TAKEBISHI\DeviceXPlorer OPC Server 7\Bin\HistoricalGroup5.dxpdb'

conn = sqlite3.connect(database_file)
c = conn.cursor()

conn= sqlite3.connect(database_file)

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

print("debug1")



# Sort DataFrame by 'NodeId', 'ServerTimeStamp' and drop Null rows
df_dropped = df_merged.sort_values(by=['NodeId','ServerTimeStamp'], ascending=[True, False])
df_dropped = df_dropped.dropna(subset=['Value'])

#df_dropped.dropna(inplace=True)

# Convert 'ServerTimeStamp' and 'SourceTimeStamp' columns to datetime
columns_to_convert = ['ServerTimeStamp', 'SourceTimeStamp']
df_dropped[columns_to_convert] = df_dropped[columns_to_convert].apply(to_datetime)

# Apply the conversion function only to hexadecimal strings in the 'value' column
df_dropped['Value'] = df_dropped['Value'].apply(lambda x: int(x, 16) if isinstance(x, str) and is_hex(x) else x)
# Remove unwanted commas and whitespaces
df_dropped['Value'] = df_dropped['Value'].apply(lambda x: ', '.join(re.findall(r'[^\s,](?:[^,]*[^\s,])?', str(x))) if isinstance(x, str) else x)
df_dropped['Value'] = df_dropped['Value'].apply(lambda x: re.sub(r'\s+', ' ', str(x)) if isinstance(x, str) else x)
df_dropped['Value'] = df_dropped['Value'].replace('', 'Filler')

df_dropped.to_csv('CleanedData.csv', index=False)


print("debug2")

# Add this after your existing imports and setup
last_update_time = time.time()

def update_last_update_time():
    global last_update_time
    last_update_time = time.time()

def is_database_updating():
    # Check if the database has been updated in the last minute
    return time.time() - last_update_time < 60

@app.route('/update_status')
def update_status():
    return jsonify({'updating': is_database_updating()})

def delete_oldest_records():
    # Count the total number of records
    total_records = collection.count_documents({})

    # If there are more than 1500 records, delete the oldest 500
    if total_records > 1500:
        oldest_records = collection.find({}, {"_id": 1}).sort([("_id", pymongo.ASCENDING)]).limit(500)
        oldest_ids = [record["_id"] for record in oldest_records]
        collection.delete_many({"_id": {"$in": oldest_ids}})
        print("Oldest 500 records deleted.")
        update_last_update_time()


# Call the function to delete oldest records whenever there are 1500 records
delete_oldest_records()

# Read existing data from MongoDB
existing_data = list(db.MotorData.find({}, {"_id": 0}))

# Convert existing data to DataFrame
df_existing = pd.DataFrame(existing_data)

# Find new rows in the DataFrame to append to MongoDB
new_rows = df_dropped[~df_dropped.apply(tuple, axis=1).isin(df_existing.apply(tuple, axis=1))]

# Convert new rows to dictionary format
new_rows_dict = new_rows.to_dict('records')


print("debug3")


# Insert new rows into MongoDB
if new_rows_dict:
    db.MotorData.insert_many(new_rows_dict)
    print("New rows added to MongoDB.")
    update_last_update_time()


print("debug4")


# Close the connection
conn.close()