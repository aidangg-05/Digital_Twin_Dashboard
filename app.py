import sqlite3
import csv
import json
import pandas as pd
from pymongo import MongoClient

#C:\Digital_Twin\Digital_Twin_Dashboard\HistoricalGroup6.dxpdb

# Path to SQLite database file
database_file = r'C:\Digital_Twin\Digital_Twin_Dashboard\HistoricalGroup5.dxpdb'
#database_file = r'c:\Program Files (x86)\TAKEBISHI\DeviceXPlorer OPC Server 7\Bin\HistoricalGroup5.dxpdb'

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

client = MongoClient('mongodb+srv://JunHui:NJHltpbad@cluster0.oiasqth.mongodb.net/')
# DB name
db = client["test"]
#print(client)


# Push csv file to MongoDB
records = df.keys()
db.testdata.insert_many(df.to_dict('records'))
print(db.testdata.find_one())


# Close the connection
conn.close()