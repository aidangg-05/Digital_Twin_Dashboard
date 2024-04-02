import sqlite3
import csv
import json
import pandas as pd
from pymongo import MongoClient
from filetime import to_datetime
from hex_to_int import is_hex

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

#Input Connection String
client = MongoClient()
# DB name
db = client["test"]
#print(client)
print(df_merged.shape)


# Sort DataFrame by 'NodeId'
df_dropped = df_merged.sort_values(by=['NodeId','ServerTimeStamp'], ascending=[True, False])

# Get top 5 rows for each unique NodeIds
df_dropped = df_dropped.groupby('NodeId').head(20)

# Convert 'ServerTimeStamp' and 'SourceTimeStamp' columns to datetime
columns_to_convert = ['ServerTimeStamp', 'SourceTimeStamp']
df_dropped[columns_to_convert] = df_dropped[columns_to_convert].apply(to_datetime)

# Apply the conversion function only to hexadecimal strings in the 'value' column
df_dropped['Value'] = df_dropped['Value'].apply(lambda x: int(x, 16) if isinstance(x, str) and is_hex(x) else x)

# Delete existing data in MongoDB collection
db.testdata.delete_many({})

df_dropped.to_csv('CleanedData.csv', index=False)

# Insert DataFrame records into MongoDB
db.testdata.insert_many(df_dropped.to_dict('records'))

# Close the connection
conn.close()