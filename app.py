import sqlite3
import csv
import json
import pandas as pd
from pymongo import MongoClient

#C:\Digital_Twin\Digital_Twin_Dashboard\HistoricalGroup6.dxpdb

# Path to SQLite database file
<<<<<<< HEAD
#database_file = r'C:\Digital_Twin\Digital_Twin_Dashboard\HistoricalGroup5.dxpdb'
database_file = r'c:\Program Files (x86)\TAKEBISHI\DeviceXPlorer OPC Server 7\Bin\HistoricalGroup5.dxpdb'
=======
database_file = r'C:\Digital_Twin\Digital_Twin_Dashboard\HistoricalGroup5.dxpdb'
#database_file = r'c:\Program Files (x86)\TAKEBISHI\DeviceXPlorer OPC Server 7\Bin\HistoricalGroup5.dxpdb'
>>>>>>> d2c5a1f9805a248532dec7ede85cedfd7d8a03af

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

<<<<<<< HEAD
client = MongoClient('mongodb://localhost:27017')
# DB name
db = client["DigitalTwin"]
#print(client)

db.MotorData.delete_many({})

# Push csv file to MongoDB
records = df.keys()
db.MotorData.insert_many(df.to_dict('records'))



=======
client = MongoClient()
# DB name
db = client["test"]
#print(client)
print(df_merged.shape)

# Drops rows "0"
df_dropped = df_merged.drop(df_merged[df_merged.eq('0').any(axis=1)].index)
print(df_dropped.shape)


db.testdata.delete_many({})


#Push csv file to MongoDB
records = df.keys()
db.testdata.insert_many(df_dropped.to_dict('records'))
#print(db.testdata.find_one())


#df.to_csv (r'C:\Digital_Twin\Digital_Twin_Dashboard\amendedData.csv', index = None, header=True) 
#print('Data has been updated')
>>>>>>> d2c5a1f9805a248532dec7ede85cedfd7d8a03af
# Close the connection
conn.close()