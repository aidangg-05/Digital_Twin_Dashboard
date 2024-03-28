import sqlite3
#from flask import Flask, render_template, jsonify, request, g, Response
#from filetime import to_datetime
#from math import floor
import pandas as pd

#app = Flask(__name__)

# Path to SQLite database file
database_file = r'C:\Digital_Twin\Digital_Twin_Dashboard\HistoricalGroup6.dxpdb'

conn= sqlite3.connect(database_file)
def get_db():
    #db = getattr(g, '_database', None)
   # if db is None:
       # db = g._database = sqlite3.connect(database_file)
   # return db
#db_df = pd.read_sql_query("SELECT * FROM error_log", conn)
#db_df.to_csv('database.csv', index=False)
 #cursor = get_db().cursor()

 # Read SQL database table into a DataFrame
df = pd.read_sql_query("SELECT * FROM HistoricalData", conn)

# Save DataFrame to CSV
df.to_csv('HistoricalData.csv', index=False)

# Close the connection
conn.close()
