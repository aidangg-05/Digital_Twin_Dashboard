import sqlite3

import pandas as pd

#app = Flask(__name__)

# Path to SQLite database file
database_file = r'C:\Digital_Twin\Digital_Twin_Dashboard\HistoricalGroup6.dxpdb'

conn = sqlite3.connect(database_file)
c = conn.cursor()

conn= sqlite3.connect(database_file)

<<<<<<< HEAD
=======
def column_exists(table_name, column_name):
    # Check if the column exists in the table
    c.execute(f"PRAGMA table_info({table_name})")
    columns = c.fetchall()
    for col in columns:
        if col[1] == column_name:
            return True
    return False

>>>>>>> 86e6a354921a092e4224e42f6bd9d3d621c6d911
def merged():
    c.execute('CREATE TABLE IF NOT EXISTS MergedData AS SELECT * FROM HistoricalData t1 INNER JOIN NodeIdKey t2 ON t1.NodeKey = t2.NodeKey')
    conn.commit()  
    print("Tables merged successfully!")
<<<<<<< HEAD
    c.execute('ALTER TABLE MergedData DROP COLUMN "NodeKey:1"')  # Use double quotes to handle special characters
    conn.commit()
    print("Column 'NodeKey:1' dropped successfully!")
=======
    
    # Check if the column exists before attempting to drop it
    if column_exists("MergedData", "NodeKey:1"):
        c.execute('ALTER TABLE MergedData DROP COLUMN "NodeKey:1"')  # Use double quotes to handle special characters
        conn.commit()
        print("Column 'NodeKey:1' dropped successfully!")
    else:
        print("Column 'NodeKey:1' does not exist. Skipping drop operation.")
>>>>>>> 86e6a354921a092e4224e42f6bd9d3d621c6d911

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

# Close the connection
conn.close()
