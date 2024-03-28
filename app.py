
from flask import Flask, render_template, jsonify, request, g, Response
from filetime import to_datetime
from math import floor

import sqlite3
database_file = r'C:\Digital_Twin\Digital_Twin_Dashboard\HistoricalGroup6.dxpdb'

conn = sqlite3.connect(database_file)
c = conn.cursor

def merged():
    c.execute('CREATE TABLE table3 as SELECT * FROM HistoricalData t1 INNER JOIN NodeIdKey t2 ON t1.NodeKey = t2.NodeKey')\
    for line in c.fetchall():
        print(line)
merged()


