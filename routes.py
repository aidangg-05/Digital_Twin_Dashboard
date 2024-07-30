from flask import Flask, Blueprint, render_template, jsonify, request, send_file
from pymongo import MongoClient
import threading
import csv
from datetime import datetime


app = Flask(__name__)

# Create a Blueprint object for defining routes
routes = Blueprint('routes', __name__)

# Connect to MongoDB
client = MongoClient('mongodb+srv://digitaltwin:digita1_twin@cnc.jvs9vv2.mongodb.net/')
db = client["DigitalTwin"]
collection = db['FaultData']

# Define route for the index page
@routes.route('/')
def index():
    return render_template('home.html')

# Define route for fetching data
@routes.route('/data')
def data():
    # Get node keys from query parameters
    node_keys = request.args.getlist('NodeKey')
    data_dict = {}

    # Fetch data from MotorData collection in MongoDB for each node key
    for key in node_keys:
        data = list(db.MotorData.find({'NodeKey': int(key)}, {'_id': 0}).sort([('ServerTimeStamp', -1)]).limit(1))
        data_dict[key] = data[0] if data else None

    return jsonify(data_dict)

@routes.route('/download_database_csv')
def download_database_csv():
    csv_file_path = 'CleanedData.csv'

    # Send the CSV file as a response for download
    return send_file(csv_file_path, as_attachment=True)

@app.route('/errordata', methods=['GET'])
def get_error_data():
    # Fetch documents with a valid timestamp
    error_data = list(db.FaultData.find({
        'timestamp': {'$exists': True, '$type': 'string'}
    }, {"_id": 0}))

    # Convert string timestamps to datetime objects for sorting
    for error in error_data:
        error['timestamp'] = datetime.strptime(error['timestamp'], '%Y-%m-%d %H:%M:%S')

    # Sort data by timestamp in descending order
    error_data = sorted(error_data, key=lambda x: x['timestamp'], reverse=True)

    # Convert datetime objects back to strings for JSON response
    for error in error_data:
        error['timestamp'] = error['timestamp'].strftime('%Y-%m-%d %H:%M:%S')

    return jsonify(error_data)

app.register_blueprint(routes)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)