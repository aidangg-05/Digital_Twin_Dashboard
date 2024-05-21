from flask import Flask, Blueprint, render_template, jsonify, request, send_file
from pymongo import MongoClient
import threading
import csv


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

@app.route('/log_error', methods=['POST'])
def log_error():
    error_data = request.get_json()
    error_type = error_data.get('error')
    timestamp = datetime.utcnow()

    if not error_type:
        return jsonify({'error': 'Invalid error data'}), 400

    # Update existing error of the same type or insert if it doesn't exist
    result = collection.update_one(
        {'error': error_type},
        {'$set': {'timestamp': timestamp}},
        upsert=True
    )

    if result.matched_count > 0:
        return jsonify({'message': 'Error timestamp updated successfully'}), 200
    else:
        return jsonify({'message': 'New error logged successfully'}), 201

@app.route('/errordata', methods=['GET'])
def get_error_data():
    errors = list(collection.find({}, {'_id': 0, 'error': 1, 'timestamp': 1}))
    return jsonify(errors)

app.register_blueprint(routes)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)