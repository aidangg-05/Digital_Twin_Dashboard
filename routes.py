from flask import Blueprint, render_template, jsonify
from pymongo import MongoClient

# Create a Blueprint object for defining routes
routes = Blueprint('routes', __name__)

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client["DigitalTwin"]

# Define route for the index page
@routes.route('/')
def index():
    return render_template('index.html')

# Define route for fetching data
@routes.route('/data')
def data():
    # Fetch data from MotorData collection in MongoDB
    data = list(db.MotorData.find({}, {'_id': 0}).sort([('_id', -1)]).limit(10))  # Get last 10 records
    # Convert ObjectId to string for each document
    for item in data:
        item['_id'] = str(item.get('_id'))  # Convert ObjectId to string
    return jsonify(data)