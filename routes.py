from flask import Flask, Blueprint, render_template, jsonify
from pymongo import MongoClient

app = Flask(__name__)

# Create a Blueprint object for defining routes
routes = Blueprint('routes', __name__)

# Connect to MongoDB
client = MongoClient('mongodb+srv://digitaltwin:digita1_twin@cnc.jvs9vv2.mongodb.net/')
db = client["DigitalTwin"]



# Define route for the index page
@routes.route('/')
def index():
    return render_template('home.html')

# Define route for fetching data
@routes.route('/data')
def data():
    # Fetch data from MotorData collection in MongoDB
    data = list(db.MotorData.find({}, {'_id': 0}).sort([('_id', -1)]).limit(10))  # Get last 10 records
    print("Data found")
    # Convert ObjectId to string for each document
    for item in data:
        item['_id'] = str(item.get('_id'))  # Convert ObjectId to string
    return jsonify(data)

app.register_blueprint(routes)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)