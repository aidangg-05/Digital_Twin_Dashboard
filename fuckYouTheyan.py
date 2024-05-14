from pymongo import MongoClient
import time
from pymongo import DESCENDING

# Connect to the MongoDB database
client = MongoClient('mongodb+srv://digitaltwin:digita1_twin@cnc.jvs9vv2.mongodb.net/')
db = client["DigitalTwin"]
collection = db["MotorData"]

# Initialize an empty list to store the values
newest_values = []

# Function to retrieve the newest value for NodeKey 1
def get_newest_value():
    # Query MongoDB for the newest document with NodeKey 1
    newest_doc = collection.find_one({"NodeKey": 1}, sort=[('ServerTimeStamp', DESCENDING)])
    if newest_doc:
        newest_value = newest_doc.get("Value")
        return newest_value
    else:
        return None

# Main loop to continuously check for new values
while True:
    # Get the newest value
    newest_value = get_newest_value()
    if newest_value is not None:
        # Append the newest value to the list
        newest_values.append(newest_value)
        print("Newest value:", newest_value)
    else:
        print("No new value found.")

    # Wait for 1 second before checking again
    time.sleep(1)
