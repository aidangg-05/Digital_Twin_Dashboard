import xmlrpc.client as xml
import pandas as pd
import matplotlib.pyplot as plt
import time 
import numpy as np
import csv
import pymongo
from pymongo import MongoClient
import uuid
import os
from datetime import datetime, timezone

MAX_ROWS = 100  # Define the maximum number of rows to keep in the CSV file

model = 'brushless_dc_machine_New'  # model name
normal_model = 'brushless_dc_machine_New'
Server = xml.Server("http://localhost:1080/RPC2").plecs


# Run simulations and pull the data
SimulationValues = Server.simulate(model)
time_df = pd.DataFrame(SimulationValues['Time'], columns=['Time'])  # time

current1 = pd.DataFrame(SimulationValues['Values'][0], columns=['Current1'])  # values
current2 = pd.DataFrame(SimulationValues['Values'][1], columns=['Current2'])  # values
current3 = pd.DataFrame(SimulationValues['Values'][2], columns=['Current3'])  # values
rotor_speed = pd.DataFrame(SimulationValues['Values'][4], columns=['RotorSpeed'])  # values
print('Done')

SimulationVal = Server.simulate(normal_model)  # run simulation and pull the data
normal_current1 = pd.DataFrame(SimulationVal['Values'][0], columns=['NormalCurrent1'])  # values
normal_current2 = pd.DataFrame(SimulationVal['Values'][1], columns=['NormalCurrent2'])  # values
normal_current3 = pd.DataFrame(SimulationVal['Values'][2], columns=['NormalCurrent3'])  # values
normal_rotor_speed = pd.DataFrame(SimulationVal['Values'][4], columns=['NormalRotorSpeed'])  # values

overcurrent_current = current2
cell_value = rotor_speed.iloc[0].values[0]
cell_value1 = normal_rotor_speed.iloc[0].values[0]
average_speed = rotor_speed.mean().values[0]
average_speed1 = normal_rotor_speed.mean().values[0]
max_rotor_speed = rotor_speed.max().values[0]
max_rotor_speed1 = normal_rotor_speed.max().values[0]
max_current = round(current1.max().values[0], 5)
max_current1 = round(normal_current1.max().values[0], 5)
max_current2 = round(current3.max().values[0], 5)
currents_of_3_phases = pd.concat([current1, current2, current3], axis=1)
max_currents_of_3_phases = currents_of_3_phases.max().max()

def check_instantaneous_power_off(time, currents):
    time1 = None
    zero_flag = False
    power_off_flag = False

    for index, row in currents.iterrows():
        if power_off_flag:
            break
        else:
            # Check if all three currents are zero
            if (row == 0).all():
                if not zero_flag:
                    time1 = time.iloc[index].values[0]  # Record the time when all three currents are zero
                    zero_flag = True
                else:
                    # Check if the same condition persists for 0.05 seconds
                    if time.iloc[index].values[0] >= time1 + 0.05:
                        power_off_flag = True  # Set the flag to indicate power off has been detected
            else:
                zero_flag = False  # Reset zero_flag if any of the three currents is not zero
                time1 = None

    return power_off_flag

power_off_flag = check_instantaneous_power_off(time_df, currents_of_3_phases)

if (max_current > max_current1) and not (
    (current1 == 0).all().values[0] and 
    (current2 == 0).all().values[0] and 
    (current3 == 0).all().values[0]
) and (cell_value == cell_value1):
    fault = "Overcurrent"
elif (max_current == max_current1) and (
    (current1 == 0).all().values[0] or 
    (current2 == 0).all().values[0] or 
    (current3 == 0).all().values[0]
):
    fault = "Single phase open circuit"
elif (max_currents_of_3_phases > max_current1) and (
    (current1 == 0).all().values[0] or 
    (current2 == 0).all().values[0] or 
    (current3 == 0).all().values[0]
):
    fault = "Phase to phase short circuit"
elif cell_value > 300:
    fault = "Overspeed"
elif (average_speed < average_speed1) and (max_current == max_current1) and max_current2 == max_current1 and not power_off_flag:
    fault = "Abnormally high resistance"
elif (average_speed > average_speed1) and (max_rotor_speed > max_rotor_speed1):
    fault = "Overvoltage"
elif power_off_flag:
    fault = "Instantaneous power off"
elif average_speed < average_speed1 and max_current2 > max_current1:
    fault = "Insufficient voltage"
else:
    fault = "no fault"

# Check if the detected fault is "no fault"
if fault == "no fault":
    # Clear the CSV file
    with open('faultDetection.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['id', 'error'])
    print("CSV file cleared.")

    # Connect to MongoDB
    client = MongoClient('mongodb+srv://digitaltwin:digita1_twin@cnc.jvs9vv2.mongodb.net/')
    db = client["DigitalTwin"]
    collection = db["FaultData"]

    # Remove all documents from the collection
    collection.delete_many({})
    print("All documents removed from MongoDB.")
else:
    # Generate a unique ID
    unique_id = str(uuid.uuid4())

    # Check if the CSV file already exists
    file_exists = os.path.exists('faultDetection.csv')

    # Write to CSV with unique ID
    with open('faultDetection.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(['id', 'error'])
        writer.writerow([unique_id, fault])
        print(f"Written to CSV: {unique_id}, {fault}")

    # Read the CSV file into a DataFrame
    df_merged = pd.read_csv('faultDetection.csv')

    # If the DataFrame exceeds the maximum number of rows, truncate it
    if len(df_merged) > MAX_ROWS:
        df_merged = df_merged.tail(MAX_ROWS)

    # Write the truncated DataFrame back to the CSV file
    df_merged.to_csv('faultDetection.csv', index=False)
    print("CSV file truncated to the most recent rows.")

    # Connect to MongoDB
    client = MongoClient('mongodb+srv://digitaltwin:digita1_twin@cnc.jvs9vv2.mongodb.net/')
    db = client["DigitalTwin"]
    collection = db["FaultData"]

    # Add timestamps to documents without a timestamp
    collection.update_many(
        {'timestamp': {'$exists': False}},
        {'$set': {'timestamp': datetime.now(timezone.utc)}}
    )

    # Remove duplicates, keeping only the latest entry
    pipeline = [
        {
            '$group': {
                '_id': '$error',
                'latestId': {'$last': '$_id'},
                'latestTimestamp': {'$last': '$timestamp'},
                'docs': {'$push': '$_id'}
            }
        },
        {
            '$project': {
                'docs': {
                    '$setDifference': ['$docs', ['$latestId']]
                }
            }
        }
    ]

    results = list(collection.aggregate(pipeline))

    for result in results:
        if result['docs']:
            collection.delete_many({'_id': {'$in': result['docs']}})

    print("Duplicates cleaned up successfully.")

    # Verify documents in MongoDB
    documents = list(collection.find())
    for doc in documents:
        print(doc)

    # Read existing data from MongoDB
    existing_data = list(db.FaultData.find({}, {"_id": 0}))

    # Convert existing data to DataFrame
    df_existing = pd.DataFrame(existing_data)
    print(df_existing.tail())  # Print the last few rows to verify

    # Find new rows in the DataFrame to append to MongoDB
    new_rows = df_merged[~df_merged.apply(tuple, axis=1).isin(df_existing.apply(tuple, axis=1))]

    # Debugging step: Print new rows to be added to MongoDB
    print("New rows to be added to MongoDB:")
    print(new_rows)

    # Convert new rows to dictionary format
    new_rows_dict = new_rows.to_dict('records')

    # Insert new rows into MongoDB
    if new_rows_dict:
        db.FaultData.insert_many(new_rows_dict)
        print("New rows added to MongoDB.")
    else:
        print("No new rows to add.")

    # Final cleanup function
    def delete_oldest_records():
        # Count the total number of records
                total_records = collection.count_documents({})

        # If there are more than 1500 records, delete the oldest 500
                if total_records > 1500:
                    oldest_records = collection.find({}, {"_id": 1}).sort([("_id", pymongo.ASCENDING)]).limit(500)
                    oldest_ids = [record["_id"] for record in oldest_records]
                    collection.delete_many({"_id": {"$in": oldest_ids}})
                    print("Oldest 500 records deleted.")

    # Call the function to delete oldest records whenever there are more than 1500 records
    delete_oldest_records()

    # Add timestamps to documents without a timestamp (again, just in case)
    collection.update_many(
        {'timestamp': {'$exists': False}},
        {'$set': {'timestamp': datetime.now(timezone.utc)}}
    )

    # Remove any remaining duplicates, keeping only the latest entry
    pipeline = [
        {
            '$group': {
                '_id': '$error',
                'latestId': {'$last': '$_id'},
                'latestTimestamp': {'$last': '$timestamp'},
                'docs': {'$push': '$_id'}
            }
        },
        {
            '$project': {
                'docs': {
                    '$setDifference': ['$docs', ['$latestId']]
                }
            }
        }
    ]

    results = list(collection.aggregate(pipeline))

    for result in results:
        if result['docs']:
            collection.delete_many({'_id': {'$in': result['docs']}})

    print("Final cleanup complete.")

    time.sleep(3)

    # Verify documents in MongoDB after final cleanup
    documents = list(collection.find())
    for doc in documents:
        print(doc)

    print("All documents verified. No duplicates or missing timestamps.")
