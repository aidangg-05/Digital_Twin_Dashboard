import threading
import time
from telegram.ext import Updater, CommandHandler
from pymongo import MongoClient
from pymongo import DESCENDING

# Initialize MongoDB connection
client = MongoClient('mongodb+srv://digitaltwin:digita1_twin@cnc.jvs9vv2.mongodb.net/')
db = client["DigitalTwin"]
collection = db["MotorData"]
chat_id_collection = db["ChatId"]  # Add ChatId collection

# Function to store chat ID in the database
def start(update, context):
    # Retrieve the chat ID
    chat_id = update.message.chat_id
    
    # Check if the chat ID already exists in the database
    existing_chat = chat_id_collection.find_one({"chat_id": chat_id})
    
    if existing_chat:
        update.message.reply_text('You have already started receiving notifications.')
    else:
        # Insert the chat ID into the ChatId collection
        chat_id_collection.insert_one({"chat_id": chat_id})
        update.message.reply_text('Hello! You will now receive notifications.')

# Function to send messages to all stored chat IDs
def send_messages_to_all(message, context):
    for chat_id_doc in chat_id_collection.find({}, {"chat_id": 1}):
        chat_id = chat_id_doc["chat_id"]
        context.bot.send_message(chat_id=chat_id, text=message)

# Initialize a variable to store the last known value of NodeKey
last_node_key_value = None

# Function to check data periodically and send notifications
def check_and_send_messages(updater):
    global last_node_key_value
    
    # Query MongoDB for data with NodeKey:47 and sort by ServerTimeStamp in descending order
    data = collection.find_one({"NodeKey": 47}, sort=[('ServerTimeStamp', DESCENDING)])
    
    if data:
        value = data.get("Value", "").strip()
        
        # Check if the value of NodeKey has changed
        if value != last_node_key_value:
            last_node_key_value = value  # Update the last known value
            
            if value == 'EMG Emergency stop EXIN':
                send_messages_to_all(updater, 'Emergency Stop Activated')  # Pass updater instead of context
            elif value == 'EMG Emergency stop SRV':
                send_messages_to_all(updater, 'Motor Resuming Operations')  # Pass updater instead of context
            else:
                print("Unknown value:", value)
        else:
            print("Value of NodeKey has not changed. No action required.")
            
# Function to send messages to all stored chat IDs
def send_messages_to_all(updater, message):
    for chat_id_doc in chat_id_collection.find({}, {"chat_id": 1}):
        chat_id = chat_id_doc["chat_id"]
        updater.bot.send_message(chat_id=chat_id, text=message)

# Function to set up the bot and start polling
def main():
    # Set up the updater and dispatcher
    updater = Updater('7043872243:AAHlFOqYhIsIwDDeHtvPWTpUZJRkhKK_aAk', use_context=True)
    dp = updater.dispatcher

    # Add handlers
    dp.add_handler(CommandHandler("start", start))

    # Start the Bot
    updater.start_polling()

    try:
        # Start a separate thread to periodically check for changes in data and send notifications
        while True:
            check_and_send_messages(updater)
            time.sleep(2)  # Check every minute
    except KeyboardInterrupt:
        # Stop the updater gracefully
        updater.stop()
        print("Bot stopped.")

if __name__ == '__main__':
    main()