import time
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from pymongo import MongoClient, DESCENDING
import asyncio

# Initialize MongoDB connection
client = MongoClient('mongodb+srv://digitaltwin:digita1_twin@cnc.jvs9vv2.mongodb.net/')
db = client["DigitalTwin"]
collection = db["MotorData"]
chat_id_collection = db["ChatId"]  # Add ChatId collection

# Initialize variables to store the last known value and state
last_node_key_value = None
last_sent_message = None

# Function to store chat ID in the database
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.message.chat_id
    existing_chat = chat_id_collection.find_one({"chat_id": chat_id})
    
    if existing_chat:
        await update.message.reply_text('You have already started receiving notifications.')
    else:
        chat_id_collection.insert_one({"chat_id": chat_id})
        await update.message.reply_text('Hello! You will now receive notifications.')

# Function to send messages to all stored chat IDs
async def send_messages_to_all(application: Application, message: str) -> None:
    for chat_id_doc in chat_id_collection.find({}, {"chat_id": 1}):
        chat_id = chat_id_doc["chat_id"]
        await application.bot.send_message(chat_id=chat_id, text=message)

# Function to check data periodically and send notifications
async def check_and_send_messages(application: Application) -> None:
    global last_node_key_value, last_sent_message
    
    data = collection.find_one({"NodeKey": 47}, sort=[('ServerTimeStamp', DESCENDING)])
    print("Data:", data)
    print("Last node key value:", last_node_key_value)

    if data:
        value = data.get("Value", "").strip()
        
        if value != last_node_key_value:
            last_node_key_value = value
            
            if value == 'EMG Emergency stop EXIN':
                message = 'Emergency Stop Activated'
            elif value == 'Filler':
                message = 'Motor Resuming Operations'
            else:
                message = f"Unknown value: {value}"
            
            if message != last_sent_message:
                await send_messages_to_all(application, message)
                print(f"Sent message: {message}")
                last_sent_message = message
            else:
                print("Value changed, but message already sent. Waiting for next change.")
        else:
            print("Value of NodeKey has not changed. No action required.")

# Function to set up the bot and start polling
async def main() -> None:
    application = Application.builder().token('7043872243:AAHlFOqYhIsIwDDeHtvPWTpUZJRkhKK_aAk').build()
    application.add_handler(CommandHandler("start", start))

    # Start the Bot with polling
    await application.initialize()
    await application.start()
    print("Bot started. Polling...")

    try:
        while True:
            await check_and_send_messages(application)
            await asyncio.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        await application.stop()
        print("Bot stopped.")

if __name__ == '__main__':
    asyncio.run(main())