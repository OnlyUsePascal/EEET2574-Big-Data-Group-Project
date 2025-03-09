import asyncio
import os
import time
import json  
import boto3
from pymongo import MongoClient
import requests
import aiohttp
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB URI from environment variable
uri = os.getenv("MONGO_URI")
access_token = os.getenv('WEATHER_API_ACCESS_TOKEN')
AWS_ACCESS_ID = os.getenv('aws_access_key_id')
AWS_ACCESS_KEY = os.getenv('aws_secret_access_key')
AWS_SESSSION_TOKEN = os.getenv('aws_session_token')

firehose_stream = 'RAW-AIR-cpFmY'
firehose = boto3.client(
    service_name = 'firehose', 
    region_name = 'us-east-1',
    aws_access_key_id = AWS_ACCESS_ID,
    aws_secret_access_key = AWS_ACCESS_KEY,
    aws_session_token = AWS_SESSSION_TOKEN)

# Function to connect to MongoDB
def connect_to_db():
    client = MongoClient(uri)
    print("Connected to MongoDB")
    db = client['ASM3'] 
    air_collection = db['air_raw'] 
    return air_collection

# Connect to the Real-time Air Quality data feed
base_url = "http://api.weatherapi.com/v1/"
async def get_air(city):
    """Fetch the current air data from Real-time Air Quality data feed."""
    async with aiohttp.ClientSession() as session:
        url = f"{base_url}current.json?key={access_token}&q=${city}&aqi=yes"
        async with session.get(url) as response:
            if response.status == 200:
                air_data = await response.json()
                return air_data
            else:
                print(f"Error: {response.status} - {await response.text()}")
                return None

# def insert_to_mongo(air_collection, air_data):
#     """Insert air data into MongoDB."""
#     try:
#         # Insert air data into the MongoDB collection
#         air_collection.insert_one(air_data)
#         print("Data inserted successfully.")
#     except Exception as e:
#         print(f"Error inserting data into MongoDB: {e}")



def update_firehose(air_data):
    print('>', json.dumps(air_data))
    response = firehose.put_record(
		DeliveryStreamName = firehose_stream,
		Record={
			'Data': json.dumps(air_data)
		}
	)
    print(response, '\n')



async def run(air_collection):
    """Main function to fetch and store air data in a loop."""
    cities = ["Ho Chi Minh", "Da Nang","Ha Noi"]
    iterator = 0
    cooldown = 15

    while True:
        for city in cities:
            response = await get_air(city)
            if response:
                # Add additional data like the location name and timestamp
                air_data = response
                air_data["report_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                # Insert the data into MongoDB
                update_firehose(air_data)

            # Sleep between location requests
        await asyncio.sleep(cooldown)

if __name__ == "__main__":
    air_collection = connect_to_db()
    asyncio.run(run(air_collection))