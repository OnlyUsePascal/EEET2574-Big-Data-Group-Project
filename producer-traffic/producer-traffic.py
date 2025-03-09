import asyncio
import base64
import configparser
import json
import os
import time
import datetime
from urllib import response
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
import boto3
import requests

load_dotenv()
TOMTOM_KEY = os.getenv('TOMTOM_KEY')
CLUSTER_URL = os.getenv('CLUSTER_URL')
AWS_ACCESS_ID = os.getenv('aws_access_key_id')
AWS_ACCESS_KEY = os.getenv('aws_secret_access_key')
AWS_SESSSION_TOKEN = os.getenv('aws_session_token')

# firehose
firehose_stream = 'RAW-TRAFFIC-Eo2oK'
firehose = boto3.client(
    service_name = 'firehose', 
    region_name = 'us-east-1',
    aws_access_key_id = AWS_ACCESS_ID,
    aws_secret_access_key = AWS_ACCESS_KEY,
    aws_session_token = AWS_SESSSION_TOKEN)

RAW_DB = 'ASM3'
RAW_COLL = 'traffic_raw'

dbClient = MongoClient(CLUSTER_URL)
collClient = dbClient\
    .get_database(RAW_DB)\
    .get_collection(RAW_COLL)
# TRAFFIC_COLLECTION = RAW_DB.traffic

def fetch_incidents(city_bbox, city_name):
    minLon, minLat, maxLon, maxLat = city_bbox
    url = (f"https://api.tomtom.com/traffic/services/5/incidentDetails"
            + f"?key={TOMTOM_KEY}"
            + "&fields={incidents{type,geometry{type,coordinates},properties{id,iconCategory,magnitudeOfDelay,events{description,code,iconCategory},startTime,endTime,from,to,length,delay,roadNumbers,timeValidity,probabilityOfOccurrence,numberOfReports,lastReportTime,tmc{countryCode,tableNumber,tableVersion,direction,points{location,offset}}}}}"
            + "&language=en-GB"
            + f"&bbox={minLon},{minLat},{maxLon},{maxLat}"
            + "&categoryFilter=1,2,4,6,8,9,10,11"
            # + "&fields={incidents{type,geometry{type,coordinates},properties{id,iconCategory,magnitudeOfDelay,events{description,code,iconCategory},startTime,endTime,from,to,length,delay,roadNumbers,timeValidity,probabilityOfOccurrence,numberOfReports,lastReportTime}}}"\
            )

    # print(url)
    res = requests.get(url)
    data_json = res.json()["incidents"]
    # add city name
    for incident in data_json:
        incident["properties"]["city"] = city_name
    return data_json
    

# def upload_incidents(incidents_hist):
#     update_requests = []
#     # print(incidents_hist)
    
#     # update incidents + upload new if not exists
#     for incidents in incidents_hist:
#         for incident in incidents:
#             incident["_id"] = incident["properties"]["id"]
#             update_requests.append(
#                 UpdateOne({"_id": incident["_id"]}, {"$set": incident}, upsert=True)
#             )
    
#     # write to db
#     collClient.bulk_write(update_requests)


def update_firehose(traffic_data):
    print('> Uploading to firehose...')
    
    # extract incidents
    records = []
    for incidents in traffic_data:
        for incident in incidents:
            incident["_id"] = incident["properties"]["id"]
            records.append(incident)

    # upload by batch
    batch_sz = 100
    while len(records) > 0:
        records_batch = records[:batch_sz]
        records = records[batch_sz:]
        
        records_formatted = [{
            'Data' : json.dumps(record).encode()}
            for record in records_batch
        ]
        response = firehose.put_record_batch(
            DeliveryStreamName = firehose_stream,
            Records=records_formatted       
        )

        time.sleep(5)

def run():
    incidents_hist = []
    cities_bbox = {
        "Ho Chi Minh City": [106.532129, 10.66594, 106.831575, 10.883411],
        "Hanoi": [105.2849, 20.5645, 106.0201, 21.3853],
        "Da Nang": [107.818782, 15.917955, 108.574858, 16.344307],
        # "Hai Phong": [106.4005, 20.2208, 107.1187, 21.0203],
        # "Can Tho": [105.225678, 9.919531, 105.845472, 10.325209],
    }
    cities = list(cities_bbox.keys())
    delay_fetch = 60 / len(cities)
    delay_upload = 60 * 5
    # delay_fetch = 5 
    # delay_upload = 20
    iterator = 0
    last_fetch = datetime.datetime.now()

    while True:
        try:
            # iterate city
            city = cities[iterator]
            print(f'> Fetching incidents for {city}')
            incidents_hist.append(
                fetch_incidents(cities_bbox[city], city))
            
            # delay between upload
            now_ = datetime.datetime.now()
            if (now_ - last_fetch).seconds > delay_upload:
                last_fetch = datetime.datetime.now()
                update_firehose(incidents_hist)
                incidents_hist = []

        except Exception as err:
            print('> Something went wrong !')
            print(err)

        # delay between fetch
        finally:
            time.sleep(delay_fetch)
            print("> Waking up!")
            iterator = (iterator + 1) % len(cities)


if __name__ == '__main__':
    run()