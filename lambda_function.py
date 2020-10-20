#!/bin/env python
import json
import requests
import boto3
import pprint
import logging
import datetime
from pytz import timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen
#from datetime import datetime





def find_bot_id(bot_name):
    for k,v in bot_ids.items():
        if k == bot_name:
            return(v)
    print("no bot ID found")
    quit()

with open("creds.json", encoding='utf-8') as f:
    credentials = json.load(f)
    airtable_api_key = credentials["airtable_api_key"]
    airtable_api_url = credentials["airtable_api_url"]
    groupme_bot_id = credentials["groupme_bot_id"]
    bot_ids = credentials["bot_ids"]


def get_airtable_records(airtable_api_key):
    headers = {'Authorization': 'Bearer {}'.format(airtable_api_key)}
    parameters = {}
    r = requests.get('{}/Table%201'.format(airtable_api_url), headers=headers)

    response = r.json()
    return(response)

def find_posts(records):
    for i in records["records"]:
       # print(i)
        if "DateTime2Send" in i["fields"]:
            date_time_str = i["fields"]["DateTime2Send"]
            date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            date_time_obj_utc = date_time_obj.replace(tzinfo=timezone('UTC'))      
            now = datetime.datetime.utcnow()
            now = now.replace(tzinfo=timezone('UTC'))
            if now > date_time_obj_utc:
                if not "POSTED" in i["fields"]:
                    bot_id = find_bot_id(i["fields"]["Bot"])
                    send_message(i["fields"]["Writeup"], bot_id)
                    update_airtable_posted(i["id"])

def update_airtable_posted(record_id):
    headers = {'Authorization': 'Bearer {}'.format(airtable_api_key), "Content-Type": "application/json"}
    data = {"records": [{"id": record_id, "fields": {"POSTED": True}}]}
    data = json.dumps(data)
    r = requests.patch('{}/Table%201'.format(airtable_api_url), headers=headers, data=data)

def send_message(msg, bot_id):
  url  = 'https://api.groupme.com/v3/bots/post'

  data = {
          'bot_id' : bot_id,
          'text'   : msg,
         }
  request = Request(url, urlencode(data).encode())
  json = urlopen(request).read().decode()


def lambda_handler(event, context):
    records = get_airtable_records(airtable_api_key)
    find_posts(records)
    return {
        'statusCode': 200,
        'body': json.dumps("Completed")
    }
lambda_handler("test", "test")