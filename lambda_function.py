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


lambda_name = "groupme_bot_1"

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("iwc_lambda_params")
response = table.get_item(Key={"lambda_name": lambda_name})
groupme_token = response["Item"]["groupme_token"]
airtable_api_key = response["Item"]["airtable_api_key"]
airtable_api_url = response["Item"]["airtable_api_url"]
bot_ids = response["Item"]["bot_ids"]

def find_bot_id(bot_name):
    for k,v in bot_ids.items():
        if k == bot_name:
            return(v)
    print("no bot ID found")
    quit()

def get_airtable_records(airtable_api_key):
    headers = {'Authorization': 'Bearer {}'.format(airtable_api_key)}
    parameters = {}
    r = requests.get('{}/Table%201'.format(airtable_api_url), headers=headers)

    response = r.json()
    print(response)
    return(response)

def find_posts(records):
    for i in records["records"]:
        print(i)
        if "DateTime2Send" in i["fields"]:
            date_time_str = i["fields"]["DateTime2Send"]
            date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            date_time_obj_utc = date_time_obj.replace(tzinfo=timezone('UTC'))      
            now = datetime.datetime.utcnow()
            now = now.replace(tzinfo=timezone('UTC'))
            if now > date_time_obj_utc:
                pprint.pprint(i["fields"])
                if not "POSTED" in i["fields"]:
                    bot_id = find_bot_id(i["fields"]["Bot"])
                    image_link = ""
                    if "Image" in i["fields"]:
                        image_link = process_image(i["fields"]['Image'][0]["url"])
                        print(image_link)
                    send_message(i["fields"]["Writeup"], bot_id, image_link)
                    update_airtable_posted(i["id"])

def update_airtable_posted(record_id):
    headers = {'Authorization': 'Bearer {}'.format(airtable_api_key), "Content-Type": "application/json"}
    data = {"records": [{"id": record_id, "fields": {"POSTED": True}}]}
    data = json.dumps(data)
    r = requests.patch('{}/Table%201'.format(airtable_api_url), headers=headers, data=data)

def send_message(msg, bot_id, image_link):
  data = {}

  url  = 'https://api.groupme.com/v3/bots/post'
  if image_link:
    data["picture_url"] = image_link

  data["bot_id"] = bot_id
  data["text"] = msg
  print(data)
  request = Request(url, urlencode(data).encode())
  json = urlopen(request).read().decode()
  print(json)

def process_image(image_link):
    params = {"stream": True}
    response = requests.get(image_link, params=params)
    local_filename = "/tmp/file.gif"
    totalbits = 0
    if response.status_code == 200:
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    totalbits += 1024
                    print("Downloaded",totalbits*1025,"KB...")
                    f.write(chunk)
    headers = {
    'X-Access-Token': groupme_token,
    'Content-Type': 'image/gif',
    }
    data = open('/tmp/file.gif', 'rb').read()
    response = requests.post('https://image.groupme.com/pictures', headers=headers, data=data)
    response = response.json()
    return(response["payload"]["picture_url"])

def lambda_handler(event, context):
   
    records = get_airtable_records(airtable_api_key)
    find_posts(records)
    return {
        'statusCode': 200,
        'body': json.dumps("Completed")
    }
lambda_handler("test", "test")