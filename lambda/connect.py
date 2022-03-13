import boto3
import json
import time

client = boto3.client('lex-runtime')


def lambda_handler(event, context):
    text = None
    user_id = None
    if "messages" in event:
        messages = event["messages"]
        message = messages[0]
        
        
        if("unstructured" in message):
            if ("text" in message["unstructured"]):
                text = message["unstructured"]["text"]
            if ("user_id" in message["unstructured"]):
                user_id = message["unstructured"]["user_id"]
        
    last_user_message = text
    user_id = 'user'
 
    if last_user_message is None or len(last_user_message) < 1:
        res = {
        "status code": 200,
        "body": {},
        "messages":[
            {
                "type":"unstructured",
                "unstructured": {
                    "user_id": None,
                    "text": "Cannot connect with Lex.",
                    "time": time.time()
                }
            }]
        }
        return res

    response = client.post_text(botName='DiningConcierge',
                                botAlias='botalias',
                                userId=user_id,
                                inputText=last_user_message)

    if response['message'] is not None or len(response['message']) > 0:
        last_user_message = response['message']

    res = {
        "status code": 200,
        "body": {},
        "messages":[
            {
                "type":"unstructured",
                "unstructured": {
                    "user_id": user_id,
                    "text": last_user_message,
                    "time": time.time()
                }
            }]
    }
    return res