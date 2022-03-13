import boto3
import datetime
import json
import logging
import math
import os
import time
from botocore.vendored import requests

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
sqs = boto3.client("sqs")


def greeting_intent(intent_request):
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Hey! How can I help ?'})

def thank_you_intent(intent_request):
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'It was pleasure serving you our top recommendations!'})

def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }

def isvalid_date(date):
    print(date)
    try:
        datetime.datetime.strptime(date,'%Y-%m-%d')
        print("True")
        return True
    except Exception as e:
        print(e)
        return False


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

def dispatch(intent_request,context):
    intent_name = intent_request['currentIntent']['name']
    if intent_name == 'GreetingIntent':
        return greeting_intent(intent_request)
    elif intent_name == 'DiningSuggestionsIntent':
        return dining_suggestion_intent(intent_request,context)
    elif intent_name == 'ThankYouIntent':
        return thank_you_intent(intent_request)

    raise Exception('Intent: ' + intent_name + 'is not valid')


def validate_dining_suggestion(location, cuisine, num_people, date, time, email):
    cuisines = ['italian', 'chinese', 'mexican', 'thai', 'japanese', 'french']
    locations = ['new york','manhattan']
    emails = ['anshdesai255@gmail.com','bansishah2701@gmail.com']
    if location is not None and location.lower() not in locations:
        return build_validation_result(False,
                                       'location',
                                       'Unfortuantely we dont provide suggestions for {}, Please select location from \n1. Manhattan \n2. New York'.format(location))

    if cuisine is not None and cuisine.lower() not in cuisines:
        return build_validation_result(False,
                                       'cuisine',
                                       'Please select from one of the cuisines: \n1.italian \n2.chinese \n3.mexican \n4.thai \n5.japanese \n6.french')

    if num_people is not None:
        num_people = int(num_people)
        if num_people > 15 or num_people <= 0:
            return build_validation_result(False,
                                           'numofpeople',
                                           'Please try again! Max number of people allowed are 15.')

    if date is not None:
        
        if len(date)!=10:
            return build_validation_result(False,'date','Enter a valid date format: yyyy-mm-dd')
        if not isvalid_date(date):
            return build_validation_result(False, 'date',
                                           'Please enter a valid date!')
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'date','Enter a valid date!')

    if time is not None:
        if len(time) != 5:
            return build_validation_result(False, 'time', 'Enter a valid time format! hh:mm')

        hour, minute = time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)

        if math.isnan(hour) or math.isnan(minute):
            return build_validation_result(False, 'time', 'Please enter valid time format!(hh:mm)')

        if hour < 11 or hour > 19:
            return build_validation_result(False, 'time',
                                          'We only work between hours 11:00 to 19:00. Please specify time in between this.')

        if datetime.datetime.strptime(date, '%Y-%m-%d').date() == datetime.date.today():
            if datetime.datetime.strptime(time, '%H:%M').time()<=datetime.datetime.today().time():
                return build_validation_result(False, 'time',
                                          'Please specify a later time than current time.')
            
    if email is not None and email.lower() not in emails:
        return build_validation_result(False,
                                       'email',
                                       'Please enter a verified email address!')

    return build_validation_result(True, None, None)

def dining_suggestion_intent(intent_request,context):
    
    location = get_slots(intent_request)["location"]
    cuisine = get_slots(intent_request)["cuisine"]
    num_people = get_slots(intent_request)["numofpeople"]
    date = get_slots(intent_request)["date"]
    time = get_slots(intent_request)["time"]
    source = intent_request['invocationSource']
    email = get_slots(intent_request)["email"]

    if source == 'DialogCodeHook':
        slots = get_slots(intent_request)
        print("Validating")
        validation_result = validate_dining_suggestion(location, cuisine, num_people, date, time, email)

        if not validation_result['isValid']:
            
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        if intent_request['sessionAttributes'] is not None:
            output_session_attributes = intent_request['sessionAttributes']
        else:
            output_session_attributes = {}

        return delegate(output_session_attributes, get_slots(intent_request))

    sqs = boto3.client("sqs")
    queue = sqs.get_queue_url(QueueName='queue').get('QueueUrl')
    response = sqs.send_message(
            QueueUrl=queue, 
            MessageBody="User input from lex to LF1",
            MessageAttributes={
                "Location": {
                    "StringValue": location,
                    "DataType": "String"
                },
                "Cuisine": {
                    "StringValue": cuisine,
                    "DataType": "String"
                },
                "Date" : {
                    "StringValue": date,
                    "DataType": "String"
                },
                "Time" : {
                    "StringValue": time,
                    "DataType": "String"
                },
                "NumPeople" : {
                    "StringValue": num_people,
                    "DataType": "String"
                },
                "email" : {
                    "StringValue": email,
                    "DataType": "String"
                }
            }
        )
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Thank you! You will receive email with recommendations soon!'})
         
def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    return dispatch(event,context)





