
import boto3
import json
import logging
from boto3.dynamodb.conditions import Key, Attr
from botocore.vendored import requests
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def getSQSMsg():
    SQS = boto3.client("sqs")
    url = 'https://sqs.us-east-1.amazonaws.com/840839741907/queue'
    response = SQS.receive_message(
        QueueUrl=url, 
        AttributeNames=['All'],
        MessageAttributeNames=['All'],
        VisibilityTimeout=60,
        WaitTimeSeconds=0
    )
    try:
        print("Response:::",response)
        message = response['Messages'][0]
        if message is None:
            logger.debug("Empty message")
            return None
    except KeyError:
        logger.debug("No message in the queue")
        return None
    message = response['Messages'][0]
    SQS.delete_message(
            QueueUrl=url,
            ReceiptHandle=message['ReceiptHandle']
        )
    logger.debug('Received and deleted message: %s' % response)
    return message

def lambda_handler(event, context):
    email_client = boto3.client('ses')
    message = getSQSMsg() #data will be a json object
    if message is None:
        logger.debug("No Cuisine found in message")
        return
    cuisine = message["MessageAttributes"]["Cuisine"]["StringValue"]
    email = message["MessageAttributes"]["email"]["StringValue"]
    location = message["MessageAttributes"]["Location"]["StringValue"]
    date = message["MessageAttributes"]["Date"]["StringValue"]
    time = message["MessageAttributes"]["Time"]["StringValue"]
    numOfPeople = message["MessageAttributes"]["NumPeople"]["StringValue"]
    if not cuisine or not email:
        logger.debug("No Cuisine or email found in message")
        return
    print(cuisine)
    es_query = "https://search-restaurant-jnfbzcafyoerpjh3ehb4qyzadi.us-east-1.es.amazonaws.com/restaurant/_search?q={category}".format(
        category=cuisine)
    esResponse = requests.get(es_query, auth=('demo','Alp@bans1'),headers={"Content-Type": "application/json"}).json()
    print("Elastic Search response:",esResponse)
    try:
        esData = esResponse["hits"]["hits"]
    except KeyError:
        logger.debug("Error extracting hits from ES response")
    # extract bID from AWS ES
    ids = []
    for restaurant in esData:
        ids.append(restaurant["_source"]["RestaurantID"])
    messageToSend = 'Hey! You can find {cuisine} restaurants in {location}.\n\nThese are available for {numPeople} people on {diningDate} at {diningTime}'.format(
            cuisine=cuisine,
            location=location,
            numPeople=numOfPeople,
            diningDate=date,
            diningTime=time,
        )
    print("Ids:::",ids)
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp_restaurants')
    itr = 1
    for id in ids:
        if itr == 3:
            break
        response = table.scan(FilterExpression=Attr('business_id').eq(id))
        item = response['Items'][0]
        if response is None:
            continue
        restaurantMsg = '\n\n' + str(itr) + '. '
        name = item["name"]
        address = item["address"]
        rating = item["rating"]
        reviewCount = item["reviewCount"]
        zipcode = item["zipcode"]
 
        if zipcode != None and address!=None:
            restaurantMsg += name +'\nAddress:' + address +" "+ zipcode
        elif zipcode!=None and address==None:
            restaurantMsg += name +'\nAddress:' + zipcode
        elif address!=None and zipcode==None:
            restaurantMsg += name +'\nAddress:' + address
        else:
            restaurantMsg += name 
        if rating!= None and reviewCount!=None:
            review = "\nReviews: "+str(reviewCount) + "\nRatings: "+ str(rating)
            messageToSend+= review
        messageToSend += restaurantMsg
        
        itr += 1
    
    messageToSend += "\n\nBon appétit!"
    print(messageToSend)
    CHARSET = "UTF-8"
    try:
        response = email_client.send_email(
        Source="anshdesai20@gmail.com",
        Destination={
            "ToAddresses": [
                email
            ]
        },
        Message= {
            "Subject": {
                "Charset": CHARSET,
                "Data": "Here are your recommendations",
            },
            'Body':{
                'Text':{
                    'Charset':CHARSET,
                    'Data':messageToSend
                }
            }
        
        }
    
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent")
        print(response["MessageId"])
    
    return {
        'statusCode': 200,
        'body': json.dumps("LF2 running succesfully")
    }