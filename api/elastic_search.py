import requests
import json
from decimal import Decimal
import datetime
from botocore.exceptions import ClientError

REGION = os.getenv("region")
AWS_ACCESS_KEYID = os.getenv("access_key")
SECRET_KEY = os.getenv("secret_key")

session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEYID,aws_secret_access_key=SECRET_KEY,region_name=REGION)

client = session.resource('dynamodb')
table = client.Table('yelp_restaurants')

def putRequests():
    resp = table.scan()    
    url = 'https://search-restaurant-jnfbzcafyoerpjh3ehb4qyzadi.us-east-1.es.amazonaws.com/restaurant/_doc/'
    headers = {"Content-Type": "application/json"}
    i=1
    while True:
        #print(len(resp['Items']))
        for item in resp['Items']:
            url = 'https://search-restaurant-jnfbzcafyoerpjh3ehb4qyzadi.us-east-1.es.amazonaws.com/restaurant/_doc/'
            
            body = {"RestaurantID": item['business_id'], "category": item['category']}
            url +=str(i)
            r = requests.post(url,auth=awsauth, data=json.dumps(body).encode("utf-8"), headers=headers)
            print(r.text)
            i += 1
            print(url)
            #break;
        if 'LastEvaluatedKey' in resp:
            resp = table.scan(
                ExclusiveStartKey=resp['LastEvaluatedKey']
            )
            #break;
        else:
            break
putRequests()