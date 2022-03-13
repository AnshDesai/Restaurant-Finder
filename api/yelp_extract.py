import requests
import json
from decimal import Decimal
import datetime
from botocore.exceptions import ClientError

AWS_ACCESS_KEYID = os.getenv("access_key")
SECRET_KEY = os.getenv("secret_key")
REGION = os.getenv("region")
session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEYID,aws_secret_access_key=SECRET_KEY,region_name=REGION)

cuisine_types = ['chinese','mexican','italian','thai','japanese','french']

client = session.resource('dynamodb')
table = client.Table('yelp_restaurants')

for cuisine_type in cuisine_types:
    offset = 0
    for i in range(20):
        offset += 50
        PARAMETERS = {
            'term': 'restaurant',
            'location': 'Manhattan',
            'categories': cuisine_type,
            'limit': 50,
            'offset': offset
        }
        response = requests.get(url=ENDPOINT, params=PARAMETERS, headers=HEADERS)
        business_data = response.json()
        try:
            for biz in business_data['businesses']:

                    table.put_item(
                        Item={
                            'business_id': biz['id'],
                            'name': biz['name'],
                            'category': biz['categories'][0]['alias'],
                            'address': biz['location']['address1'],
                            'city': biz['location']['city'],
                            'zipcode': biz['location']['zip_code'],
                            'reviewCount': biz['review_count'],
                            'rating': Decimal(str(biz['rating'])),
                            'insertedAtTimestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        },
                        ConditionExpression='attribute_not_exists(businessId) AND attribute_not_exists(insertedAtTimestamp)'
                    )
        except Exception as e:
            print(e)
            continue
    # print(offset)