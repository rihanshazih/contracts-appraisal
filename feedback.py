import json
from requests_futures.sessions import FuturesSession
import boto3
import os
from decimal import Decimal
from uuid import uuid4

session = FuturesSession()

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
feedback_table = dynamodb.Table('contract-appraisal-feedback')


def handle(event, context):
    if os.environ.get('DEBUG') == 'true':
        print(event)

    feedback = json.loads(event['body'])

    if 'type_id' not in feedback or 'old_price_per_unit' not in feedback or 'old_price_type' not in feedback:
        return bad_request()

    feedback_table.put_item(Item={
        'id': str(uuid4()),
        'feedback': json.loads(json.dumps(feedback), parse_float=Decimal)
    })

    return build_response()


def bad_request():
    return {
        "statusCode": 400,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": "true"
        }
    }


def build_response():
    response = {
        "statusCode": 201,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": "true"
        }
    }
    return response
