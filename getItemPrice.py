import json
from requests_futures.sessions import FuturesSession
import boto3
from statistics import median
from decimal import Decimal
import time
from wsgiref.handlers import format_date_time
import os
import urllib.parse

session = FuturesSession()

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
price_cache_table = dynamodb.Table('contract-appraisal-price-cache')
prices_v2 = dynamodb.Table('contract-appraisal-prices-v2')
refined_table = dynamodb.Table('contract-appraisal-refined')

cache_time = (10 * 24 * 60 * 60)  # first number is days


def average(lst):
    if len(lst) == 0:
        return 0
    return sum(lst) / len(lst)


def handle(event, context):
    if os.environ.get('DEBUG') == 'true':
        print(event)

    is_aws_load_balancer = False

    if is_aws_load_balancer:
        # event['path'] # todo: extract type_id from alb path
        type_id = 1
    else:
        type_id = int(event['pathParameters']['type_id'])

    print("type_id: %d" % type_id)

    material_efficiency = None
    time_efficiency = None
    include_private = False
    security = "highsec,lowsec,nullsec,wormhole"
    bpc = False
    if 'queryStringParameters' in event and event['queryStringParameters'] is not None:
        print(event['queryStringParameters'])
        query_parameters = event['queryStringParameters']
        if 'security' in query_parameters:
            # decoding is required as ALB does not decode url parameters
            security = urllib.parse.unquote(query_parameters['security'])
        if 'material_efficiency' in query_parameters:
            material_efficiency = int(query_parameters['material_efficiency'])
        if 'time_efficiency' in query_parameters:
            time_efficiency = int(query_parameters['time_efficiency'])
        if 'include_private' in query_parameters:
            include_private = query_parameters['include_private'].lower() == "true"
            if include_private and security != "highsec,lowsec,nullsec,wormhole":
                return {
                    "statusCode": 400,
                    "body": json.dumps({ "error": "Combining the parameter include_private=true with non-default "
                                                  "securities does not return any data and is therefore prevented." }),
                    "headers": {
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Credentials": "true"
                    }
                }
        if 'bpc' in query_parameters:
            bpc = query_parameters['bpc'].lower() == "true"

    cache_id = "%d-%s-%s-%s-%s-%s" % (type_id, material_efficiency, time_efficiency, include_private, security, bpc)
    print("cache_id:%s" % cache_id)
    if os.environ.get('PRICES_V2') == 'true':
        response = prices_v2.query(
            KeyConditionExpression="price_key = :cache_id",
            ExpressionAttributeValues={":cache_id": cache_id}
        )
        if response['Count'] > 0:
            print("Cache V2 hit for %s" % cache_id)
            item = response['Items'][0]
            item['average'] = float(item['average'])
            item['five_percent'] = float(item['five_percent'])
            item['maximum'] = float(item['maximum'])
            item['median'] = float(item['median'])
            item['minimum'] = float(item['minimum'])
            item['type_id'] = int(item['type_id'])
            if 'contracts' in item:
                item['contracts'] = int(item['contracts'])
            del item['price_key']
            return build_response(item, time.time() + cache_time, is_aws_load_balancer)

    #  THE FOLLOWING CODE IS DEPRECATED AND WILL BE FADED OUT
    response = price_cache_table.query(
        KeyConditionExpression="cache_id = :cache_id",
        ExpressionAttributeValues={":cache_id": cache_id}
    )
    if response['Count'] > 0:
        print("Cache V1 hit for %s" % cache_id)
        item = response['Items'][0]
        item['average'] = float(item['average'])
        item['contracts'] = int(item['contracts'])
        item['five_percent'] = float(item['five_percent'])
        item['maximum'] = float(item['maximum'])
        item['median'] = float(item['median'])
        item['minimum'] = float(item['minimum'])
        item['type_id'] = int(item['type_id'])
        expires = item['ttl_date']
        del item['cache_id']
        del item['ttl_date']
        return build_response(item, expires, is_aws_load_balancer)
    print("Cache miss for %s" % cache_id)

    securities = security.split(",")

    print('Loading Price Data ...')
    price_data = get_price_data(type_id, bpc)

    if price_data is None:
        print('Found no price data')
        return empty_response(is_aws_load_balancer)

    unit_prices = []
    contract_count = 0
    for p in price_data.prices:
        # include_private must not be used with non-default securities (which are highsec,lowsec,nullsec,wormhole)
        if (include_private or p.security in securities) \
                and (material_efficiency is None or p.material_efficiency == material_efficiency) \
                and (time_efficiency is None or p.time_efficiency == time_efficiency):
            # todo: consider the amount of items, don't do that by putting the price into they array times the amount as that will cause memory issues
            unit_prices.append(p.price_per_unit)
            if p.contract_id != 1:
                contract_count += 1

    print('Found %d unit prices' % len(unit_prices))

    if len(unit_prices) == 0:
        return empty_response()

    five = min(unit_prices)
    if (len(unit_prices)) >= 20:
        five = average(unit_prices[0:int(len(unit_prices)*0.05)])

    result = {
        'type_id': price_data.type_id,
        'type_name': price_data.type_name,
        'median': float(median(unit_prices)),
        'average': float(average(unit_prices)),
        'minimum': float(min(unit_prices)),
        'maximum': float(max(unit_prices)),
        'five_percent': float(five),
        'contracts': contract_count
    }

    result['ttl_date'] = int(time.time()) + cache_time # now + 5 days
    result['cache_id'] = cache_id
    price_cache_table.put_item(
        Item=json.loads(json.dumps(result), parse_float=Decimal)
    )
    expires = result['ttl_date']
    del result['ttl_date']
    del result['cache_id']

    return build_response(result, expires, is_aws_load_balancer)


def empty_response(is_aws_load_balancer=False):
    response = {
        "statusCode": 204,
        "headers": {
            "Expires": format_date_time(time.time() + cache_time),  # now + 5 days
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": "true"
        }
    }
    if is_aws_load_balancer:
        response['statusDescription'] = '204 No Content'
        response['isBase64Encoded'] = False
    return response


def map_price_data_from_dynamo(entry):
    prices = []
    for i in entry['prices']:
        prices.append(PriceElement(float(i['price_per_unit']), int(i['amount']), int(i['timestamp']),
                                   i['security'], i['public_structure'], int(i['contract_id']),
                                   int(i['time_efficiency']), int(i['material_efficiency'])))
    result = TypeData(int(entry['type_id']), entry['type_name'], prices, entry['is_bpc'])
    return result


def build_response(result, expires, is_aws_load_balancer=False):
    response = {
        "statusCode": 200,
        "body": json.dumps(result),
        "headers": {
            "Expires": format_date_time(expires),
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": "true"
        }
    }
    if is_aws_load_balancer:
        response['statusDescription'] = '200 OK'
        response['isBase64Encoded'] = False
    return response


def get_price_data(type_id, bpc):
    kce = "type_id = :type_id AND is_bpc = :bpc"
    eav = {":type_id": type_id, ":bpc": str(bpc)}
    response = refined_table.query(
        KeyConditionExpression=kce,
        ExpressionAttributeValues=eav
    )

    if response['Count'] == 0:
        return None
    else:
        return map_price_data_from_dynamo(response['Items'][0])


class PriceElement:
    def __init__(self, price_per_unit, amount, timestamp, security, public_structure, contract_id, time_efficiency=0,
                 material_efficiency=0):
        self.contract_id = contract_id
        self.amount = amount
        self.security = security
        self.material_efficiency = material_efficiency
        self.time_efficiency = time_efficiency
        self.public_structure = public_structure
        self.timestamp = timestamp
        self.price_per_unit = price_per_unit


class TypeData:
    def __init__(self, type_id, type_name, prices, is_bpc):
        self.prices = prices
        self.type_name = type_name
        self.is_bpc = is_bpc
        self.type_id = type_id
