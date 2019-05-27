from requests_futures.sessions import FuturesSession
import boto3
import time
import math
import datetime
from boto3.dynamodb.conditions import Key
from random import randint
from uuid import uuid4
from datetime import datetime

session = FuturesSession()

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
structures_table = dynamodb.Table('contract-appraisal-structures')
contracts_table = dynamodb.Table('contract-appraisal-contracts')
scheduling_table = dynamodb.Table('contract-appraisal-scheduling')


def handle(event, context):
    print('Received %d records' % len(event['Records']))

    contracts = []
    for record in event['Records']:

        # as we're using ttl to schedule executions, we do not care about inserts or updates,
        # only about removes which happen when the ttl is hit
        event_name = record['eventName']
        if event_name != 'REMOVE':
            print('Skipping %s' % event_name)
            continue

        print(event)

        # note that this image is in DynamoDB style, not a regular python object and needs to be converted accordingly
        item = record['dynamodb']
        old_image = item['OldImage']
        contract_id = int(old_image['contract_id']['N'])

        contract = get_contract(contract_id)
        if contract is None:
            # if we can't find the contract then we have to ignore it
            # i don't know how this could happen, but i don't want the script to fail
            continue
        contracts.append(contract)

    # if we are running into downtime, then reschedule the contracts
    now = datetime.now()
    if now.hour == 11 and now.minute < 10:
        for c in contracts:
            reschedule(c, within_hour=True)

    if len(contracts) > 100:
        print('rescheduling %d contracts' % (len(contracts) - 100))
        reschedule_contracts = contracts[100:]
        for r in reschedule_contracts:
            reschedule(r, within_hour=True)
        contracts = contracts[:100]
    process_contracts(contracts)


def process_contracts(contracts):
    futures = []
    for contract in contracts:
        headers = {}
        if 'Etag' in contract:
            headers['If-None-Match'] = contract['ETag']
        futures.append({
            'future': session.get(url='https://esi.evetech.net/v1/contracts/public/items/%d' % contract['contract_id'],
                                  headers=headers),
            'contract': contract
        })

    for f in futures:
        response = f['future'].result()
        contract = f['contract']

        score = 0
        if response.status_code == 200:
            etag = response.headers['ETag']
            # print('Updating etag for %d' % contract['contract_id'])
            contracts_table.update_item(
                Key={
                    'contract_id': contract['contract_id']
                },
                UpdateExpression="SET ETag = :s",
                ExpressionAttributeValues={
                    ':s': etag,
                },
                ReturnValues="NONE"
            )
            reschedule(contract)
            continue
        elif response.status_code == 304 or response.status_code == 204:
            reschedule(contract)
            continue
        elif response.status_code == 403:
            error = response.json()
            if 'error' in error and 'accepted by player' in error['error']:
                date = contract['date_issued']
                days_since_issued = (datetime.datetime.now() - datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ')).days
                if days_since_issued == 0:
                    score = 5
                else:
                    score = 1.0 / math.sqrt(days_since_issued)
        elif response.status_code == 404:
            # score should remain 0, as this contract probably expired or was deleted
            pass
        else:
            if response.status_code >= 420:
                print('%d' % response.status_code)
                reschedule(contract)
                continue
            else:
                print('unknown error code %d, %s' % (response.status_code, response.content))

        score = round(score * 100)
        # print('score %d for contract %d' % (score, contract['contract_id']))

        date_scored = datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%SZ')
        contracts_table.update_item(
            Key={
                'contract_id': contract['contract_id']
            },
            UpdateExpression="SET score = :s, date_scored = :d",
            ExpressionAttributeValues={
                ':s': score,
                ':d': date_scored
            },
            ReturnValues="NONE"
        )


def reschedule(contract, within_hour=False):
    if not within_hour and 'date_issued' in contract and get_age(contract['date_issue']).days > 1:
        # if more than a day has passed, then wait 10 to 20 hours (to stay within a day)
        minimum_wait = 10 * 60 * 60
        maximum_wait = 20 * 60 * 60
        delay = randint(minimum_wait, maximum_wait)
    else:
        # while we're on the first day check the contract within 10 to 60 minutes
        delay = randint(10 * 60, 60 * 60)

    print('scheduling %d with a delay of %d seconds' % (contract['contract_id'], delay))

    scheduling_table.put_item(
        Item={
            'id': str(uuid4()),
            'contract_id': contract['contract_id'],
            'ttl': int(time.time()) + delay
        }
    )


def get_contract(contract_id):
    response = contracts_table.query(
        KeyConditionExpression=Key('contract_id').eq(contract_id)
    )
    if response['Count'] == 0:
        return None
    return response['Items'][0]


def get_age(date_string):
    return datetime.now() - datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%SZ')
