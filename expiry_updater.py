from requests_futures.sessions import FuturesSession
import boto3
import time
import math
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
    contracts = []
    for record in event['Records']:

        # as we're using ttl to schedule executions, we do not care about inserts or updates,
        # only about removes which happen when the ttl is hit
        event_name = record['eventName']
        if event_name != 'REMOVE':
            continue

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
        return

    if len(contracts) > 100:
        print('rescheduling %d contracts' % (len(contracts) - 100))
        reschedule_contracts = contracts[100:]
        for r in reschedule_contracts:
            reschedule(r, within_hour=True)
        contracts = contracts[:100]

    if len(contracts) > 0:
        print('processing %d contracts' % len(contracts))
        process_contracts(contracts)


def process_contracts(contracts):
    futures = []
    for contract in contracts:
        headers = {}
        if 'ETag' in contract:
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
        print('received status code %d for contract %d' % (response.status_code, contract['contract_id']))
        if 'x-esi-error-limit-remain' in response.headers and response.status_code < 400:
            lowest_remain = int(response.headers['x-esi-error-limit-remain'])
            if lowest_remain < 100:
                print('remaining tries before error limiting: %d' % lowest_remain)
        if response.status_code == 200:
            etag = response.headers['ETag']
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
            print(error)
            if 'error' in error and 'accepted by player' in error['error']:
                date = contract['date_issued']
                days_since_issued = (datetime.now() - datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ')).days
                if days_since_issued == 0:
                    score = 5
                else:
                    score = 1.0 / math.sqrt(days_since_issued)
        elif response.status_code == 404:
            # score should remain 0, as this contract probably expired or was deleted
            pass
        else:
            if response.status_code >= 420:
                reschedule(contract)
                continue
            else:
                print('unknown error code %d, %s' % (response.status_code, response.content))

        score = round(score * 100)

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
        print('scored %d with %d' % (contract['contract_id'], score))


def reschedule(contract, within_hour=False):
    contract_age = get_age(contract['date_issued'])
    if not within_hour and 'date_issued' in contract and contract_age.days >= 1:

        print('debug: long delay for %d with days age %d' % (contract['contract_id'], contract_age.days))

        hours = contract_age.total_seconds() // 3600  # // makes an int division
        # scale
        # 1 day: 12h
        # 2 days: 17h
        # 9 days: 2d
        # 24 days: 3d
        maximum_wait = int(math.sqrt(25 * hours) * 60 * 60)

        minimum_wait = maximum_wait // 2

        delay = randint(minimum_wait, maximum_wait)
    else:
        # while we're on the first day check the contract within 10 to 60 minutes
        delay = randint(10 * 60, 60 * 60)

    print('rescheduling %d with a delay of %d seconds' % (contract['contract_id'], delay))

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
