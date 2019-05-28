import json
from requests_futures.sessions import FuturesSession
import boto3
from collections import Counter
from decimal import Decimal
from uuid import uuid4
import time
from random import randint
from datetime import datetime

session = FuturesSession()
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
structures_table = dynamodb.Table('contract-appraisal-structures')
contracts_table = dynamodb.Table('contract-appraisal-contracts')
latest_id_table = dynamodb.Table('contract-appraisal-latest-contract-id')
scheduling_table = dynamodb.Table('contract-appraisal-scheduling')


def get_public_structures():
    return session.get('https://esi.evetech.net/v1/universe/structures/').result().json()


def get_latest_id():
    return int(latest_id_table.scan()['Items'][0]['value'])


def set_latest_id(id):
    latest_id_table.put_item(Item={'id': 1, 'value': int(id)})


def handle(event, context):
    # if we are running into downtime, then skip this run
    now = datetime.now()
    if now.hour == 11 and now.minute < 10:
        return

    batch_size = 1000
    if 'batch_size' in event:
        batch_size = int(event['batch_size'])
    skip_pages = False
    if 'skip_pages' in event:
        skip_pages = event['skip_pages'].lower() == "true"

    print('Loading Contracts ...')
    contracts = parse_contracts(skip_pages)
    print('Loaded %d contracts from esi' % len(contracts))
    if len(contracts) == 0:
        return

    print('Loading latest contract id ...')

    latest_id = get_latest_id()

    print('Checking which contracts already exist for %d contracts ...' % len(contracts))

    new_latest_id = latest_id
    new_contracts = []
    for contract in contracts:
        contract_id = int(contract['contract_id'])
        if contract_id > latest_id:
            if contract_id > new_latest_id:
                new_latest_id = contract_id
            new_contracts.append(contract)

    set_latest_id(new_latest_id)

    if len(new_contracts) > batch_size:
        new_contracts = new_contracts[:batch_size]

    print('Enhancing %d contracts ...' % len(new_contracts))
    new_contracts = enhance_contracts(new_contracts)

    for contract in new_contracts:
        contracts_table.put_item(
            Item=json.loads(json.dumps(contract), parse_float=Decimal)
        )
        scheduling_table.put_item(
            Item={
                'id': str(uuid4()),
                'contract_id': contract['contract_id'],
                # schedule the first check for within 10 to 60 minutes into the future
                'ttl': int(time.time()) + randint(10 * 60, 60 * 60)
            }
        )

    print('Added %d new contracts' % len(new_contracts))

    return 'ok'


def get_etag_header(url, etags):
    for e in etags:
        if e['url'] == url:
            return {'If-None-Match': e['value']}
    return {'If-None-Match': ''}


def parse_contracts(skip_pages):

    etags_table = dynamodb.Table('contract-appraisal-etags')
    etags = etags_table.scan()['Items']

    contracts = []
    new_etags = []
    urls = []

    for region_id in range(10000001, 10000070):
        if region_id not in [10000024, 10000026]:  # skip non existent regions
            urls.append('https://esi.evetech.net/v1/contracts/public/%d/' % region_id)

    while len(urls) > 0:
        futures = {}
        for url in urls:
            futures[url] = session.get(url, headers = get_etag_header(url, etags))

        urls = []

        for url, future in futures.items():
            print('Loading contracts for %s' % url)
            response = future.result()
            print('Response is %d' % response.status_code)
            headers = response.headers
            if 'X-Pages' in headers:
                pages = int(headers['X-Pages'])
                if pages > 1 and not skip_pages and '?page=' not in url:
                    for i in range(2, pages + 1):
                        urls.append(url + ("?page=%d" % i))

            if response.status_code is 200:
                content = response.json()
                for contract in content:
                    if contract['type'] == 'item_exchange':
                        contract['start_location_id'] = int(contract['start_location_id'])
                        if contract['title'] == '':
                            contract['title'] = 'n/a'
                        contracts.append(contract)

                new_etags.append({
                    'url': url,
                    'value': headers['ETag']
                })

    for e in new_etags:
        etags_table.put_item(Item=e)

    return contracts


def most_common(lst):
    data = Counter(lst)
    return data.most_common(1)[0][0]


def enhance_contracts(contracts):

    targets = []

    for contract in contracts:
        contract['contract_items'] = []

        targets.append({
            'url': 'https://esi.evetech.net/v1/contracts/public/items/%s/' % contract['contract_id'],
            'contract_id': contract['contract_id']
        })

    while len(targets) > 0:

        requests = []
        for target in targets:
            requests.append({
                'url': target['url'],
                'future': session.get(target['url']),
                'contract_id': target['contract_id']
            })
        targets = []

        for request in requests:
            response = request['future'].result()

            if response.status_code == 200:
                content = response.json()
                print('Response for %s has %d items' % (request['url'], len(content)))
                for contract in contracts:
                    if contract['contract_id'] == request['contract_id']:
                        contract['contract_items'].extend(content)
                        type_ids = []
                        for i in contract['contract_items']:
                            type_ids.append(int(i['type_id']))
                        if len(type_ids) > 0:
                            contract['most_common_type_id'] = most_common(type_ids)

                headers = response.headers
                pages = int(headers['X-Pages'])
                if pages > 1 and '?page=' not in request['url']:
                    for i in range(2, pages + 1):
                        targets.append({
                            'url': request['url'] + ("?page=%d" % i),
                            'contract_id': request['contract_id']
                        })
            else:
                print('Received %d for %s' % (response.status_code, request['url']))

    return contracts


def prep_for_db(contract):
    contract['reward'] = float(contract['reward'])
    contract['volume'] = float(contract['volume'])
    contract['collateral'] = float(contract['collateral'])
    contract['price'] = float(contract['price'])
    contract['price'] = float(contract['price'])
    contract['contract_id'] = int(contract['contract_id'])
    contract['start_location_id'] = int(contract['start_location_id'])
    contract['days_to_complete'] = int(contract['days_to_complete'])
    contract['issuer_corporation_id'] = int(contract['issuer_corporation_id'])
    contract['end_location_id'] = int(contract['end_location_id'])
    if 'most_common_type_id' in contract:
        contract['most_common_type_id'] = int(contract['most_common_type_id'])
    contract['issuer_id'] = int(contract['issuer_id'])
    for i in contract['contract_items']:
        if 'record_id' in i:
            i['record_id'] = int(i['record_id'])
        if 'item_id' in i:
            i['item_id'] = int(i['item_id'])
        i['quantity'] = int(i['quantity'])
        i['type_id'] = int(i['type_id'])
        if 'time_efficiency' in i:
            i['time_efficiency'] = int(i['time_efficiency'])
            i['material_efficiency'] = int(i['material_efficiency'])
        if 'runs' in i:
            i['runs'] = int(i['runs'])
    return contract
