from __future__ import annotations
import random
import csv
import aiohttp
import asyncio
from typing import Any
import traceback
import time
from urllib.parse import urlencode

COLUMNS = ['vin', 'open_recalls', 'name_of_recall', 'campaign', 'date_of_recall_announcement',
           'brief_description_of_recall', 'safety_risk', 'remedy', 'customer_satisfaction_programs', 'raw_json']


def chunks(xs, n):
    n = max(1, n)
    return (xs[i:i + n] for i in range(0, len(xs), n))


def get_exception_traceback(exception):
    tb = ''.join(traceback.format_exception(exception))
    return tb


def write_to_csv(file_path: str, data: list[Any]):
    with open(file_path, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(data)


async def request_page(vin_no: str, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
    retries = 3
    is_success = False
    response = None
    error = None
    url = "https://www.digitalservices.ford.com/owner/api/v2/recalls?"
    payload = {
        "vin": vin_no,
        "country": "usa",
        "langscript": "LATN",
        "language": "en",
        "region": "en-us"
    }

    api = url + urlencode(payload)

    headers = {
        'Origin': 'https://www.ford.com',
        'Referer': 'https://www.ford.com/',
    }

    for _ in range(retries):
        try:
            response = await session.get(url=api, headers=headers, ssl=True)
            if response.status in [200, 404]:
                is_success = True
                break
        except asyncio.exceptions.TimeoutError as e:
            error = e

    if not is_success:
        if error:
            raise error
        else:
            raise Exception(f"Request failed with status code: {response.status}, text: {await response.text()}")

    return response


details_list = []


async def parse_vehical_recalls(vin_no: str, session: aiohttp.ClientSession):
    page = await request_page(vin_no, session)
    vehicle_recall_json = await page.json()
    details_dict = {}
    vin = vin_no
    vehicle_details = vehicle_recall_json['vehicleDetails']  # if required

    customer_satisfaction_programs = vehicle_recall_json['fsa']
    for item in customer_satisfaction_programs:
        try:
            del item['descriptionLang']
        except Exception as e:
            pass
        item['campaign'] = item.pop('fsaNumber')
        item['date'] = item.pop('launchDate')

    nhtsa = vehicle_recall_json['nhtsa']
    name_of_recall = ''
    campaign = ''
    date_of_recall_announcement = ''
    brief_description_of_recall = ''
    safety_risk = ''
    remedy = ''
    if len(nhtsa) > 0:
        open_recalls = "Yes"
        for x in nhtsa:
            name_of_recall = x['description']
            campaign = x['manufacturerRecallNumber'] + '/' + x['nhtsaRecallNumber']
            date_of_recall_announcement = x['recallDate']
            brief_description_of_recall = x['recallDescription']
            safety_risk = x['safetyRiskDescription']
            remedy = x['remedyDescription']
            details_dict['vin'] = vin
            details_dict['open_recalls'] = open_recalls
            details_dict['name_of_recall'] = name_of_recall.lower().title()
            details_dict['campaign'] = campaign
            details_dict['date_of_recall_announcement'] = date_of_recall_announcement
            details_dict['brief_description_of_recall'] = brief_description_of_recall.lower().title()
            details_dict['safety_risk'] = safety_risk.lower().title()
            details_dict['remedy'] = remedy.lower().title()
            details_dict['customer_satisfaction_programs'] = customer_satisfaction_programs
            details_dict['raw_json'] = vehicle_recall_json
            details_list.append(details_dict.copy())

    else:
        open_recalls = "No"
        details_dict['vin'] = vin
        details_dict['open_recalls'] = open_recalls
        details_dict['name_of_recall'] = name_of_recall
        details_dict['campaign'] = campaign
        details_dict['date_of_recall_announcement'] = date_of_recall_announcement
        details_dict['brief_description_of_recall'] = brief_description_of_recall
        details_dict['safety_risk'] = safety_risk
        details_dict['remedy'] = remedy
        details_dict['customer_satisfaction_programs'] = customer_satisfaction_programs
        details_dict['raw_json'] = vehicle_recall_json
        details_list.append(details_dict.copy())

    return details_list


async def runner(all_vins_file_path: str, output_data_file_path: str, concurrency: int, batch_size: int):
    # get all vin nos
    with open(all_vins_file_path) as infile:
        all_vin_nos = [vin.strip() for vin in infile]
        print(f"Total vins: {len(all_vin_nos)}")

    vins_to_scrape = all_vin_nos
    # random.shuffle(vins_to_scrape)
    print(f"Vins to scrape: {len(vins_to_scrape)}")
    print()

    # Start scraping in batches
    async def _wrapper(func, id, *args, **kwargs) -> (bool, Any | Exception):
        try:
            result = await func(*args, **kwargs)
            return True, id, result
        except Exception as e:
            return False, id, e

    batches = chunks(vins_to_scrape, batch_size)
    start_time = time.time()
    passed = []
    failed = []

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=concurrency)) as session:
        for batch_index, batch in enumerate(batches):
            batch_start_time = time.time()
            batch_data = []
            print("------------------------------------")
            print(f"Processing batch {batch_index + 1}, {len(batch)} vins")
            batch_result = await asyncio.gather(*[_wrapper(parse_vehical_recalls, vin, vin, session) for vin in batch])
            batch_end_time = time.time()
            print(
                f'Took {batch_end_time - batch_start_time} seconds to process batch {batch_index + 1} of size {len(batch)}')
            for result in batch_result:
                if result[0]:
                    batch_data.append(result[2])
                    passed.append(result[1])
                else:
                    print(f"Failed for vin: {result[1]}")
                    print(f'\033[2;31;40m {get_exception_traceback(result[2])}')
                    failed.append(result[1])

            # Write to file
            if len(batch_data) > 0:
                # pprint(batch_data)
                write_to_csv(output_data_file_path, batch_data[0])

    end_time = time.time()
    print("-------------------")
    print()
    print(f"Took {end_time - start_time} seconds")
    print(f"Tried to scrape: {len(vins_to_scrape)}")
    print(f"Success: {len(passed)}")
    print(f"Failed: {len(failed)}")
    for vin in failed:
        print(vin)
    print()


if __name__ == '__main__':
    ALL_VINS_FILE_PATH = "vin_nos_ford"
    OUTPUT_DATA_FILE_PATH = "vehicle_record_data_ford.csv"
    CONCURRENCY = 45
    BATCH_SIZE = 200

    asyncio.run(runner(all_vins_file_path=ALL_VINS_FILE_PATH, output_data_file_path=OUTPUT_DATA_FILE_PATH,
                       concurrency=CONCURRENCY, batch_size=BATCH_SIZE))
