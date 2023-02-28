from __future__ import annotations
import csv
import aiohttp
import asyncio
from typing import Any
import traceback
import time

COLUMNS = ['vin', 'open_recalls', 'recall_status', 'name_of_recall', 'campaign', 'date_of_recall_announcement',
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


def dictionary_helper(details_dict: dict, vin_no: str, open_recalls):
    details_dict['vin'] = vin_no
    details_dict['open_recalls'] = open_recalls
    details_dict['recall_status'] = ''
    details_dict['name_of_recall'] = ''
    details_dict['campaign'] = ''
    details_dict['date_of_recall_announcement'] = ''
    details_dict['brief_description_of_recall'] = ''
    details_dict['safety_risk'] = ''
    details_dict['remedy'] = ''

    return details_dict


async def request_page(vin_no: str, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
    retries = 3
    is_success = False
    response = None
    error = None
    api = f"https://www.chevrolet.com/ownercenter/api/{vin_no}/gfas?cb=16775740181110.5469480851262047"

    for _ in range(retries):
        try:
            response = await session.get(url=api, ssl=True)
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
    vehicle_json = await page.json()
    details_dict = {}

    if 'VEHICLE_INVALID_VIN' in vehicle_json['messages']:
        dictionary_helper(details_dict, vin_no, open_recalls="Invalid Vin")
        details_list.append(details_dict.copy())
    else:
        vehicle_recall_json = vehicle_json['data']['gfas']

        if len(vehicle_recall_json) > 0:
            open_recalls = "Yes"
            for x in vehicle_recall_json:
                try:
                    name_of_recall = x['gfaTexts'][0]['subject']
                except IndexError:
                    name_of_recall = 'unknown status'
                recall_status = x['vinStatusInfo']['vinStatus']

                try:
                    campaign = x['governmentAgencies'][0]['govtAgencyNum']
                except IndexError:
                    campaign = ''

                try:
                    date_of_recall_announcement = x['governmentAgencies'][0]['notificationDate']
                except IndexError:
                    date_of_recall_announcement = ''

                try:
                    brief_description_of_recall = x['gfaTexts'][0]['description']
                except Exception as e:
                    brief_description_of_recall = ''

                try:
                    safety_risk = x['gfaTexts'][0]['safetyRisk']
                except Exception as e:
                    safety_risk = ''

                try:
                    remedy = x['gfaTexts'][0]['remedy']
                except Exception as e:
                    remedy = ''

                details_dict['vin'] = vin_no
                details_dict['open_recalls'] = open_recalls
                details_dict['recall_status'] = recall_status
                details_dict['name_of_recall'] = name_of_recall.lower().title()
                details_dict['campaign'] = campaign
                details_dict['date_of_recall_announcement'] = date_of_recall_announcement
                details_dict['brief_description_of_recall'] = brief_description_of_recall.lower().title()
                details_dict['safety_risk'] = safety_risk.lower().title()
                details_dict['remedy'] = remedy.lower().title()
                details_list.append(details_dict.copy())

        if len(vehicle_recall_json) == 0:
            dictionary_helper(details_dict, vin_no, open_recalls="No")
            details_list.append(details_dict.copy())

    return details_list


async def runner(all_vins_file_path: str, output_data_file_path: str, concurrency: int, batch_size: int):
    # get all vin nos
    with open(all_vins_file_path) as infile:
        all_vin_nos = [vin.strip() for vin in infile]
        print(f"Total vins: {len(all_vin_nos)}")

    vins_to_scrape = all_vin_nos[:10]
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
    ALL_VINS_FILE_PATH = "vin_nos_chevrolet"
    OUTPUT_DATA_FILE_PATH = "vehicle_record_data_chevrolet_1.csv"
    CONCURRENCY = 45
    BATCH_SIZE = 10

    asyncio.run(runner(all_vins_file_path=ALL_VINS_FILE_PATH, output_data_file_path=OUTPUT_DATA_FILE_PATH,
                       concurrency=CONCURRENCY, batch_size=BATCH_SIZE))
