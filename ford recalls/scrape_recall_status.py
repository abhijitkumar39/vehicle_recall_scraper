import csv
import os
import time

import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from collections import OrderedDict


def get_recall_status(driver, vin_no):
    driver.get("https://www.ford.com/support/recalls/")
    driver.find_element(By.XPATH, "//*[@id='vin-field-vin-selector-label']").clear()
    driver.find_element(By.XPATH, "//*[@id='vin-field-vin-selector-label']").send_keys(vin_no)
    continue_click = driver.find_element(By.XPATH, "*//span[@class='button-label' and text()='See Recalls']")
    try:
        continue_click.click()
    except Exception as e:
        time.sleep(2)
        continue_click.click()
    try:
        WebDriverWait(driver, 5).until(
            EC.visibility_of_element_located((By.XPATH, "(*//div[@class='support-accordion'])[1]")))
    except Exception as e:
        return "Invalid Vin no"

    element = driver.find_element(By.XPATH, "(*//div[@class='support-accordion'])[1]")
    return element.get_attribute('innerHTML')


def parse_html(driver, vin_no):
    vin_nos = []
    recall_name = []
    recall_status = []

    html_string = get_recall_status(driver, vin_no)
    if html_string == 'Invalid Vin no':
        vin_nos = [vin_no]
        recall_name = ['Invalid_vin']
        recall_status = ['Invalid_vin']
    soup = BeautifulSoup(html_string, "html.parser")

    # Find all elements with class "accordion-title false"
    accordion_titles = soup.find_all(class_="accordion-title false")
    accordion_status = soup.find_all(class_="accordion-subtitle")

    for title in accordion_titles:
        name_of_recall = title.text.strip().lower().title()
        vin_nos.append(vin_no)
        recall_name.append(name_of_recall)
    for title in accordion_status:
        status = title.text.strip().lower().title()
        recall_status.append(status)

    # dictionary of lists
    dict = {'vin_no': vin_nos, 'name': recall_name, 'status': recall_status}
    print(dict)
    df = pd.DataFrame(dict)
    # saving the dataframe
    df.to_csv('recall_status.csv', header=False, index=False, mode='a')
    vin_nos.clear()
    recall_name.clear()
    recall_status.clear()


def vins_with_open_recalls(vins_file_path):
    df = pd.read_csv(vins_file_path)
    open_recalls = df.loc[df['open_recalls'] == 'Yes', 'vin'].tolist()
    vins_with_recall_status = list(OrderedDict.fromkeys(open_recalls))
    return vins_with_recall_status


if __name__ == '__main__':
    ALL_VINS_FILE_PATH = "vin_nos_ford"
    OUTPUT_DATA_FILE_PATH = "recall_status.csv"
    vins_with_recall_status = vins_with_open_recalls('vehicle_record_data_ford.csv')

    driver = webdriver.Chrome(executable_path="chromedriver")

    scrapped_vins = set()
    mode = 'w'
    if os.path.exists('recall_status.csv'):
        mode = 'a'
        with open('recall_status.csv', newline='') as f:
            data = csv.DictReader(f)
            for row in data:
                scrapped_vins.add(row["vin_no"])

    # List of all urls that we need to scrape
    urls_to_scrape = [url for url in vins_with_recall_status if url not in scrapped_vins]
    # random.shuffle(urls_to_scrape)
    print(f"Urls already scrapped: {len(scrapped_vins)}")
    print(f"Urls to scrape: {len(urls_to_scrape)}")
    print()
    for vin in urls_to_scrape:
        print(vin)
        parse_html(driver, vin_no=vin)
