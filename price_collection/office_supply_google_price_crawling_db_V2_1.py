import logging
import copy
import json
import random
import re
import requests
import time
import urllib3
import socket
import sys

from bs4 import BeautifulSoup
from json import JSONEncoder
from datetime import datetime
from unidecode import unidecode

mm_dd_yyyy = datetime.now().strftime('%m-%d-%Y')

user_agents = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36']

urllib3.disable_warnings()

http_session = None

do_retry = False
retry_attempts = 0

report_time = 'all'
report_date = 'None'
device_name = socket.gethostname()
CLIENT = 'officesupply'

################################################ Strike-Io-API ################################################

STRIKE_IO_API_BASE_URL = "http://194.163.43.249:8383/strike-io-api/google"
# STRIKE_IO_API_BASE_URL = "http://localhost:8383/strike-io-api/google"
STRIKE_IO_API_GET_OPEN_INPUT_URL = f"{STRIKE_IO_API_BASE_URL}/inputs?client={CLIENT}&type=OPEN&count=10&reportDate=<report-date>&reportTime=<report-time>"
STRIKE_IO_API_GET_RETRY_INPUT_URL = f"{STRIKE_IO_API_BASE_URL}/inputs?client={CLIENT}&type=RETRY&count=1&reportDate=<report-date>&reportTime=<report-time>"
STRIKE_IO_API_UPDATE_INPUT_URL = f"{STRIKE_IO_API_BASE_URL}/inputs?client={CLIENT}&type=update&reportDate=<report-date>&reportTime=<report-time>"
STRIKE_IO_API_UPDATE_PRODUCT_URL = f"{STRIKE_IO_API_BASE_URL}/outputs?client={CLIENT}"


class ModelEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__


class Seller:
    """ Do not change the attribute, this is should be identical to the Merchant class in the strik-io-api"""

    def __init__(self, name, price, shipping):
        self.name = name
        self.price = price
        self.shipping = shipping


class Request:
    """ Do not change the attribute, this is should be identical to the Input class in the strik-io-api"""

    def __init__(self):
        self.id = None
        self.strikeId = None
        self.uniqueId = None
        self.sku = None
        self.client = CLIENT
        self.url = None
        self.status = None
        self.deviceName = None
        self.attemptCount = None
        self.reportDate = None
        self.reportTime = None


class Product:
    """ Do not change the attribute, this is should be identical to the Output class in the strik-io-api"""

    def __init__(self, id, strike_id, unique_id, sku, merchants, report_date, report_time):
        self.client = CLIENT
        self.id = id
        self.strikeId = strike_id
        self.uniqueId = unique_id
        self.sku = sku
        self.merchants = merchants
        self.reportDate = report_date
        self.reportTime = report_time

# Method to get the inputs from the strike-io-api


def get_inputs_from_api(type: str):

    inputs = []

    if(type == 'OPEN'):
        strike_input_api_get_inputs_url = STRIKE_IO_API_GET_OPEN_INPUT_URL.replace('<report-date>', report_date).replace('<report-time>', report_time)
    elif(type == 'RETRY'):
        strike_input_api_get_inputs_url = STRIKE_IO_API_GET_RETRY_INPUT_URL.replace('<report-date>', report_date).replace('<report-time>', report_time)

    try:
        session = requests.Session()
        log_and_console_info(strike_input_api_get_inputs_url)
        response = session.get(strike_input_api_get_inputs_url, verify=False)

        # log_and_console_info(f"response >> {response.text}")

        if response.status_code == 200:
            json_array = json.loads(response.text)

            for json_data in json_array:

                log_and_console_info(f"Received Input to search >> {json_data}")

                input_req = Request()

                input_req.id = json_data['id']
                input_req.strikeId = json_data['strikeId']
                input_req.uniqueId = json_data['uniqueId']
                input_req.sku = json_data['sku']
                input_req.url = json_data['url']
                input_req.deviceName = json_data['deviceName']
                input_req.attemptCount = json_data['attemptCount']
                input_req.status = json_data['status']
                input_req.reportDate = json_data['reportDate']
                input_req.reportTime = json_data['reportTime']

                inputs.append(input_req)

        elif(response.status_code == 404):
            log_and_console_info("No inputs available for in strike-io-api for crawling!")
            # do_retry
        else:
            log_and_console_error(f"Something went wrong! http code = {response.status_code}, response = {response.text}")

    except requests.exceptions.RequestException as req_exception:
        logging.error(req_exception, exc_info=True)
        # wait_till(120)  # wait 2 mins before retry

    return inputs


def update_input_to_retry(input: Request, status: str):
    """Method to update the input to retry in strike-io-api"""

    strike_input_api_update_input_url = STRIKE_IO_API_UPDATE_INPUT_URL.replace('<report-date>', report_date).replace('<report-time>', report_time)

    attempt_count = input.attemptCount + 1
    post_header = {'Content-Type': 'application/json'}

    retry_input = copy.copy(input)

    retry_input.deviceName = device_name
    retry_input.attemptCount = attempt_count
    retry_input.reportDate = report_date
    retry_input.reportTime = report_time

    log_and_console_info(f'status {status}')
    log_and_console_info(f'input.status {input.status}')
    log_and_console_info(f'retry_input.status {retry_input.status}')

    if(status == 'RETRY'):
        if(attempt_count > 3):
            retry_input.status = 'CLOSED'
        else:
            retry_input.status = 'RETRY'
    elif(status in ['NOT_FOUND', 'NO_MERCHANT']):
        if(attempt_count > 1):
            retry_input.status = 'CLOSED'
        else:
            retry_input.status = 'RETRY'
    elif(status == 'INPUT_ERROR'):
        retry_input.status = 'INPUT_ERROR'

    retry_input_json = json.dumps(retry_input, cls=ModelEncoder)
    log_and_console_info(f'retry_input_json {retry_input_json}')

    # Send the request
    input_retry_response = requests.post(strike_input_api_update_input_url, data=retry_input_json, headers=post_header)

    if(input_retry_response.status_code == 200):
        log_and_console_info(f"Updated the input id : {input.id} with [status={retry_input.status}, deviceName={device_name}, attemptCount={attempt_count}]")
    else:
        log_and_console_error(f"Update FAILED for the input id : {input.id} with [status={retry_input.status}, device_name={device_name}, attempt_count={attempt_count}]")


def update_product(product: Product):

    product_json = json.dumps(product, cls=ModelEncoder)

    log_and_console_info(f"Inserting product : {product_json}")

    post_header = {'Content-Type': 'application/json'}

    # Send the request
    post_response = requests.post(STRIKE_IO_API_UPDATE_PRODUCT_URL, data=product_json, headers=post_header)

    if(post_response.status_code == 200):
        log_and_console_info(F"Output save SUCCESS! [output id = {product.strikeId}]")
    else:
        log_and_console_error(F"Output save FAILED! [output id = {product.strikeId}]")
        log_and_console_error(F"{post_response.text}]")

#####################


def log_and_console_info(message: str):
    print(message)
    logging.info(message)


def log_and_console_error(message: str):
    print(message)
    logging.error(message)


def http_client():
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": random.choice(user_agents),
            "referer": "https://www.google.com"
        }
    )

    def log_url(res, *args, **kwargs):
        logging.info(f"{res.url}, {res.status_code}")

    session.hooks["response"] = log_url
    return session


def make_request(session: requests.Session, url: str):
    while True:
        try:
            response = session.get(url, verify=False)

            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                print_captcha_error()
                log_and_console_error("Quiting - CAPTCHA Occurred!")
                exit("Quiting - Captcha occurred!")
            else:
                return
        except requests.exceptions.RequestException as req_exception:
            logging.error(req_exception, exc_info=True)
            wait_till(120)  # wait 2 mins before retry


def wait_till(duration: int):
    log_and_console_info(f'Sleeping for {duration} seconds.')

    for i in range(1, duration):
        print(f'Resuming in {duration - i} seconds...')
        time.sleep(1)


def print_captcha_error():

    log_and_console_error("CAPTCHA Occurred!")

    log_and_console_error('   _____              _____    _______    _____   _    _             ')
    log_and_console_error('  / ____|     /\     |  __ \  |__   __|  / ____| | |  | |     /\     ')
    log_and_console_error(' | |         /  \    | |__) |    | |    | |      | |__| |    /  \    ')
    log_and_console_error(' | |        / /\ \   |  ___/     | |    | |      |  __  |   / /\ \   ')
    log_and_console_error(' | |____   / ____ \  | |         | |    | |____  | |  | |  / ____ \  ')
    log_and_console_error('  \_____| /_/    \_\ |_|         |_|     \_____| |_|  |_| /_/    \_\ ')


def open_inputs_from_file(filename: str):
    log_and_console_info(f"Opening input file {filename}")
    lines = []
    with open(filename, 'r') as f:
        inputs = f.readlines()
    for input_record in inputs:
        if(input_record.strip() != ''):
            lines.append(input_record.strip().split('\t'))
    return lines


def extract_next_urls(response: requests.Response):
    soup = BeautifulSoup(response.text, 'lxml')
    next_urls = []
    try:
        for next_url_element in soup.select('#sh-fp__pagination-button-wrapper > span > a'):
            next_urls.append(next_url_element.attrs['data-url'])

    except AttributeError as attribute_error:
        logging.error(attribute_error, exc_info=True)
    return next_urls


def extract_data(response: requests.Response):
    soup = BeautifulSoup(response.text, 'lxml')

    # Saving the html content as a html file
    # with open(strike_id+'.html', 'w', encoding='utf-8') as f:
    #     f.write(str(soup))

    try:
        sellers_out = []
        sellers = soup.select('#sh-osd__online-sellers-cont > tr')

        ind = 1
        for seller in sellers:
            name = price = "na"
            shipping = 0.0
            try:
                name_element = seller.select_one('td:nth-child(1) > div:nth-child(1)')

                if(name_element.attrs['class'].__contains__('sh-osd__pb-grid-wrapper')):
                    # Skipping if the hidden class is found in div
                    continue

                # print(name_element.attrs['class'])

                if(len(name_element.select('a')) == 1):

                    # Removing the span that is not needed
                    span_to_remove = name_element.find('span')
                    span_to_remove.decompose()

                    name = seller.select_one('td:nth-child(1) > div:nth-child(1) > a').text
                else:
                    name = seller.select_one('td:nth-child(1) > div:nth-child(1)').text

                price_table = seller.select('td:nth-child(4) > div > div.sh-osd__content > table > tr')

                for price_tr in price_table:
                    # creating a dictionary with the details we get about pricing
                    label = price_tr.select_one('td:nth-child(1)').text
                    # If the lable is not empty then we get the value
                    if(len(label)):
                        value = price_tr.select_one('td:nth-child(2)').contents[0].getText().replace('$', '').replace(',', '')
                        # print(label, value)
                        if(value.isnumeric()):
                            value = float(value)
                        elif(value.__contains__('now')):
                            value = value.split(' now')[0]
                            value = float(value)
                        # Swith case to set the price details accordingly
                        match label:
                            case 'Item price':
                                price = value
                            case 'Shipping':
                                shipping = value
                if(name == 'na' or price == 'na' or shipping == 'See website'):
                    continue  # Skipping the sellers with invaldi data

                name = re.sub('^[^0-9a-zA-Z]', '', unidecode(name))

                log_and_console_info(f'Seller [name={name}, price={price}, shipping={shipping}]')
                sellers_out.append(Seller(name, price, shipping))

            except Exception as error:

                log_and_console_error(f'Error occurred while scrapping data from seller at position {ind}')
                logging.error(error)

                continue  # Skipping this seller

            finally:
                # Increment the index
                ind = ind + 1

        return sellers_out
    except AttributeError as attribute_error:
        logging.error(attribute_error, exc_info=True)
        return None


def extract_data(response: requests.Response):
    soup = BeautifulSoup(response.text, 'lxml')

    # Saving the html content as a html file
    # with open(strike_id+'.html', 'w', encoding='utf-8') as f:
    #     f.write(str(soup))

    try:
        sellers_out = []
        sellers = soup.select('#sh-osd__online-sellers-cont > tr')

        ind = 1
        for seller in sellers:
            name = price = "na"
            shipping = 0.0
            try:
                name_element = seller.select_one('td:nth-child(1) > div:nth-child(1)')

                if(name_element.attrs['class'].__contains__('sh-osd__pb-grid-wrapper')):
                    # Skipping if the hidden class is found in div
                    continue

                # print(name_element.attrs['class'])

                if(len(name_element.select('a')) == 1):

                    # Removing the span that is not needed
                    span_to_remove = name_element.find('span')
                    span_to_remove.decompose()

                    name = seller.select_one('td:nth-child(1) > div:nth-child(1) > a').text
                else:
                    name = seller.select_one('td:nth-child(1) > div:nth-child(1)').text

                price_table = seller.select('td:nth-child(4) > div > div.sh-osd__content > table > tr')

                for price_tr in price_table:
                    # creating a dictionary with the details we get about pricing
                    label = price_tr.select_one('td:nth-child(1)').text
                    # If the lable is not empty then we get the value
                    if(len(label)):
                        value = price_tr.select_one('td:nth-child(2)').contents[0].getText().replace('$', '').replace(',', '')
                        # print(label, value)
                        if(value.isnumeric()):
                            value = float(value)
                        elif(value.__contains__('now')):
                            value = value.split(' now')[0]
                            value = float(value)
                        # Swith case to set the price details accordingly
                        match label:
                            case 'Item price':
                                price = value
                            case 'Shipping':
                                shipping = value
                if(name == 'na' or price == 'na' or shipping == 'See website'):
                    continue  # Skipping the sellers with invaldi data

                name = re.sub('^[^0-9a-zA-Z]', '', unidecode(name))

                log_and_console_info(f'Seller [name={name}, price={price}, shipping={shipping}]')
                sellers_out.append(Seller(name, price, shipping))

            except Exception as error:

                log_and_console_error(f'Error occurred while scrapping data from seller at position {ind}')
                logging.error(error)

                continue  # Skipping this seller

            finally:
                # Increment the index
                ind = ind + 1

        return sellers_out
    except AttributeError as attribute_error:
        logging.error(attribute_error, exc_info=True)
        return None


def scrape_data(req: Request):
    prod_id = req.id
    strike_id = req.strikeId
    unique_id = req.uniqueId
    sku = req.sku
    url_in = req.url

    log_and_console_info(f"Searching product with [strike_id={strike_id}, sku={sku}, unique_id={unique_id}]")

    #################################   Extract Details   #################################
    start = datetime.now()

    log_and_console_info(f'Time taken to scrape this product {datetime.now() - start}')

    result = re.search('/product/(.*)/', url_in)

    if(result is not None):
        uid = result.group(1)
        new_url = 'https://www.google.com/shopping/product/<urlid>/offers?prds=cid:<urlid>,cs:1,scoring:p'
        url = new_url.replace('<urlid>', uid)
        html_response = make_request(http_session, url)

        if html_response is None:
            log_and_console_error("Error Occurred!")
            # write_into_error_file(input_record)
            update_input_to_retry(req, 'RETRY')
            # continue  # Skipping after writing into error file
        else:
            sellers_details = extract_data(html_response)
            next_urls = extract_next_urls(html_response)
            if(len(next_urls)):
                next_urls.pop()
                # Restricting to first 2 URL to limit the sellers count to 25
                first_three_urls = next_urls[0:2]
                log_and_console_info(first_three_urls)
                for next_url in first_three_urls:
                    next_url = "https://www.google.com" + next_url
                    next_page_response = make_request(http_session, next_url)
                    if next_page_response is not None:
                        sellers_details.extend(extract_data(next_page_response))

            if(len(sellers_details)):

                found_sellers = set()
                dist_sellers = []
                for seller in sellers_details:
                    if seller.name.lower() not in found_sellers:
                        dist_sellers.append(seller)
                    found_sellers.add(seller.name.lower())

                # Sorting with the price, second value from the array
                dist_sellers.sort(key=lambda x: float(x.price))

                # Limiting the seller count to 25
                dist_sellers = dist_sellers[0:25]
                # Getting the count of the sellers
                sellers_count = len(dist_sellers)
                if(sellers_count > 0):
                    product = Product(prod_id, strike_id, unique_id, sku, dist_sellers, report_date, report_time)
                    update_product(product)
                log_and_console_info(f'Time taken {datetime.now() - start}')

                log_and_console_info(f"Found {len(dist_sellers)} sellers")
            else:
                log_and_console_info(f"Not Found valid sellers!")
                update_input_to_retry(req, 'NOT_FOUND')
    else:
        log_and_console_error(f"Error - Not valid google url!")
        update_input_to_retry(req, 'NOT_FOUND')


def main():
    global report_date
    global do_retry
    global http_session

    http_session = http_client()

    try:
        logging.basicConfig(filename='app.log', format='%(asctime)s %(message)s', level=logging.INFO)

        log_and_console_info(f'Received {len(sys.argv)} arguments')

        if(len(sys.argv) < 2):
            log_and_console_error("Missing mandatory argument reportDate!")
            sys.exit("Stopping the script! Missing mandatory argument reportDate!")
        else:
            report_date = sys.argv[1]

            log_and_console_info(f"Arguments passed to the script [reportDate={report_date}]")

        program_start_time = datetime.now()
        log_and_console_info(f"##### Starting the crawling at {program_start_time}")

        input_available = True
        while(input_available):

            if(do_retry):
                request_inputs = get_inputs_from_api('RETRY')
            else:
                request_inputs = get_inputs_from_api('OPEN')

            if(len(request_inputs) > 0):
                for input in request_inputs:
                    # Calling the scareForInput method to navigate to the url
                    # and get the required information from the website.
                    scrape_data(input)
            elif(do_retry == False and len(request_inputs) == 0):
                log_and_console_info('No more OPEN inputs found! Setting the do_retry flag to True.')
                log_and_console_info('Crawling for the RETRY inputs are iniitated!')
                do_retry = True
            else:
                if(do_retry):
                    input_available = False

        log_and_console_info(f"##### Program execution time is {datetime.now() - program_start_time}")

    except Exception as error:
        log_and_console_error(f'Error occurred {error}')
        logging.error(error, exc_info=True)
    finally:
        log_and_console_info('Quiting the application!')
        quit('Quiting the application!')


if __name__ == '__main__':
    main()
