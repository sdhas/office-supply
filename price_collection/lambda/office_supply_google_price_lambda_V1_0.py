import boto3
import logging
import copy
import json
import re
import requests
import time
import urllib3

from bs4 import BeautifulSoup
from json import JSONEncoder
from datetime import datetime
from unidecode import unidecode

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#### Lambda #####

TABLE_NAME = "google_office_supply_output"
SQS_GOOGLE_OFFICE_SUPPLY_URL = "https://sqs.us-east-2.amazonaws.com/629901033185/url_queue"

sqs = boto3.client('sqs', region_name="us-east-2",)

dynamodb = boto3.resource('dynamodb', region_name="us-east-2")
table = dynamodb.Table(TABLE_NAME)

user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'

urllib3.disable_warnings()

http_session = None

do_retry = False
retry_attempts = 0

report_time = 'all'
report_date = 'None'
CLIENT = 'officesupply'


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
        self.merchants = json.dumps(merchants, cls=ModelEncoder)
        self.reportDate = report_date
        self.reportTime = report_time

# Method to get the inputs from the strike-io-api


def send_message_to_input_queue(message: object):
    sqs.send_message(
        QueueUrl=SQS_GOOGLE_OFFICE_SUPPLY_URL,
        MessageBody=message
    )


def get_inputs_from_event(event):

    # logger.info('## ENVIRONMENT VARIABLES')
    # logger.info(os.environ['AWS_LAMBDA_LOG_GROUP_NAME'])
    # logger.info(os.environ['AWS_LAMBDA_LOG_STREAM_NAME'])
    # logger.info('## EVENT')
    # logger.info(event)

    records = event['Records']
    inputs = []

    try:
        for record in records:
            # message_id = record['messageId']
            input_raw = record['body']

            input_raw_splits = str(input_raw).split('<>')

            # log_and_console_info(f"Received Input to search >> {input_raw}")

            input_req = Request()

            input_req.id = input_raw_splits[0]
            input_req.strikeId = input_raw_splits[1]
            input_req.uniqueId = input_raw_splits[2]
            input_req.sku = input_raw_splits[3]
            input_req.url = input_raw_splits[4]
            input_req.attemptCount = input_raw_splits[5]
            input_req.status = input_raw_splits[6]
            input_req.reportDate = input_raw_splits[7]
            input_req.reportTime = input_raw_splits[8]

            inputs.append(input_req)

    except requests.exceptions.RequestException as req_exception:
        # print(req_exception, exc_info=True)
        logger.error(req_exception, exc_info=True)

    return inputs


def update_input_to_retry(input: Request, status: str):
    """Method to update the input to retry in strike-io-api"""

    attempt_count = int(input.attemptCount) + 1

    retry_input = copy.copy(input)

    retry_input.attemptCount = attempt_count
    retry_input.reportDate = report_date
    retry_input.reportTime = report_time

    log_and_console_info(f'Retry - Updating input status {input.status} to {status}')

    # if(status == 'RETRY'):
    #     if(attempt_count > 1):
    #         retry_input.status = 'CLOSED'
    #     else:
    #         retry_input.status = 'RETRY'
    if(status in ['RETRY', 'NOT_FOUND', 'NO_MERCHANT']):
        if(attempt_count > 1):
            retry_input.status = 'CLOSED'
        else:
            retry_input.status = 'RETRY'

        # update to queue
        retry_inputs = [retry_input]
        retry_input_json = json.dumps(retry_inputs, cls=ModelEncoder)
        log_and_console_info(f"Input with ID {retry_input.id} and attempt count {retry_input.attemptCount} will be attempted again")
        send_message_to_input_queue(retry_input_json)

    elif(status == 'INPUT_ERROR'):
        retry_input.status = 'INPUT_ERROR'


def update_products(products: list):

    log_and_console_info(f'Updating {len(products)} outputs!')

    for product in products:
        response = table.put_item(
            Item=product.__dict__
        )

#####################


def log_and_console_info(message: str):
    # print(message)
    logger.info(message)


def log_and_console_error(message: str):
    # print(message)
    logger.error(message)


def http_client():
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "referer": "https://www.google.com"
        }
    )

    def log_url(res, *args, **kwargs):
        logger.info(f"{res.url}, {res.status_code}")

    session.hooks["response"] = log_url
    return session


def make_request(url: str):
    try:
        http_session = http_client()
        response = http_session.get(url, verify=False)

        if response.status_code == 200:
            return response
        elif response.status_code == 429:
            print_captcha_error()
            log_and_console_error("Quiting - CAPTCHA Occurred!")
            # exit("Quiting - Captcha occurred!")
        else:
            return response
    except requests.exceptions.RequestException as req_exception:
        # print(req_exception, exc_info=True)
        logger.error(req_exception, exc_info=True)
        # wait_till(120)  # wait 2 mins before retry
        return None
    except Exception as ex:
        # print(ex, exc_info=True)
        logger.error(ex, exc_info=True)


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


def extract_data(response_text: str):

    soup = BeautifulSoup(response_text, 'html.parser')

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

                # log_and_console_info(f'Seller [name={name}, price={price}, shipping={shipping}]')
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
        # print(attribute_error, exc_info=True)
        logging.error(attribute_error, exc_info=True)
        return None


def scrape_data(req: Request):
    prod_id = req.id
    strike_id = req.strikeId
    unique_id = req.uniqueId
    sku = req.sku
    url_in = req.url

    # log_and_console_info(f"Searching product with [strike_id={strike_id}, sku={sku}, unique_id={unique_id}]")

    #################################   Extract Details   #################################
    start = datetime.now()

    result = re.search('/product/(.*)/', url_in)

    if(result is not None):
        uid = result.group(1)
        new_url = 'https://www.google.com/shopping/product/<urlid>/offers?prds=cid:<urlid>,cs:1,scoring:p'
        url = new_url.replace('<urlid>', uid)
        html_response = make_request(url)
        # print(html_response)

        if html_response is None:
            log_and_console_error("Error Occurred!")
            # write_into_error_file(input_record)
            update_input_to_retry(req, 'RETRY')
            # continue  # Skipping after writing into error file
        else:
            sellers_details = extract_data(html_response.text)

            if(len(sellers_details)):

                found_sellers = set()
                dist_sellers = []
                for seller in sellers_details:
                    if seller.name.lower() not in found_sellers:
                        dist_sellers.append(seller)
                    found_sellers.add(seller.name.lower())

                # Sorting with the price, second value from the array
                dist_sellers.sort(key=lambda x: float(x.price))

                # Limiting the seller count to 20
                dist_sellers = dist_sellers[0:20]
                # Getting the count of the sellers
                sellers_count = len(dist_sellers)

                log_and_console_info(f'Time taken to scrape this product {datetime.now() - start}')
                log_and_console_info(f"Found {len(dist_sellers)} sellers")
                if(sellers_count > 0):
                    return Product(prod_id, strike_id, unique_id, sku, dist_sellers, report_date, report_time)
                    # update_products([product])
            else:
                log_and_console_info("Not Found valid sellers!")
                update_input_to_retry(req, 'NOT_FOUND')
    else:
        log_and_console_error("Error - Not valid google url!")
        update_input_to_retry(req, 'NOT_FOUND')


def lambda_handler(event, context):

    global report_date
    global do_retry

    try:

        program_start_time = datetime.now()
        log_and_console_info(f"##### Starting the crawling at {program_start_time} #####")

        request_inputs = get_inputs_from_event(event)
        inputs_count = len(request_inputs)

        log_and_console_info(f"Number of inputs received is {inputs_count}")

        if(inputs_count > 0):
            inputs_process_start_time = datetime.now()
            output_list = []
            input_index = 0
            for input in request_inputs:
                # increasing the index
                input_index = input_index + 1
                # Calling the scrape_data method to navigate to the url
                # and get the required information from the website.
                output = scrape_data(input)
                if output is not None:
                    output_list.append(output)
                # Updating the outputs in chunk of OUTPUT_BATCH_SIZE, and the rest outputs when input reached its count
                if len(output_list):
                    # Updating the outputs
                    update_products(output_list)
                    log_and_console_info(f'Time taken to process {len(output_list)} outputs is {datetime.now() - inputs_process_start_time} ')
                    # Clearing the output list
                    output_list.clear()

        # elif(do_retry == False and len(request_inputs) == 0):
        #     log_and_console_info('No more OPEN inputs found! Setting the do_retry flag to True.')
        #     log_and_console_info('Crawling for the RETRY inputs are iniitated!')
        #     do_retry = True
        # else:
        #     if(do_retry):
        #         input_available = False

        log_and_console_info(f"##### Program execution time is {datetime.now() - program_start_time}")

    except Exception as error:
        log_and_console_error(f'Error occurred {error}')
        logger.error(error, exc_info=True)
    finally:
        log_and_console_info('Quiting the application!')
        # quit('Quiting the application!')
