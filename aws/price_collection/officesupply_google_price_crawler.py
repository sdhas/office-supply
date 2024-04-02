import boto3
import logging
import json
import re
import os
import requests
import time
import urllib3

from bs4 import BeautifulSoup
from json import JSONEncoder
from datetime import datetime
from unidecode import unidecode

logger = logging.getLogger()
logger.setLevel(logging.INFO)

OUTPUT_SQS_URL = os.getenv('OUTPUT_SQS_URL') #"https://sqs.us-east-2.amazonaws.com/629901033185/office_supply_google_price_crawling_output_queue"

sqs = boto3.client('sqs', region_name="us-east-2")

mm_dd_yyyy = datetime.now().strftime('%m-%d-%Y')

user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'

urllib3.disable_warnings()

http_session = None

class ModelEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__


class Seller:

    def __init__(self, name, price, shipping):
        self.name = name
        self.price = price
        self.shipping = shipping


class Request:

    def __init__(self, req_string:str):
        if req_string is not None and req_string.strip() != "":        
            req_splits = req_string.strip().split('\t')
            self.strike_id = req_splits[0]
            self.unique_id = req_splits[1]
            self.url = req_splits[2]
            self.sku = req_splits[3]
            self.datetime = str(req_splits[4])
            self.id = f"{req_splits[0]}_{req_splits[4]}".replace(".","-")
            self.request_string = req_string

    def get_request_string(self): 
        return f"{self.strike_id}\t{self.unique_id}\t{self.url}\t{self.sku}\t{self.datetime}"


class Product:

    def __init__(self, id, report_date_time, output_str, status):
        self.id = id
        self.datetime = report_date_time
        self.output = output_str
        self.status = status

def get_inputs_from_event(event):

    records = event['Records']
    inputs = []

    try:
        logger.info(f"Total records in this event is {len(records)}")
        for record in records:
            input_raw = record['body']

            input_raw_splits = str(input_raw).split('\t')

            if len(input_raw_splits) != 5:
                logger.error(f"Input has only {len(input_raw_splits)}/5 - {input_raw}")
                continue

            input_req = Request(input_raw)

            inputs.append(input_req)

    except requests.exceptions.RequestException as req_exception:
        logger.error(req_exception, exc_info=True)

    return inputs

def update_products_to_output_queue(products: list):
    log_and_console_info(f'Updating outputs to ouput queue {len(products)}')

    entries = []
    for product in products:
        # log_and_console_debug(f'Output product id is {product.id}')
        entry = {'Id': product.id, 
                 'MessageBody': json.dumps(product, cls=ModelEncoder)}
        entries.append(entry)

    response = sqs.send_message_batch(QueueUrl=OUTPUT_SQS_URL, 
                                        Entries=entries)
    if response.get('Failed') is not None:
        logger.error(f"Error occurred while sending message to output queue. {response}")
        raise

#####################

def log_and_console_debug(message: str):
    logger.debug(message)
    
def log_and_console_info(message: str):
    logger.info(message)

def log_and_console_error(message: str):
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
        logger.debug(f"{res.url}, {res.status_code}")

    session.hooks["response"] = log_url
    return session


def make_request(url: str):
    try:
        http_session = http_client()
        response = http_session.get(url, verify=False)

        if response.status_code == 200:
            return response
        elif response.status_code == 429:
            log_and_console_error("CAPTCHA Occurred!")
            # exit("Quiting - Captcha occurred!")
        else:
            return response
    except requests.exceptions.RequestException as req_exception:
        # print(req_exception, exc_info=True)
        logger.error(req_exception, exc_info=True)
        return None
    except Exception as ex:
        # print(ex, exc_info=True)
        logger.error(ex, exc_info=True)

def extract_next_urls(response: requests.Response):
    soup = BeautifulSoup(response.text, 'html.parser')
    next_urls = []
    try:
        for next_url_element in soup.select('#sh-fp__pagination-button-wrapper > span > a'):
            next_urls.append(next_url_element.attrs['data-url'])

    except AttributeError as attribute_error:
        logging.error(attribute_error, exc_info=True)
    return next_urls


def extract_data(response: requests.Response):

    soup = BeautifulSoup(response.text, 'html.parser')

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
                
                # Replacing , | from the seller name
                name = name.replace(',',' ').replace('|',' ')
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
        logging.error(attribute_error, exc_info=True)
        return None


def scrape_data(req: Request):
    prod_id = req.id
    strike_id = req.strike_id
    unique_id = req.unique_id
    sku = req.sku
    url_in = req.url
    report_date_time = req.datetime

    log_and_console_info(f"Searching product with [strike_id={strike_id}, sku={sku}, unique_id={unique_id}]")

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
            return Product(prod_id, report_date_time, None, "ERROR")
            # write_into_error_file(input_record)
            # update_input_to_retry(req, 'RETRY')
            # continue  # Skipping after writing into error file
        else:
            sellers_details = extract_data(html_response)            
            next_urls = extract_next_urls(html_response)
            if(len(next_urls)):
                next_urls.pop()
                # Restricting to first 1 URL to limit the sellers count to 25
                limited_next_urls = next_urls[0:1]
                log_and_console_debug(limited_next_urls)
                for next_url in limited_next_urls:
                    next_url = "https://www.google.com" + next_url
                    next_page_response = make_request(next_url)
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

                log_and_console_debug(f'Time taken to scrape this product {datetime.now() - start}')
                log_and_console_info(f"Found {len(dist_sellers)} sellers")
                
                sellers_details_str = ""
                status = "OK"                    
                    
                # Joining list of seller objects 
                sellers_details_str = '|'.join(["|".join([sel.name, str(sel.price), str(sel.shipping)]) for sel in dist_sellers])

                ouput_str = sku + "|" + mm_dd_yyyy + "|" + sellers_details_str
            else:
                log_and_console_info("Not Found valid sellers!")
                status = "NO_SELLERS"
                ouput_str = None

            return Product(prod_id, report_date_time, ouput_str, status)
    else:
        log_and_console_error("Error - Not valid google url!")


def lambda_handler(event, context):

    try:

        program_start_time = datetime.now()
        log_and_console_debug(f"##### Starting the crawling at {program_start_time} #####")

        request_inputs = get_inputs_from_event(event)
        inputs_count = len(request_inputs)

        log_and_console_debug(f"Number of inputs received is {inputs_count}")
        outputs =[]
        if(inputs_count > 0):
            for input in request_inputs:
                # Calling the scrape_data method to navigate to the url
                # and get the required information from the website.
                output = scrape_data(input)
                if output is not None:
                    # update_product(output)  
                    outputs.append(output)
        
        if len(outputs):
            update_products_to_output_queue(outputs)

        log_and_console_debug(f"##### Program execution time is {datetime.now() - program_start_time}")

    except Exception as error:
        log_and_console_error(f'Error occurred {error}')
        logger.error(error, exc_info=True)
    finally:
        log_and_console_debug('Finished execution!')
