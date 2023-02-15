import logging
import os
import requests
import time
import urllib3
import json
import random
import urllib.request

from bs4 import BeautifulSoup
from datetime import datetime

IDENTIFICATION_FILE = "identification.txt"
INPUT_FILE = "input.txt"
OUTPUT_FILE = "output.txt"
ERROR_FILE = "error.txt"
NOTFOUND_FILE = "notfound.txt"
CREDS_FILE = 'C:\strikeaprice\creds.txt'

mm_dd_yyyy = datetime.now().strftime('%m-%d-%Y')

proxy_netnut_url = None
proxy_netnut_port = None
proxy_netnut_username = None
proxy_netnut_password = None
resolved_http_proxy_url = f'http://{proxy_netnut_username}:{proxy_netnut_password}@{proxy_netnut_url}:{proxy_netnut_port}'

user_agents = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
               'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3']

urllib3.disable_warnings()


def log_and_console_info(message: str):
    print(f'INFO : {message}')
    logging.info(f'INFO : {message}')


def log_and_console_error(message: str):
    print(f'ERROR : {message}')
    logging.error(f'ERROR : {message}')


def read_and_set_credentials():
    global proxy_netnut_url
    global proxy_netnut_port
    global proxy_netnut_username
    global proxy_netnut_password

    log_and_console_info(f"Opening creds file {CREDS_FILE}")

    with open(CREDS_FILE, 'r') as f:
        inputs = f.readlines()
    for input_record in inputs:
        if(input_record.strip() != ''):
            prop = input_record.strip().split('=')
            if prop[0] == 'proxy.netnut.url':
                proxy_netnut_url = prop[1]
                log_and_console_info(f'Proxy url set : {proxy_netnut_url}')
            elif prop[0] == 'proxy.netnut.port':
                proxy_netnut_port = prop[1]
                log_and_console_info(f'Proxy port set : {proxy_netnut_port}')
            elif prop[0] == 'proxy.netnut.username':
                proxy_netnut_username = prop[1]
                log_and_console_info(f'Proxy username set : {proxy_netnut_username}')
            elif prop[0] == 'proxy.netnut.password':
                proxy_netnut_password = prop[1]
                log_and_console_info(f'Proxy password set : masked (********)')
    if(None in [proxy_netnut_url, proxy_netnut_port, proxy_netnut_username, proxy_netnut_password]):
        log_and_console_error("Not all the required credentials are found!")
        quit("Not all the required credentials are found!")


def prepare_reuest_client():
    proxy = urllib.request.ProxyHandler({'https': 'http://user-83896:Strike$123@205.164.3.104:1212'})
    opener = urllib.request.build_opener(proxy)
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    urllib.request.install_opener(opener)


def call_url(url: str):
    attempt = 1
    while True:
        try:
            proxy_handler = urllib.request.ProxyHandler({'http': resolved_http_proxy_url})
            opener = urllib.request.build_opener(proxy_handler)
            opener.addheaders = [('User-agent', random.choice(user_agents))]
            urllib.request.install_opener(opener)
            content = urllib.request.urlopen(url).read().decode()

            if content.__contains__('Robot or human?'):
                logging.error('Blocked by walmart!')
                raise Exception('Blocked')

            return content

        except urllib.error.HTTPError as http_error:
            if http_error.code == 404:
                log_and_console_info(f'Product Not Found!')
                return 'NOT_FOUND'
        except Exception as exception:
            log_and_console_error(f'Attempt {attempt} Failed!')
            if(attempt == 5):
                return None
            logging.error(exception, exc_info=True)
            wait_till(30)  # wait 30 seconds before retry
            attempt = attempt + 1


def wait_till(duration: int):
    log_and_console_info(f'Sleeping for {duration} seconds.')

    for i in range(1, duration):
        print(f'Resuming in {duration - i} seconds...')
        time.sleep(1)


def print_data_count():
    input_count = output_count = error_count = notfound_count = 0
    with open(INPUT_FILE, 'r', encoding='utf8', errors='replace') as input_file:
        input_count = len(input_file.readlines())

    if(os.path.exists(OUTPUT_FILE)):
        with open(OUTPUT_FILE, 'r', encoding='utf8', errors='replace') as output_file:
            output_count = len(output_file.readlines())
            output_count = output_count - 1  # Excluding the titles

    if(os.path.exists(ERROR_FILE)):
        with open(ERROR_FILE, 'r', encoding='utf8', errors='replace') as error_file:
            error_count = len(error_file.readlines())

    if(os.path.exists(NOTFOUND_FILE)):
        with open(NOTFOUND_FILE, 'r', encoding='utf8', errors='replace') as notfound_file:
            notfound_count = len(notfound_file.readlines())

    log_and_console_info("\n\n")
    log_and_console_info("########################################")
    log_and_console_info(f"##### Total Input is     : {input_count}")
    log_and_console_info(f"##### Total Output is    : {output_count}")
    log_and_console_info(f"##### Total Error is     : {error_count}")
    log_and_console_info(f"##### Total Not Found is : {notfound_count}")
    log_and_console_info("########################################")


def print_captcha_error():

    log_and_console_error("CAPTCHA Occurred!")

    log_and_console_error('   _____              _____    _______    _____   _    _             ')
    log_and_console_error('  / ____|     /\     |  __ \  |__   __|  / ____| | |  | |     /\     ')
    log_and_console_error(' | |         /  \    | |__) |    | |    | |      | |__| |    /  \    ')
    log_and_console_error(' | |        / /\ \   |  ___/     | |    | |      |  __  |   / /\ \   ')
    log_and_console_error(' | |____   / ____ \  | |         | |    | |____  | |  | |  / ____ \  ')
    log_and_console_error('  \_____| /_/    \_\ |_|         |_|     \_____| |_|  |_| /_/    \_\ ')


def write_into_error_file(input_record_splits: list):
    error_file = open(ERROR_FILE, "a")

    flag = False
    for input_split in input_record_splits:
        if flag:
            error_file.write("\t")
        flag = True
        error_file.write(str(input_split).strip())

    error_file.write("\n")
    error_file.close()


def write_into_notfound_file(input_record_splits: list):
    notfound_file = open(NOTFOUND_FILE, "a")

    flag = False
    for input_split in input_record_splits:
        if flag:
            notfound_file.write("\t")
        flag = True
        notfound_file.write(str(input_split).strip())

    notfound_file.write("\n")
    notfound_file.close()


def update_identification_file(strike_id: str):
    identification_file = open(IDENTIFICATION_FILE, "a")
    identification_file.write(strike_id + "\t" + str(datetime.now()) + "\n")
    identification_file.close()


def get_identification_value():
    log_and_console_info(f"Opening identification file {IDENTIFICATION_FILE}")
    indent_ids = []
    idents = []
    last_id = 'none'
    if(os.path.exists(IDENTIFICATION_FILE)):
        with open(IDENTIFICATION_FILE, 'r') as f:
            idents = f.readlines()
        if(len(idents)):
            for ident in idents:
                indent_ids.append(ident.split('\t')[0])

            last_id = indent_ids[-1]
    return last_id


def create_output_file():
    if(not os.path.exists(OUTPUT_FILE)):
        output_file = open(OUTPUT_FILE, "a")
        output_file.write("Strike ID\tUnique ID\tInventory Number/SKU\tTitle\tW_SKU\tW_GTIN13\tBrand\tModel\tPrice\tAvaialbility\tCondition\tDelivery\tReview\tRating\tImage")
        output_file.write("\n")
        output_file.close()


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


# def extract_data(response: requests.Response):
def extract_data(content: str):

    title = sku = gtin13 = brand = model = price = availability = item_condition = delivery = reviews = rating = image = 'na'
    soup = BeautifulSoup(content, 'lxml')

    # Saving the html content as a html file
    # with open(strike_id+'.html', 'w', encoding='utf-8') as f:
    #     f.write(str(soup))

    try:
        data = (json.loads(soup.find('script', attrs={'type': 'application/ld+json'}).text))

        title = data['name']
        sku = data['sku']
        gtin13 = data['gtin13']
        brand = data['brand']['name']
        model = data['model']
        image = data['image']

        if 'offers' in data:
            offers = data['offers']
            availability = str(offers['availability']).split('/')[-1]
            item_condition = str(offers['itemCondition']).split('/')[-1]
            delivery = str(offers['availableDeliveryMethod']).split('/')[-1]

            if 'price' in offers:
                price = data['offers']['price']
        if 'aggregateRating' in data:
            reviews = data['aggregateRating']['reviewCount']
            rating = data['aggregateRating']['ratingValue']

    except AttributeError as attribute_error:
        logging.error(attribute_error, exc_info=True)
        return None

    log_and_console_info(
        f'Seller [title={title}, sku={sku}, gtin13={gtin13}, brand={brand}, model={model}, price={price}, availability={availability}, item_condition={item_condition}, delivery={delivery}, reviews={reviews}, rating={rating}, image={image}]')
    return (title, sku, gtin13, brand, model, price, availability, item_condition, delivery, reviews, rating, image)


def main():
    read_and_set_credentials()
    logging.basicConfig(filename='app.log', format='%(asctime)s %(message)s', level=logging.INFO)
    log_and_console_info(f'Last seen date is {mm_dd_yyyy}')
    program_start_time = datetime.now()
    log_and_console_info(f"##### Starting the crawling at {program_start_time}")

    # Create the output file with headers
    create_output_file()

    # Returns 'none' if there is no identification file created
    last_id = get_identification_value()
    log_and_console_info(f'last id : {last_id}')

    inputs = open_inputs_from_file(INPUT_FILE)
    total_input_count = len(inputs)

    # Looping the list of inputs as range to get the index of the input
    for i in range(len(inputs)):
        input_record = inputs[i]
        strike_id = input_record[0].strip()
        ###################### Identification block ######################
        # If last_id is 'none' [identification file not created] or 'found_last_value' [last value in identification is ound in input file], then start program
        # If last_id is a valid input id and matched with input list, then set found_last_value to last_id
        if(last_id != 'none' and last_id != 'found_last_value'):
            if(last_id != strike_id):
                continue  # Skipping the input if that is not the last value written in identification file
            elif(last_id == strike_id):
                last_id = 'found_last_value'
                continue  # Setting new value and Skipping the input last value, if that is the last value written in identification file

        log_and_console_info(f"################ Processing input {i+1} of {total_input_count} ################")

        unique_id = input_record[1].strip()
        inventory_no = input_record[2].strip()
        url_in = input_record[3].strip()
        url = url_in.split('?')[0]

        log_and_console_info(f"Searching product with id = {strike_id} and url {url}")
        time.sleep(0.5)  # Sleeping for 0.5 second
        start = datetime.now()

        log_and_console_info("Sleeping for 1 second!")
        time.sleep(1)  # sleeping for 1 second
        web_content = call_url(url)

        # if html_response is None:
        if web_content is None:
            log_and_console_error("Error Occurred!")
            write_into_error_file(input_record)
            continue  # Skipping after writing into error file
        else:
            title = sku = gtin13 = brand = model = price = availability = item_condition = delivery = reviews = rating = image = 'na'

            if(web_content != 'NOT_FOUND'):
                (title, sku, gtin13, brand, model, price, availability, item_condition, delivery, reviews, rating, image) = extract_data(web_content)

            output_file = open(OUTPUT_FILE, "a", encoding='utf8', errors='ignore')
            output_file.write(strike_id + "\t" + unique_id + "\t" + inventory_no + "\t" + title + "\t" + str(sku) + "\t" + str(gtin13) + "\t" + brand + "\t" + str(model) +
                              "\t" + str(price) + "\t" + availability + "\t" + item_condition + "\t" + delivery + "\t" + str(reviews) + "\t" + str(rating) + "\t" + image + "\n")
            output_file.close()

            log_and_console_info(f'Time taken {datetime.now() - start}')

        update_identification_file(strike_id)

    log_and_console_info(f"##### Program execution time is {datetime.now() - program_start_time} seconds")
    log_and_console_info('--FINISHED--')

    print_data_count()


if __name__ == '__main__':
    main()
