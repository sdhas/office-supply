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

user_agents = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
               'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3']

urllib3.disable_warnings()
cookie = None
heaers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "sec-ch-ua": "\"Not_A Brand\";v=\"99\", \"Google Chrome\";v=\"109\", \"Chromium\";v=\"109\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "upgrade-insecure-requests": "1"
}


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


def update_cookie_from_response(response):
    global cookie
    tmp_cookie = ''
    for header in response.getheaders():
        if header[0] == 'Set-Cookie':
            tmp_cookie = tmp_cookie + header[1] + ';'

    if tmp_cookie != '':
        cookie = tmp_cookie


# def http_client():
#     session = requests.Session()
#     session.headers.update(
#         {
#             "User-Agent": random.choice(user_agents),
#             "referer": "https://www.google.com"
#         }
#     )

#     def log_url(res, *args, **kwargs):
#         logging.info(f"{res.url}, {res.status_code}")

#     session.hooks["response"] = log_url
#     return session


# def make_request(url: str):
#     attempt = 1
#     while True:
#         try:
#             session = http_client()

#             sid = random.randint(1111, 99999999)
#             proxy_netnut_username_sid = f'{proxy_netnut_username}-sid-{sid}'
#             resolved_http_proxy_url = f'http://{proxy_netnut_username_sid}:{proxy_netnut_password}@{proxy_netnut_url}:{proxy_netnut_port}'
#             # log_and_console_info(resolved_http_proxy_url)
#             session.proxies = {'http': resolved_http_proxy_url, 'https': resolved_http_proxy_url}
#             response = session.get(url)
#             ip_content = session.get('https://ip.nf/me.json')
#             log_and_console_info(f'IP {ip_content.text}')
#             if response.status_code == 200:
#                 if response.text.__contains__('Robot or human?'):
#                     log_and_console_error('Robot or human? - Robot Detected!')
#                     print_robot_error()
#                     raise Exception('Robot or human?')
#                 return response
#             elif response.status_code == 404:
#                 log_and_console_info('Product Not Found!')
#                 return 'NOT_FOUND'
#             elif response.status_code == 429:
#                 print_captcha_error()
#                 raise Exception('Captcha Error')
#             else:
#                 return
#         except Exception as exception:

#             log_and_console_error(f'Attempt {attempt} Failed!')
#             if(attempt == 5):
#                 return None
#             logging.error(exception, exc_info=True)
#             wait_till(5)  # wait before retry
#             attempt = attempt + 1


def call_url(url: str):
    global heaers
    global cookie
    attempt = 1
    while True:
        try:
            heaers.update({"User-Agent": random.choice(user_agents)})
            if cookie is not None:
                heaers.update({'cookie': cookie})

            sid = random.randint(1111, 99999999)
            proxy_netnut_username_sid = f'{proxy_netnut_username}-sid-{sid}'
            resolved_http_proxy_url = f'http://{proxy_netnut_username_sid}:{proxy_netnut_password}@{proxy_netnut_url}:{proxy_netnut_port}'
            # log_and_console_info(resolved_http_proxy_url)
            proxy_handler = urllib.request.ProxyHandler({'http': resolved_http_proxy_url, 'https': resolved_http_proxy_url})
            opener = urllib.request.build_opener(proxy_handler)
            opener.addheaders = heaers.items()
            urllib.request.install_opener(opener)
            # ip_content = urllib.request.urlopen('https://ip.nf/me.json').read().decode()
            # log_and_console_info(f'IP {ip_content}')
            response = urllib.request.urlopen(url)
            content = response.read().decode()

            if content.__contains__('Robot or human?'):
                logging.error('Robot or human? - Robot Detected!')
                print_robot_error()
                raise Exception('Robot or human? - Robot Detected!')

            # Updating the global cookie with the cookies from response
            update_cookie_from_response(response)
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
            wait_till(5)  # wait 30 seconds before retry
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


def print_robot_error():
    log_and_console_error("ROBOT Detected!")
    print('  _____    ____   ____    ____  _______ ')
    print(' |  __ \  / __ \ |  _ \  / __ \|__   __|')
    print(' | |__) || |  | || |_) || |  | |  | |   ')
    print(' |  _  / | |  | ||  _ < | |  | |  | |   ')
    print(' | | \ \ | |__| || |_) || |__| |  | |   ')
    print(' |_|  \_\ \____/ |____/  \____/   |_|   ')


def print_captcha_error():

    log_and_console_error("CAPTCHA Occurred!")

    print('   _____              _____    _______    _____   _    _             ')
    print('  / ____|     /\     |  __ \  |__   __|  / ____| | |  | |     /\     ')
    print(' | |         /  \    | |__) |    | |    | |      | |__| |    /  \    ')
    print(' | |        / /\ \   |  ___/     | |    | |      |  __  |   / /\ \   ')
    print(' | |____   / ____ \  | |         | |    | |____  | |  | |  / ____ \  ')
    print('  \_____| /_/    \_\ |_|         |_|     \_____| |_|  |_| /_/    \_\ ')


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
    logging.basicConfig(filename='app.log', format='%(asctime)s %(message)s', level=logging.INFO)

    read_and_set_credentials()
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
        random_sleep = random.randint(1, 3)
        log_and_console_info(f"Sleeping for {random_sleep} second!")
        time.sleep(random_sleep)  # sleeping
        html_response = call_url(url)
        # html_response = make_request(url)

        # if html_response is None:
        if html_response is None:
            log_and_console_error("Error Occurred!")
            write_into_error_file(input_record)
            continue  # Skipping after writing into error file
        else:
            title = sku = gtin13 = brand = model = price = availability = item_condition = delivery = reviews = rating = image = 'na'

            if(html_response != 'NOT_FOUND'):
                (title, sku, gtin13, brand, model, price, availability, item_condition, delivery, reviews, rating, image) = extract_data(html_response)

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
