import logging
import os
import random
import re
import requests
import time
import urllib3

from bs4 import BeautifulSoup
from datetime import datetime
from unidecode import unidecode

IDENTIFICATION_FILE = "identification.txt"
INPUT_FILE = "input.txt"
OUTPUT_FILE = "output.txt"
ERROR_FILE = "error.txt"
NOTFOUND_FILE = "notfound.txt"

mm_dd_yyyy = datetime.now().strftime('%m-%d-%Y')

user_agents = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36']

urllib3.disable_warnings()


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
        output_file.write("Strike ID\tUnique ID\tInventory Number/SKU\tlast_seen")  # Merchant 1	Merchant Price 1	Shipping 1
        # Setting the range 1 to 26 to print the merchant till 25
        for i in range(1, 26):
            output_file.write(f"\tMerchant {i}\tMerchant Price {i}\tShipping {i}")
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
                sellers_out.append((name, price, shipping))

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


def main():
    logging.basicConfig(filename='app.log', format='%(asctime)s %(message)s', level=logging.INFO)
    log_and_console_info(f'Last seen date is {mm_dd_yyyy}')
    program_start_time = datetime.now()
    log_and_console_info(f"##### Starting the crawling at {program_start_time}")

    # Create the output file with headers
    create_output_file()

    client = http_client()
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
        url_in = input_record[3].strip()
        inventory_no = input_record[4].strip()
        result = re.search('/product/(.*)/', url_in)

        if(result is not None):
            uid = result.group(1)
            new_url = 'https://www.google.com/shopping/product/<urlid>/offers?prds=cid:<urlid>,cs:1,scoring:p'
            url = new_url.replace('<urlid>', uid)

            log_and_console_info(f"Searching product with id = {strike_id} and inventory number {inventory_no}")
            time.sleep(0.5)  # Sleeping for 0.5 second
            start = datetime.now()
            html_response = make_request(client, url)

            if html_response is None:
                log_and_console_error("Error Occurred!")
                write_into_error_file(input_record)
                continue  # Skipping after writing into error file
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
                        next_page_response = make_request(client, next_url)
                        if next_page_response is not None:
                            sellers_details.extend(extract_data(next_page_response))

                if(len(sellers_details)):

                    found_sellers = set()
                    dist_sellers = []
                    for seller in sellers_details:
                        if str(seller[0]).lower() not in found_sellers:
                            dist_sellers.append(seller)
                        found_sellers.add(str(seller[0]).lower())

                    # Sorting with the price, second value from the array
                    dist_sellers.sort(key=lambda x: float(x[1]))

                    # Limiting the seller count to 25
                    dist_sellers = dist_sellers[0:25]
                    # Getting the count of the sellers
                    sellers_count = len(dist_sellers)
                    if(sellers_count > 0):
                        output_file = open(OUTPUT_FILE, "a", encoding='utf8', errors='ignore')
                        # Looping the sellers to get average price and concatenate seller values
                        sellers_details_str = ""
                        for seller_info in dist_sellers:
                            sellers_details_str = sellers_details_str + "\t" + seller_info[0] + "\t" + str(seller_info[1]) + "\t" + str(seller_info[2])

                        output_file.write(strike_id + "\t" + unique_id + "\t" + inventory_no + "\t" + mm_dd_yyyy)
                        output_file.write(sellers_details_str)

                        output_file.write('\n')
                        output_file.close()

                    log_and_console_info(f'Time taken {datetime.now() - start}')

                    log_and_console_info(f"Found {len(dist_sellers)} sellers")
                else:
                    log_and_console_info(f"Not Found valid sellers!")
                    write_into_notfound_file(input_record)
                    continue  # Skipping after writing to the notfound file
            update_identification_file(strike_id)
        else:
            log_and_console_error(f"Error - Not valid google url!")

    log_and_console_info(f"##### Program execution time is {datetime.now() - program_start_time} seconds")
    log_and_console_info('--FINISHED--')

    print_data_count()


if __name__ == '__main__':
    main()
