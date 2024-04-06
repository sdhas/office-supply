import os
import boto3
import logging

from datetime import datetime, timezone
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr, Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.getenv('TABLE_NAME')
BASE_FOLDER = os.getenv('BASE_FOLDER') 
BUCKET_NAME = os.getenv('BUCKET_NAME')

OUTPUT_FOLDER = f"{BASE_FOLDER}/output"

dynamodb = boto3.resource('dynamodb', region_name="us-east-2")
table = dynamodb.Table(TABLE_NAME)
s3 = boto3.client('s3')

rcu = 0

def calculate_and_updat_rcu(response):
  """Calculates the RCU consumed by a query response.
  Args:
    response: The response from a DynamoDB query.
  Returns:
    The number of RCUs consumed by the query.
  """
  global rcu

  # Get the number of items returned by the query.
  num_items = len(response['Items'])

  # Calculate the RCU consumed by each item.
  rcu_per_item = 1 if response['Count'] else 0.5

  # Calculate the total RCU consumed by the query.
  total_rcu = num_items * rcu_per_item

  rcu += total_rcu

def create_file_with_input(file_name:str, item_list: list):
    if len(item_list):
        with open(file_name, 'w', encoding='utf8', errors='ignore') as file:
            for item in item_list:
                req = item.get('req')
                input_string = ",".join(req['request_string'].split('\t')[:-1])
                file.write(f"{input_string}\n")
            
            file.close()               
                
        s3_path = f"{OUTPUT_FOLDER}/{file_name}"
        s3.upload_file(file_name, BUCKET_NAME, s3_path)
        logging.info(f"Uploaded file {file_name}")
                    
        # remove the file from /tmp/
        os.remove(file_name)

def lambda_handler(event, context):

    report_date = datetime.now(timezone.utc).strftime('%m%d%Y')

    if 'report_date' in event:
        report_date = str(event['report_date'])

    report_datetime_db = f"{report_date}"

    logging.info(f"Getting the report generated for date {report_date}")
    
    try:
        last_evaluated_key = None
        items = [] # Result Array

        while True:
            if last_evaluated_key == None:
                response = table.scan(
                    FilterExpression = Attr('datetime').eq(report_datetime_db)
                ) # This only runs the first time - provide no ExclusiveStartKey initially
                calculate_and_updat_rcu(response)
            else:
                response = table.scan(
                    FilterExpression = Attr('datetime').eq(report_datetime_db),
                    ExclusiveStartKey=last_evaluated_key # In subsequent calls, provide the ExclusiveStartKey
                )
                calculate_and_updat_rcu(response)

            items.extend(response['Items']) # Appending to our resultset list
            
            # Set our lastEvlauatedKey to the value for next operation,
            # else, there's no more results and we can exit
            if 'LastEvaluatedKey' in response:
                last_evaluated_key = response['LastEvaluatedKey']
            else:
                break

        if len(items):
            logger.info(f"Total number of records : {len(items)}")
            
            file_name = f"Output_{report_datetime_db}.csv"
            not_found_file_name = f"Not_Found_{report_datetime_db}.txt"
            no_sellers_file_name = f"No_Sellers_{report_datetime_db}.txt"
            error_file_name = f"Error_{report_datetime_db}.txt"

            os.chdir('/tmp/')
            ok_list = []
            not_found_list = []
            no_sellers_list = []
            error_list = []
            for item in items:
                if item.get('status') == 'OK':
                    ok_list.append(item)
                elif item.get('status') == 'NOT_FOUND':
                    not_found_list.append(item)
                elif item.get('status') == 'NO_SELLERS':
                    no_sellers_list.append(item)
                elif item.get('status') == 'ERROR':
                    error_list.append(item)

            logging.info(f"Records count is [Total={len(items)}, OK={len(ok_list)}, NOT_FOUND={len(not_found_list)}, NO_SELLERS={len(no_sellers_list)}, ERROR={len(error_list)}]")

            if len(ok_list):
            
                os.chdir('/tmp/')           

                report_stat = f"Records count is [Total={len(ok_list)}]"
                logging.info(report_stat)

                with open(file_name, 'w') as out_file:
                    # Inventory Number/SKU	Last Seen	Merchant 1	Merchant Price 1	Shipping 1
                    out_file.write("Inventory Number/SKU,Last Seen")
                    # Setting the range 1 to 26 to print the merchant till 25
                    for i in range(1, 26):
                        out_file.write(f",Merchant {i},Merchant Price {i},Shipping {i}")
                    out_file.write("\n")

                    for item in ok_list:
                        # print(f"item > {item}")
                        # print(f"Item type is {type(item)}")
                        # print(f"Merchant type is {type(item.get('merchants'))}")
                        ouput_str = item.get('output')
                        if len(ouput_str):
                            modified_ouput_str = ouput_str.replace("|",",")
                            out_file.write(f"{modified_ouput_str}\n")     

                # Closing file                   
                out_file.close()   

                s3_path = f"{OUTPUT_FOLDER}/{file_name}"
                s3.upload_file(file_name, BUCKET_NAME, s3_path)

                logging.info(f"Uploaded output file {file_name}")
                            
                # remove the file from /tmp/
                os.remove(file_name)
            else:
                logging.error("No records found for generting report.")

            # Not Found file
            if len(not_found_list):
                create_file_with_input(not_found_file_name, not_found_list)                

            # No Sellers file
            if len(no_sellers_list):
                create_file_with_input(no_sellers_file_name, no_sellers_list)
            
            # Error file
            if len(error_list):
                create_file_with_input(error_file_name, error_list)
            
        else:
            logger.info("No records found to generate report.")
            return "NOT_FOUND"
                
    except ClientError as e:
        logger.error(f"Exception occurred {e.response['Error']['Message']}")        
        logger.error(e,exc_info=True)
        return "ERROR"
    finally:
        logger.info(f"Consumed RCU is {rcu}")
    
    return "OK"