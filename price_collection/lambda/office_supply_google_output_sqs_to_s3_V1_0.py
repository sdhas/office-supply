import boto3
import logging
import json
import os

from decimal import Decimal
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

bucket_name = "strikeaprice-ohio"
s3_path = "price_crawling/office_supply/output/" 

OFFICE_SUPPLY_GOOGLE_OUTPUT_SQS_URL = "https://sqs.us-east-2.amazonaws.com/629901033185/office_supply_google_price_crawling_output_queue"

s3 = boto3.client('s3')

sqs = boto3.client('sqs', region_name="us-east-2")

def get_messages_from_output_sqs():

    outputs = []

    # Poll for messages
    read_flag = True

    while read_flag:
        response = sqs.receive_message(
            QueueUrl=OFFICE_SUPPLY_GOOGLE_OUTPUT_SQS_URL,
            AttributeNames=['All'],
            MaxNumberOfMessages=1,  # Adjust as needed
            MessageAttributeNames=['All'],
            VisibilityTimeout=0,
            WaitTimeSeconds=20  # Adjust as needed
        )
        logger.info(f"Received response: {response}")

        messages = response.get('Messages', [])
        if messages:
            logger.info(f"Polled {len(messages)} messages")
            for message in messages:
                # Process the message
                # logger.info(f"Received message: {message['Body']}")
                output_json_data = json.loads(message['Body'], parse_float=Decimal)
                outputs.append(output_json_data)

                # Delete the message from the queue
                receipt_handle = message['ReceiptHandle']

                del_res = sqs.delete_message(
                    QueueUrl=OFFICE_SUPPLY_GOOGLE_OUTPUT_SQS_URL,
                    ReceiptHandle=receipt_handle
                )

                logger.info(f"Delete response is {del_res}")
        else:
            read_flag = False
            logger.info("No messages received. Waiting...")

    return outputs

def lambda_handler(event, context):   
        
    try:

        outputs = get_messages_from_output_sqs() 

        if len(outputs):
            logger.info(f"Total number of records : {len(outputs)}")

            current_date = get_current_ist_date()
            
            file_name = "officesupply_report_" + current_date + ".csv"
            os.chdir('/tmp')

            with open(file_name, 'w') as out_file:
                # Inventory Number/SKU	Last Seen	Merchant 1	Merchant Price 1	Shipping 1
                out_file.write("Inventory Number/SKU,Last Seen")
                # Setting the range 1 to 26 to print the merchant till 25
                for i in range(1, 26):
                    out_file.write(f",Merchant {i},Merchant Price {i},Shipping {i}")
                out_file.write("\n")

                for item in outputs:
                    sellers = item.get('merchants')
                    if len(sellers):
                        sellers_str_list = []
                        sellers_str = ""
                        for seller in sellers:
                            sellers_str_list.append(f"{seller.get('name')},{str(seller.get('price'))},{str(seller.get('shipping'))}")
                        if len(sellers_str_list):
                            sellers_str = ",".join(sellers_str_list)
                        out_file.write(f"{item.get('sku')},{item.get('reportDate')},{sellers_str}\n")

            output_file_full_path = s3_path + file_name
            s3.upload_file(file_name, bucket_name, output_file_full_path)

            logger.info(f"File {output_file_full_path} created.")

        else:
            logger.info("No records found to generate report.")
            return "NOT_FOUND"
        
        # s3.put_object(Body=file_content, Bucket=bucket_name, Key=file_key)
        
    except ClientError as e:
        logger.error(f"Exception occurred {e.response['Error']['Message']}")        
        logger.error(e,exc_info=True)
        # send exception email
        return "ERROR"
    except Exception as ex:
        logger.error(ex,exc_info=True)
        return "ERROR"
    return "OK"

def get_current_ist_date():

    ist_timezone = timezone(timedelta(hours=5, minutes=30))

    # Get the current time in UTC
    utc_now = datetime.utcnow()

    # Convert UTC time to IST time
    ist_now = utc_now.replace(tzinfo=timezone.utc).astimezone(ist_timezone)

    # Format the date as a string
    current_date = ist_now.strftime('%Y-%m-%d')

    return current_date