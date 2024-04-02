import os
import re
import boto3
import logging
import random
import string

from datetime import datetime, timezone

CLIENT = "OfficeSupply"
REPORT_TYPE = "Google"

INPUT_SQS_URL = os.getenv('INPUT_SQS_URL')
BASE_FOLDER = os.getenv('BASE_FOLDER')
BUCKET_NAME = os.getenv('INPUT_BUCKET')
NOTIFICATION_SQS_URL = os.getenv("NOTIFICATION_SQS_URL","https://sqs.us-east-2.amazonaws.com/629901033185/chitti_notifications")

INPUT_FOLDER = f"{BASE_FOLDER}/input/"
INPUT_FILE = f"{BASE_FOLDER}/input/input.txt"
PROCESSED_FILE = f"{BASE_FOLDER}/processed/<processed_file>.txt"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
sqs = boto3.client('sqs', region_name="us-east-2")  # client is required to interact with

def chitti_post(message: str):
    try:
        sqs = boto3.client('sqs', region_name="us-east-2")
        response = sqs.send_message(QueueUrl=NOTIFICATION_SQS_URL, MessageBody=message)
        if response.get('Failed') is not None:
            logger.error(f"Error occurred. {response}")

    except Exception as ex:
        logger.error(f"Error {ex}")

def lambda_handler(event, context):

    try:
        mmddyyyy = datetime.now(timezone.utc).strftime('%m%d%Y')
        if 'report_date' in event:
            mmddyyyy = str(event['report_date'])

        logger.info(f"Loading requests for report date {mmddyyyy}")

        report_datetime = f"{mmddyyyy}"
        processing_time = datetime.now().strftime('%H:%M:%S')

        s3 = boto3.client('s3')
        sqs = boto3.client('sqs', region_name="us-east-2")  # client is required to interact with
        
        s3_obj = s3.get_object(Bucket=BUCKET_NAME, Key=INPUT_FILE)
        content_str = s3_obj.get("Body").read().decode()
        content_str = content_str.strip()
        input_lines = content_str.split('\n')

        input_lines_filtered = list(filter(lambda x: x.strip() != "", input_lines))

        updated_inputs = []
        
        for inp in input_lines_filtered:
            cleaned_line = str(inp).strip().replace('|', ' ')
            input_line_with_date = f"{cleaned_line}\t{report_datetime}"
            updated_inputs.append(input_line_with_date)
        
        logger.info(f"Total lines in the file is {len(updated_inputs)}")

        max_batch_size = 10 #current maximum allowed
        chunks = [updated_inputs[x:x+max_batch_size] for x in range(0, len(updated_inputs), max_batch_size)]
        for chunk in chunks:
            entries = []
            message_group_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
            for x in chunk:
                splits = str(x).split('\t')
                strike_id = str(splits[0]).strip()
                sku = re.sub('[^0-9a-zA-Z]+', '0', str(splits[1]).strip())
                message_id = f"{strike_id}_{sku}_{report_datetime}"
                # logger.info(f"message_id = {message_id}")
                if INPUT_SQS_URL.endswith('.fifo'):
                    entry = {'Id': message_id, 
                            'MessageBody': str(x),
                            'MessageGroupId':message_group_id}
                else:
                    entry = {'Id': message_id, 
                            'MessageBody': str(x)}

                entries.append(entry)
            response = sqs.send_message_batch(QueueUrl=INPUT_SQS_URL, 
                                                Entries=entries)
            if response.get('Failed') is not None:
                logger.error(f"Error occurred while sending message to queue. {response}")
                raise
        chitti_post(f"{CLIENT} {REPORT_TYPE} crawling initiated.\nNumber of inputs {len(updated_inputs)}")

        # copy the input file to processed
        input_file_name = INPUT_FILE.split('/')[-1].split('.')[0]
        processed_file_name = f"{input_file_name}_{report_datetime}_{processing_time}"

        processed_file = PROCESSED_FILE.replace('<processed_file>',processed_file_name)

        copy_response = s3.copy_object(
            Bucket = BUCKET_NAME,
            CopySource = f"{BUCKET_NAME}/{INPUT_FILE}",
            Key = processed_file,
        )

        if copy_response is not None and copy_response['ResponseMetadata'] is not None and copy_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info(f"Succeccfully processed the input file {INPUT_FILE} and moved as {processed_file}")
                
    except Exception as ex:
        logger.error(ex, exc_info=True)
        logger.error(f"Error getting object {INPUT_FILE} from bucket {BUCKET_NAME}.")
        chitti_post(f"ALERT\n{CLIENT} {REPORT_TYPE} crawling initiation failed.")
        raise ex
    finally: 
        logger.info("End of the program!")
