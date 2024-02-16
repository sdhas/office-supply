import boto3
import logging
import json

from decimal import Decimal
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

OFFICE_SUPPLY_GOOGLE_OUTPUT_SQS_URL = "https://sqs.us-east-2.amazonaws.com/629901033185/office_supply_google_price_crawling_output_queue"
TABLE_NAME = "officesupply_google"

dynamodb = boto3.resource('dynamodb', region_name="us-east-2")
table = dynamodb.Table(TABLE_NAME)
s3 = boto3.client('s3')

sqs = boto3.client('sqs', region_name="us-east-2")

def lambda_handler(event, context):
        
    try:

        records = event['Records']
        logger.info(f"Total records in this event is {len(records)}")        

        with table.batch_writer() as batch:

            for record in records:
                output_raw = record['body']

                output_json_data = json.loads(output_raw, parse_float=Decimal)
                batch.put_item(
                    Item = output_json_data
                )
        
    except ClientError as e:
        logger.error(f"Exception occurred {e.response['Error']['Message']}")        
        logger.error(e,exc_info=True)
        # send exception email
        return "ERROR"
    except Exception as ex:
        logger.error(ex,exc_info=True)
        return "ERROR"
    return "OK"