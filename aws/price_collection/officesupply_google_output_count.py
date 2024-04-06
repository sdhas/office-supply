import os
import boto3
import logging

from datetime import datetime, timezone
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.getenv('TABLE_NAME')
dynamodb = boto3.resource('dynamodb', region_name="us-east-2")
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):  

    report_date = datetime.now(timezone.utc).strftime('%m%d%Y')

    if 'report_date' in event:
        report_date = str(event['report_date'])

    logging.info(f"Getting the count of output for date {report_date}")
    report_datetime = f"{report_date}"
    try:
        last_evaluated_key = None
        all_items = []
        ok_items = []
        no_sellers_items = []
        not_found_items = []
        error_items = []

        while True:
            if last_evaluated_key == None:
                response = table.scan(
                    FilterExpression = Attr('datetime').eq(report_datetime)
                ) # This only runs the first time - provide no ExclusiveStartKey initially
            else:
                response = table.scan(
                    FilterExpression = Attr('datetime').eq(report_datetime),
                    ExclusiveStartKey=last_evaluated_key # In subsequent calls, provide the ExclusiveStartKey
                )

            all_items.extend(response['Items']) # Appending to our resultset list
            
            # Set our lastEvlauatedKey to the value for next operation,
            # else, there's no more results and we can exit
            if 'LastEvaluatedKey' in response:
                last_evaluated_key = response['LastEvaluatedKey']
            else:
                break

        logger.info(f"Total number of output records : {len(all_items)}")

        if len(all_items):
            for item in all_items:
                if item.get('status') == "OK":
                    ok_items.append(item)
                elif item.get('status') == "NO_SELLERS":
                    no_sellers_items.append(item)
                elif item.get('status') == "NOT_FOUND":
                    not_found_items.append(item)
                elif item.get('status') == "ERROR":
                    error_items.append(item)
            
            logger.info(f"OK : {len(ok_items)}")
            logger.info(f"NO_SELLERS : {len(no_sellers_items)}") 
            logger.info(f"NOT_FOUND : {len(not_found_items)}") 
            logger.info(f"ERROR : {len(error_items)}") 
                
    except ClientError as e:
        logger.error(f"Exception occurred {e.response['Error']['Message']}")        
        logger.error(e,exc_info=True)
        return "ERROR"
    
    return "OK"