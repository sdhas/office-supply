import boto3
import logging

from datetime import datetime
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

mmddyyyy = datetime.now().strftime('%m%d%Y')

TABLE_NAME = "officesupply_google"
dynamodb = boto3.resource('dynamodb', region_name="us-east-2")
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):  

    global ftp 
        
    report_datetime = f"{mmddyyyy}all"
    try:
        last_evaluated_key = None
        all_items = []
        ok_items = []
        no_sellers_items = []

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
            
            logger.info(f"OK : {len(ok_items)}")
            logger.info(f"NO_SELLERS : {len(no_sellers_items)}")      
                
    except ClientError as e:
        logger.error(f"Exception occurred {e.response['Error']['Message']}")        
        logger.error(e,exc_info=True)
        return "ERROR"
    
    return "OK"