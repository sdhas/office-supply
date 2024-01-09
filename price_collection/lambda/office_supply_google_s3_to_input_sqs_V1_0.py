import urllib.parse
import boto3
import logging

OFFICE_SUPPLY_GOOGLE_INPUT_SQS_URL = "https://sqs.us-east-2.amazonaws.com/629901033185/office_supply_google_price_crawling_queue"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
sqs = boto3.client('sqs', region_name="us-east-2")  # client is required to interact with


def lambda_handler(event, context):

    try:
        # Get the object from the event and show its content type
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

        logger.info(f"Reading the object {key} from the bucket {bucket}")

        s3_obj = s3.get_object(Bucket=bucket, Key=key)
        content_str = s3_obj.get("Body").read().decode()
        content_str = content_str.strip()
        input_lines = content_str.split('\n')

        input_lines_filtered = list(filter(lambda x: x.strip() != "", input_lines))
        
        logger.info(f"Total lines in the file is {len(input_lines_filtered)}")

        maxBatchSize = 10 #current maximum allowed
        chunks = [input_lines_filtered[x:x+maxBatchSize] for x in range(0, len(input_lines_filtered), maxBatchSize)]
        for chunk in chunks:
            entries = []
            for x in chunk:
                entry = {'Id': str(x.split('\t')[0]), 
                        'MessageBody': str(x)}
                entries.append(entry)
            response = sqs.send_message_batch(QueueUrl=OFFICE_SUPPLY_GOOGLE_INPUT_SQS_URL, 
                                              Entries=entries)
            if response.get('Failed') is not None:
                logger.error(f"Error occurred while sending message to queue. {response}")
                raise
        
    except Exception as ex:
        logger.error(ex, exc_info=True)
        logger.error(f"Error getting object {key} from bucket {bucket}.")
        raise ex
    finally: 
        logger.info("End of the program!")
