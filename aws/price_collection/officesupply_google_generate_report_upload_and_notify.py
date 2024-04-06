import os
import boto3
import smtplib
import logging

from ftplib import FTP
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr

CLIENT = "OfficeSupply"
REPORT_TYPE = "Google"

BASE_FOLDER = os.getenv('BASE_FOLDER')
BUCKET_NAME = os.getenv('INPUT_BUCKET')
TABLE_NAME = os.getenv('TABLE_NAME')
NOTIFICATION_SQS_URL = os.getenv("NOTIFICATION_SQS_URL","https://sqs.us-east-2.amazonaws.com/629901033185/chitti_notifications")

REPORT_FOLDER = f'{BASE_FOLDER}/report/'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

mmddyyyy = datetime.now().strftime('%m%d%Y')
yyyymmdd = datetime.now().strftime('%Y%m%d')

FTP_USER = os.getenv('FTP_USER','test@strikeaprice.com')
FTP_PWD = os.getenv('FTP_PWD','test@123')
FTP_HOST = 'ftp.marlencinemas.com'
FTP_PORT = '21'
FTP_PATH = os.getenv('FTP_PATH','/Reports/')

GMAIL_USER = os.getenv('GMAIL_USER','sanjuthapaulraj@gmail.com')
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD','dummy')

MAIL_TO = os.getenv('MAIL_TO','sathiesh.dhas@gmail.com').split(',')
MAIL_CC = os.getenv('MAIL_CC','sanjutha@marlenindia.com,dhas@strikeaprice.com,prathika@strikeaprice.com').split(',')
MAIL_BCC = os.getenv('MAIL_BCC','sathiesh.dhas@gmail.com').split(',')
ERROR_MAIL_TO = os.getenv('ERROR_MAIL_TO','sanjutha@marlenindia.com,dhas@strikeaprice.com,prathika@strikeaprice.com').split(',')

ftp = None
ok_message = f'Hi Team\n{CLIENT} {REPORT_TYPE} report uploaded successfully. Mail sent to client.\n<msg_details>\nThanks'
nok_message = f'ALERT\nHi Team\n{CLIENT} {REPORT_TYPE} Amazon report failed. Please Check.\nThanks'


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

def chitti_post(message: str):
    try:
        sqs = boto3.client('sqs', region_name="us-east-2")
        response = sqs.send_message(QueueUrl=NOTIFICATION_SQS_URL, MessageBody=message)
        if response.get('Failed') is not None:
            logger.error(f"Error occurred. {response}")

    except Exception as ex:
        logger.error(f"Error {ex}")

def upload_to_ftp(file_name:str):

    with open(file_name, 'rb') as file:
        ftp.storbinary(f'STOR {FTP_PATH}{file.name}', file)

def validate_uploaded_file(file_name):
    logger.info(f"Checking if the file {file_name} is uploaded in FTP")
    if FTP_PATH + file_name in ftp.nlst(FTP_PATH):
        logger.info("File Found in FTP")
        return True
    else:
        logger.info("File NOT Found in FTP")
        return False
    
def send_error_email(error_text: str):
    sent_to_list = ERROR_MAIL_TO
    sent_subject = f"ERROR - {CLIENT} {REPORT_TYPE} - Report"
    logger.info(f"Sending error email with subject {error_text}")
    send_email(sent_to_list, sent_subject, error_text)

    chitti_post(nok_message)

def send_success_email(file_name : str):

    logger.info("Sending mail to client on success report")
    
    sent_to_list = MAIL_TO
    sent_subject = "Report From Strikeaprice.com"
    sent_body = f"Hi,\n\nGood Day to You.\n\nWe have uploaded the daily report in the ftp now.\n\nKindly check and let us know if there's any feedback.\n\nPath : /Reports => {file_name}\n\nRegards,\nSanjutha."

    send_email(sent_to_list, sent_subject, sent_body)

    chitti_post(ok_message)


def send_email(mail_to:list, mail_subject: str, mail_body:str):

    sent_from = GMAIL_USER
    
    try:
        message = "From: %s\r\n" % sent_from\
        + "To: %s\r\n" % ",".join(mail_to)\
        + "CC: %s\r\n" % ",".join(MAIL_CC)\
        + "Subject: %s\r\n" % mail_subject\
        + "\r\n" \
        + mail_body

        to_address = mail_to + MAIL_CC + MAIL_BCC

        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(sent_from, to_address, message)
        server.close()

        logger.info(message)
        logger.info('Email sent!')
    except Exception as exception:
        logger.error("Exception occurred in sending email!")
        logger.error(exception,exc_info=True)

def lambda_handler(event, context):

    global ftp
    global ok_message
    global nok_message

    report_date_db = datetime.now(timezone.utc).strftime('%m%d%Y')

    if 'report_date' in event:
        report_date_db = str(event['report_date'])
    
    report_datetime_db = f"{report_date_db}"
    logging.info(f"Getting the report generated for date {report_date_db}")
    
    try:
        last_evaluated_key = None
        items = [] # Result Array

        while True:
            if last_evaluated_key == None:
                response = table.scan(
                    FilterExpression = Attr('datetime').eq(report_datetime_db) & Attr('status').eq('OK')
                ) # This only runs the first time - provide no ExclusiveStartKey initially
                calculate_and_updat_rcu(response)
            else:
                response = table.scan(
                    FilterExpression = Attr('datetime').eq(report_datetime_db) & Attr('status').eq('OK'),
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
            
            # Report file name
            file_name = f"{yyyymmdd}_SAP_OS.csv"

            os.chdir('/tmp/')           

            report_stat = f"Records count is [Total={len(items)}]"
            logging.info(report_stat)
            ok_message = ok_message.replace('<msg_details>',report_stat)

            with open(file_name, 'w') as out_file:
                # Inventory Number/SKU	Last Seen	Merchant 1	Merchant Price 1	Shipping 1
                out_file.write("Inventory Number/SKU,Last Seen")
                # Setting the range 1 to 26 to print the merchant till 25
                for i in range(1, 26):
                    out_file.write(f",Merchant {i},Merchant Price {i},Shipping {i}")
                out_file.write("\n")

                for item in items:
                    # print(f"item > {item}")
                    # print(f"Item type is {type(item)}")
                    # print(f"Merchant type is {type(item.get('merchants'))}")
                    ouput_str = item.get('output')
                    if len(ouput_str):
                        modified_ouput_str = ouput_str.replace("|",",")
                        out_file.write(f"{modified_ouput_str}\n")   
            # Closing file                   
            out_file.close()

            s3_path = REPORT_FOLDER + file_name
            s3.upload_file(file_name, BUCKET_NAME, s3_path)
            
            # creating FTP connection
            ftp = FTP(FTP_HOST, FTP_USER, FTP_PWD)

            upload_to_ftp(file_name)

            if validate_uploaded_file(file_name):
                send_success_email(file_name)
            else:
                send_error_email("File not uploaded in FTP")
            
            ftp.close()

            # remove the file from /tmp/
            os.remove(file_name)                        
        else:
            logger.info("No records found to generate report.")
            chitti_post(f"{nok_message}\n- No records found to generate report.")
            return "NOT_FOUND"
                
    except ClientError as e:
        logger.error(f"Exception occurred {e.response['Error']['Message']}")        
        logger.error(e,exc_info=True)
        chitti_post(f"{nok_message}\n- Something went wrong.")
        return "ERROR"
    finally:
        logger.info(f"Consumed RCU is {rcu}")
    
    return "OK"