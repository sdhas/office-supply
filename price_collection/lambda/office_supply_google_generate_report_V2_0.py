import os
import boto3
import smtplib
import logging

from ftplib import FTP
from datetime import datetime
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

mmddyyyy = datetime.now().strftime('%m%d%Y')
yyyymmdd = datetime.now().strftime('%Y%m%d')

BUCKET_NAME = "strikeaprice-ohio"
S3_PATH_PREFIX = "price_crawling/office_supply/output/"

# TEST
FTP_HOST = 'ftp.marlencinemas.com'
FTP_PORT = '21'
FTP_USER = 'test@strikeaprice.com'
FTP_PWD = 'test@123'
FTP_PATH = '/Reports/'
CONFIRM_MAIL_RECEPIENTS = ['sathiesh.dhas@gmail.com']

# PROD
# FTP_HOST = 'ftp.marlencinemas.com'
# FTP_PORT = '21'
# FTP_USER = 'officesupply@marlencinemas.com'
# FTP_PWD = 'OfficeSupply@123'
# FTP_PATH = '/Reports/'
# CONFIRM_MAIL_RECEPIENTS = ['blang@officesupply.com']

ftp = None

TABLE_NAME = "officesupply_google"
dynamodb = boto3.resource('dynamodb', region_name="us-east-2")
table = dynamodb.Table(TABLE_NAME)
s3 = boto3.client('s3')

GMAIL_USER = 'sanjuthapaulraj@gmail.com'
GMAIL_APP_PASSWORD = 'wsyqguekuupctlgb'

MAIL_CC = ['sanjutha@marlenindia.com','dhas@strikeaprice.com','prathika@strikeaprice.com']
MAIL_BCC = ['sathiesh.dhas@gmail.com']
ERROR_MAIL_RECEPIENTS = ['sanjutha@marlenindia.com','dhas@strikeaprice.com','prathika@strikeaprice.com']

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
    sent_to_list = ERROR_MAIL_RECEPIENTS
    sent_subject = "ERROR - Office Supply - Report"
    logger.info(f"Sending error email with subject {error_text}")
    send_email(sent_to_list, sent_subject, error_text)

def send_success_email(file_name : str):

    logger.info("Sending mail to client on success report")
    
    sent_to_list = CONFIRM_MAIL_RECEPIENTS
    sent_subject = "Report From Strikeaprice.com"
    sent_body = f"Hi,\n\nGood Day to You.\n\nWe have uploaded the daily report in the ftp now.\n\nKindly check and let us know if there's any feedback.\n\nPath : /Reports => {file_name}\n\nRegards,\nSanjutha."

    send_email(sent_to_list, sent_subject, sent_body)


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
        
    report_datetime = f"{mmddyyyy}all"
    try:
        last_evaluated_key = None
        items = [] # Result Array

        while True:
            if last_evaluated_key == None:
                response = table.scan(
                    FilterExpression = Attr('datetime').eq(report_datetime) & Attr('status').eq('OK')
                ) # This only runs the first time - provide no ExclusiveStartKey initially
            else:
                response = table.scan(
                    FilterExpression = Attr('datetime').eq(report_datetime) & Attr('status').eq('OK'),
                    ExclusiveStartKey=last_evaluated_key # In subsequent calls, provide the ExclusiveStartKey
                )

            items.extend(response['Items']) # Appending to our resultset list
            
            # Set our lastEvlauatedKey to the value for next operation,
            # else, there's no more results and we can exit
            if 'LastEvaluatedKey' in response:
                last_evaluated_key = response['LastEvaluatedKey']
            else:
                break

        if len(items):
            logger.info(f"Total number of records : {len(items)}")
            
            file_name = f"{yyyymmdd}_SAP_OS.csv"
            os.chdir('/tmp/')

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
            
            
            s3_path = S3_PATH_PREFIX + file_name
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
            send_error_email("No records found to generate report.")
            return "NOT_FOUND"
                
    except ClientError as e:
        logger.error(f"Exception occurred {e.response['Error']['Message']}")        
        logger.error(e,exc_info=True)
        # Sending error email
        send_error_email(e.response['Error']['Message'])
        return "ERROR"
    
    return "OK"