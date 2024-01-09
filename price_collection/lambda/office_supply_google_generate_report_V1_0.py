import os
import json
import boto3
import smtplib
import logging
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = "office_supply_google_price_crawling_db"
dynamodb = boto3.resource('dynamodb', region_name="us-east-2")
table = dynamodb.Table(TABLE_NAME)
s3 = boto3.client('s3')

def send_email(file_name:str):
    # Change the items with: ######Change Me#######
    gmail_user = 'sanjuthapaulraj@gmail.com'
    gmail_app_password = "wsyqguekuupctlgb"
    sent_from = gmail_user
    sent_to = ['sathiesh.dhas@gmail.com', 'prathika@strikeaprice.com']
    sent_subject = "AWS - TEST - StrikeAPrice report uploaded"
    sent_body = f"Report file {file_name} uploaded!"

    email_text = """\
From: %s
To: %s
Subject: %s
%s
""" % (sent_from, ", ".join(sent_to), sent_subject, sent_body)

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(gmail_user, gmail_app_password)
        server.sendmail(sent_from, sent_to, email_text.encode("utf-8"))
        server.close()
        logger.info(email_text)
        logger.info('Email sent!')
    except Exception as exception:
        logger.error("Exception occurred in sending email!")
        logger.error(exception,exc_info=True)

def lambda_handler(event, context):   
        
    try:
        last_evaluated_key = None
        items = [] # Result Array

        while True:
            if last_evaluated_key == None:
                response = table.scan(
                    FilterExpression = Attr('client').eq('officesupply') & Attr('reportDate').eq('01.06.2023') & Attr('reportTime').eq('all')
                ) # This only runs the first time - provide no ExclusiveStartKey initially
            else:
                response = table.scan(
                    FilterExpression = Attr('client').eq('officesupply') & Attr('reportDate').eq('01.06.2023') & Attr('reportTime').eq('all'),
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
            
            file_name = "officesupply_report_01.06.2024.csv"                 
            os.chdir('/tmp')

            with open(file_name, 'w') as out_file:
                # Inventory Number/SKU	Last Seen	Merchant 1	Merchant Price 1	Shipping 1
                out_file.write("Inventory Number/SKU\tLast Seen")
                # Setting the range 1 to 26 to print the merchant till 25
                for i in range(1, 26):
                    out_file.write(f"\tMerchant {i}\tMerchant Price {i}\tShipping {i}")
                out_file.write("\n")

                for item in items:
                    # print(f"item > {item}")
                    # print(f"Item type is {type(item)}")
                    # print(f"Merchant type is {type(item.get('merchants'))}")
                    sellers = item.get('merchants')
                    if len(sellers):
                        sellers_str_list = []
                        sellers_str = ""
                        for seller in sellers:
                            sellers_str_list.append(f"{seller.get('name')},{str(seller.get('price'))},{str(seller.get('shipping'))}")
                        if len(sellers_str_list):
                            sellers_str = ",".join(sellers_str_list)
                        out_file.write(f"{item.get('sku')},{item.get('reportDate')},{sellers_str}\n")
            
            bucket_name = "strikeaprice-ohio"
            s3_path = "price_crawling/office_supply/output/" + file_name
            s3.upload_file(file_name, bucket_name, s3_path)
            send_email(file_name)
        else:
            logger.info("No records found to generate report.")
            return "NOT_FOUND"
        
    except ClientError as e:
        logger.error(f"Exception occurred {e.response['Error']['Message']}")        
        logger.error(e,exc_info=True)
        # send exception email
        return "ERROR"
    return "OK"