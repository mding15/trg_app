# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 13:09:00 2025

@author: mgdin
"""

import json
import datetime


from trg_config import config
from api import email2 as email2_util

# logger
from api.logging_config import get_logger
logger = get_logger(__name__)


support_emails = config['SUPPORT_EMAILS']
bcc_emails =['mding@tailriskglobal.com']

def handle_request(input_data):

    logger.info('===== New Request =====')
    logger.info('route: scheduleDemo')

    if input_data:    
        logger.info(json.dumps(input_data, indent=4))
    
    # send email to the prospective client
    send_request_received_email(input_data)
    
    # send notification email to support team
    send_notification(input_data)


def send_request_received_email(input_data):
    subject = "Demo Request Received"
    email = input_data.get('email')    
    body_txt  = ""
    body_html = request_received_email(input_data)
    
    email2_util.send_email([email], subject, body_txt, body_html, cc=[], bcc=bcc_emails)

def send_notification(input_data):
    subject = "Demo Request Notification"
    receiver_emails = support_emails
    body_txt  = ""
    body_html = notification_email(input_data)
    
    email2_util.send_email(receiver_emails, subject, body_txt, body_html, cc=[], bcc=[])

def request_received_email(input_data):
    return read_email_template(input_data, 'demo_request_received_email.html')
    
def notification_email(input_data):
    return read_email_template(input_data, 'demo_request_email.html')

def read_email_template(input_data, template):
    template_file = config['TEMPLATES_DIR'] / template
    try:
        with open(template_file, "r") as file:
            content = file.read()
    except FileNotFoundError:
        raise Exception(f"Error: The template file {template_file} was not found.")
        
    # Replace placeholders with actual values
    fullName    = input_data.get('fullName')
    email       = input_data.get('email')
    phone       = input_data.get('phone')
    companyName = input_data.get('companyName')
    demoDate    = input_data.get('demoDate')
    demoTime    = input_data.get('demoTime')
    requestTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    content = content.replace("[fullName]"    , fullName)
    content = content.replace("[email]"       , email)
    content = content.replace("[phone]"       , phone)
    content = content.replace("[companyName]" , companyName)
    content = content.replace("[demoDate]"    , demoDate)
    content = content.replace("[demoTime]"    , demoTime)
    content = content.replace("[requestTime]" , requestTime)
    
    return content

    

#######################################################################################
def test():
    input_data={
        "fullName": "Michael Ding",
        "email": "mgding@gmail.com",
        "phone": "2018873140",
        "companyName": "TRG",
        "demoDate": "2025-02-20",
        "demoTime": "12:14"
        }
    
    handle_request(input_data)
