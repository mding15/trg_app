# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 15:02:59 2024

@author: mgdin

"""
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


###########################################################################
# TEST sender from gmail
def test_gemail():
    
    sender_email = "tailriskglobal@gmail.com"
    sender_password = os.environ['EMAIL_PASSWORD']
    smtp_server = 'smtp.gmail.com'
    port = 587
    
    receiver_emails = ['mding@tailriskglobal.com']

    # Set up the MIME
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ", ".join(receiver_emails) 
    message["Subject"] = 'Test Email'
    
    # Attach the body with the msg instance
    body = 'This is a test email'
    message.attach(MIMEText(body, "plain"))

    try:
        # Create SMTP session for sending the mail
        server = smtplib.SMTP(smtp_server, port)  # Use Gmail's SMTP server
        server.starttls()  # Enable security
        server.login(sender_email, sender_password)  # Login with your email and password
        text = message.as_string()
        server.sendmail(sender_email, receiver_emails, text)
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email. Error: {e}")

#################################################################
#
# Azure Communication Services
#
# 
# pip install azure-communication-email
# https://portal.azure.com/
# https://learn.microsoft.com/en-us/python/api/overview/azure/communication-email-readme?view=azure-python
#
# endpoint="https://trg-email.unitedstates.communication.azure.com/"

from azure.communication.email import EmailClient
key = os.environ['EMAIL_KEY']
connection_string = f"endpoint=https://trg-email.unitedstates.communication.azure.com/;accessKey={key}"

def test_azure_email():

    try:
        connection_string = f"endpoint=https://trg-email.unitedstates.communication.azure.com/;accessKey={key}"
        client = EmailClient.from_connection_string(connection_string)

        message = {
            "senderAddress": "DoNotReply@tailriskglobal.com",
            "recipients": {
                "to": [{"address": "pcontreras@tailriskglobal.com"},
                       {"address": "azhan@tailriskglobal.com"}]
            },
            "content": {
                "subject": "Test Email",
                "plainText": "Hello world via email.",
                "html": """
				<html>
					<body>
						<h1>Hello world via email.</h1>
					</body>
				</html>"""
            },
            
        }

        poller = client.begin_send(message)
        result = poller.result()
        print("Message sent: ", result['status'])

    except Exception as ex:
        print(ex)


