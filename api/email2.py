# -*- coding: utf-8 -*-
"""
Created on Tue Dec  3 16:56:22 2024

@author: mgdin
"""
import os
import threading
from flask_mail import Mail, Message
from api import app
from trg_config import config
from datetime import datetime


# turn off Azure logging
import logging

# Suppress Azure SDK logs
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

# Suppress urllib3 logs
logging.getLogger("urllib3").setLevel(logging.WARNING)


# Azure email
from azure.communication.email import EmailClient
key = os.environ['EMAIL_KEY']
connection_string = f"endpoint=https://trg-email.unitedstates.communication.azure.com/;accessKey={key}"


# Email server configuration
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = 'tailriskglobal@gmail.com'
app.config['MAIL_PASSWORD'] = os.environ["EMAIL_PASSWORD"]


# Initialize Flask-Mail
mail = Mail(app)

support_emails = config['SUPPORT_EMAILS']


################################################################################
# services:
def send_activation_email(username, receiver_emails, activation_link):
    
    subject = "TRG Account Activation"    
    body_txt = read_activation_txt(username, activation_link)
    body_html = read_activation_html(username, activation_link)
    send_email(receiver_emails, subject, body_txt, body_html, cc=[], bcc=support_emails)
    
def send_reset_password_email(username, receiver_emails, activation_link):

    subject = "TRG Reset Passowrd"    
    body_txt  = read_reset_password_txt (username, activation_link)
    body_html = read_reset_password_html(username, activation_link)

    send_email(receiver_emails, subject, body_txt, body_html, cc=[], bcc=support_emails)
    
    
def send_activation_completed_email(user):
    username = f'{user.firstname} {user.lastname}'
    receiver_emails = support_emails 
    subject = "New User Activated"

    body_txt = read_activation_completed_txt(username)
    body_html = read_activation_completed_html(username)

    send_email(receiver_emails, subject, body_txt, body_html, cc=[], bcc=[])


def send_portfolio_status_notification(port_id, port_name, status, error_message, user_id=None, user_name=None, receiver_emails=None):
    """Send portfolio status notification email, Added by AI"""
    
    if receiver_emails is None:
        receiver_emails = support_emails
    
    subject = f"Portfolio {port_id} Status Changed to {status}"
    
    body_txt = read_portfolio_status_txt(port_id, port_name, status, error_message, user_id, user_name)
    body_html = read_portfolio_status_html(port_id, port_name, status, error_message, user_id, user_name)
    
    send_email(receiver_emails, subject, body_txt, body_html, cc=[], bcc=[])
    
    
################################################################################

def send_email(receiver_emails, subject, body_txt, body_html, cc=[], bcc=[]):
    thread = threading.Thread(target=send_outlook, args=(receiver_emails, subject, body_txt, body_html, cc, bcc))
    thread.start()

    # Optional: Wait for the thread to finish (if needed)
    # thread.join()  # Uncomment if you want to wait for completion


def send_outlook(receiver_emails, subject, body_txt, body_html, cc=[], bcc=[]):

    # subject = 'Activation Email'
    # receiver_emails = ['mgding@gmail.com']
    # body_txt = read_activation_txt('Michael', 'https://tailriskglobal.com/activation')
    # body_html = read_activation_html('Michael', 'https://tailriskglobal.com/activation')
    # bcc = support_emails
    # cc = []
    
    try:
        connection_string = f"endpoint=https://trg-email.unitedstates.communication.azure.com/;accessKey={key}"
        client = EmailClient.from_connection_string(connection_string)

        message = {
            "senderAddress": "DoNotReply@tailriskglobal.com",
            "recipients": {
                "to":  [{'address': x} for x in  receiver_emails],
                "cc":  [{'address': x} for x in   cc],
                "bcc": [{'address': x} for x in  bcc],
            },
            
            "content": {
                "subject": subject,
                "plainText": body_txt,
                "html": body_html
            },
            
        }

        poller = client.begin_send(message)
        result = poller.result()
        print(f"Sent email to {receiver_emails}, status:", result['status'])

    except Exception as ex:
        print(ex)




#
# sender has gmail account
#

def send_gmail(receiver_emails, subject, body, images=[], cc=[], bcc=[]):
    
    sender_email = "tailriskglobal@gmail.com"
    # receiver_emails = ['mgding@gmail.com']
    # subject = 'Activation Email'
    # body = read_activation_html('Michael', 'https://tailriskglobal.com/activation')
    # images = ['logo', 'logo2', 'welcome']
    # cc = []
    # bcc = ['mding@tailriskglobal.com']
    
    # Create the email message
    msg = Message(
        subject=subject,
        sender=sender_email,
        recipients=receiver_emails,
        cc=cc,
        bcc=bcc
    )
    
    msg.html = body
    
    # Attach an image
    if images:
        for img in images:
            attach_img(msg, img)

    # Send the email
    mail.send(msg)
    print ("Email sent successfully!")
    

def attach_img(msg, name):
    filename = f'{name}.svg'
    # Attach an image
    msg.attach(
        filename,  # Filename
        "image/svg+xml",  # MIME type
        load_img(filename),  # Image content
        headers={"Content-ID": f"<{name}>"}  # Content-ID for embedding
    )

######################################################################################
# read email content from templates

def read_activation(template_file, username, activation_link):
    try:
        with open(template_file, "r") as file:
            content = file.read()
    except FileNotFoundError:
        raise Exception(f"Error: The template file {template_file} was not found.")
        
    # Replace placeholders with actual values
    content = content.replace("[User]", username)
    content = content.replace("[activation-link]", activation_link)
    
    return content    
    
def read_activation_txt(username, activation_link):
    template_file = config['TEMPLATES_DIR'] / 'activation_email.txt'
    return read_activation(template_file, username, activation_link)
    
def read_activation_html(username, activation_link):
    template_file = config['TEMPLATES_DIR'] / 'activation_email.html'
    return read_activation(template_file, username, activation_link)

def read_activation_completed(template_file, username):
    try:
        with open(template_file, "r") as file:
            content = file.read()
    except FileNotFoundError:
        raise Exception(f"Error: The template file {template_file} was not found.")

    # Replace only [User]
    content = content.replace("[User]", username)
    return content

def read_activation_completed_txt(username):
    template_file = config['TEMPLATES_DIR'] / 'activation_completed_email.txt'
    return read_activation_completed(template_file, username)

def read_activation_completed_html(username):
    template_file = config['TEMPLATES_DIR'] / 'activation_completed_email.html'
    return read_activation_completed(template_file, username)

def read_reset_password_template(username, activation_link, template_file):
    # template_file = config['TEMPLATES_DIR'] / 'reset_password_email.html'
    
    try:
        with open(template_file, "r") as file:
            content = file.read()
    except FileNotFoundError:
        raise Exception(f"Error: The HTML template file {template_file} was not found.")
        
    # Replace placeholders with actual values
    content = content.replace("[User]", username)
    content = content.replace("[activation-link]", activation_link)
    
    return content    
    
def read_reset_password_txt(username, activation_link):
    template_file = config['TEMPLATES_DIR'] / 'reset_password_email.txt'
    return read_reset_password_template(username, activation_link, template_file)
    
def read_reset_password_html(username, activation_link):
    template_file = config['TEMPLATES_DIR'] / 'reset_password_email.html'
    return read_reset_password_template(username, activation_link, template_file)

def read_portfolio_status_template(port_id, port_name, status, error_message, user_id, user_name, template_file):
    """Added by AI, read portfolio status notification template"""
    
    try:
        with open(template_file, "r") as file:
            content = file.read()
    except FileNotFoundError:
        raise Exception(f"Error: The template file {template_file} was not found.")
        
    # Replace placeholders with actual values
    content = content.replace("[PortfolioID]", str(port_id))
    content = content.replace("[PortfolioName]", str(port_name) if port_name else "Unknown")
    content = content.replace("[Status]", str(status))
    content = content.replace("[ErrorMessage]", str(error_message))
    content = content.replace("[Timestamp]", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    if user_id:
        content = content.replace("[UserID]", str(user_id))
    if user_name:
        content = content.replace("[UserName]", user_name)
    
    return content

def read_portfolio_status_txt(port_id, port_name, status, error_message, user_id, user_name):
    """Added by AI, read portfolio status notification template"""
    template_file = config['TEMPLATES_DIR'] / 'portfolio_status_notification.txt'
    return read_portfolio_status_template(port_id, port_name, status, error_message, user_id, user_name, template_file)
    
def read_portfolio_status_html(port_id, port_name, status, error_message, user_id, user_name):
    """Added by AI, read portfolio status notification template"""
    template_file = config['TEMPLATES_DIR'] / 'portfolio_status_notification.html'
    return read_portfolio_status_template(port_id, port_name, status, error_message, user_id, user_name, template_file)

#############################################################################################

# *** deprecated ***
def activation_template(username, activation_link):
    
    template_file = config['TEMPLATES_DIR'] / 'activation_email.html'
    
    try:
        with open(template_file, "r") as file:
            html_content = file.read()
    except FileNotFoundError:
        raise Exception(f"Error: The HTML template file {template_file} was not found.")
        
    # Replace placeholders with actual values
    html_content = html_content.replace("[User]", username)
    html_content = html_content.replace("[activation-link]", activation_link)
    
    return html_content    

# *** deprecated ***
def reset_password_template(username, activation_link):
    template_file = config['TEMPLATES_DIR'] / 'reset_password_email.html'
    
    try:
        with open(template_file, "r") as file:
            html_content = file.read()
    except FileNotFoundError:
        raise Exception(f"Error: The HTML template file {template_file} was not found.")
        
    # Replace placeholders with actual values
    html_content = html_content.replace("[User]", username)
    html_content = html_content.replace("[activation-link]", activation_link)
    
    return html_content    
    

# filename='logo.svg'
def load_img(filename):
    file_path = config['TEMPLATES_DIR'] / filename
            
    try:
        with open(file_path, "rb") as img_file:
            img = img_file.read()
    except FileNotFoundError:
        raise Exception("Error: The image file 'welcome_image.jpg' was not found.")

    return img    
    
########################################
# test

def test():
    receiver_emails = ["mgding@gmail.com"]
    username = 'Michael Ding'
    activation_link = 'https://tailriskglobal.com/test'
    
    # test activation email
    send_activation_email(username, receiver_emails, activation_link)
    
    # test reset_password email
    send_reset_password_email(username, receiver_emails, activation_link)
    
    # test portfolio status notification
    send_portfolio_status_notification(
        port_id=1234, 
        port_name="Test Portfolio", 
        status="pending", 
        error_message="All positions are unknown", 
        user_id="test_user_123",
        user_name="Test User",
        receiver_emails=receiver_emails
    )
    
    

