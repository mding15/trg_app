# -*- coding: utf-8 -*-
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from trg_config import config
from api import email2 as email2_util
from database2 import pg_connection

from api.logging_config import get_logger
logger = get_logger(__name__)

support_emails = config['SUPPORT_EMAILS']
bcc_emails = ['mding@tailriskglobal.com']

NY_TZ = ZoneInfo('America/New_York')


def handle_request(input_data):
    logger.info('===== New Request =====')
    logger.info('route: requestDemo')

    if input_data:
        logger.info(json.dumps(input_data, indent=4))

    save_to_db(input_data)
    send_request_received_email(input_data)
    send_notification(input_data)


def save_to_db(input_data):
    first_name = input_data.get('firstName', '')
    last_name  = input_data.get('lastName', '')
    email      = input_data.get('email', '')
    company    = input_data.get('company', '')
    aum        = input_data.get('aum') or None
    interest   = input_data.get('interest') or None
    message    = input_data.get('message') or None

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO demo_request
                    (first_name, last_name, email, company, aum, interest, message, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'new')
            """, (first_name, last_name, email, company, aum, interest, message))
        conn.commit()

    logger.info(f'demo_request saved: {email}')


def send_request_received_email(input_data):
    subject   = 'Demo Request Received'
    email     = input_data.get('email')
    body_txt  = ''
    body_html = _build_email(input_data, 'demo_request_v2_received_email.html')
    email2_util.send_email([email], subject, body_txt, body_html, cc=[], bcc=bcc_emails)


def send_notification(input_data):
    subject   = 'Demo Request Notification'
    body_txt  = ''
    body_html = _build_email(input_data, 'demo_request_v2_email.html')
    email2_util.send_email(support_emails, subject, body_txt, body_html, cc=[], bcc=[])


def _build_email(input_data, template_name):
    template_file = config['TEMPLATES_DIR'] / template_name
    try:
        with open(template_file, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        raise Exception(f'Email template not found: {template_file}')

    first_name = input_data.get('firstName', '')
    last_name  = input_data.get('lastName', '')
    email      = input_data.get('email', '')
    company    = input_data.get('company', '')
    aum        = input_data.get('aum') or 'Not specified'
    interest   = input_data.get('interest') or 'Not specified'
    message    = input_data.get('message') or '-'
    request_time = datetime.now(NY_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')

    content = content.replace('[firstName]',   first_name)
    content = content.replace('[lastName]',    last_name)
    content = content.replace('[email]',       email)
    content = content.replace('[company]',     company)
    content = content.replace('[aum]',         aum)
    content = content.replace('[interest]',    interest)
    content = content.replace('[message]',     message)
    content = content.replace('[requestTime]', request_time)

    return content
