# -*- coding: utf-8 -*-
"""
Created on Sun Oct 27 21:05:57 2024

@author: mgdin

create_account api implementation:
    - insert user info into database
    - create new client
    - send email
    - update password in database
    
"""
import re
import random
import string
from trg_config import config
from api import app
#from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
from itsdangerous import URLSafeTimedSerializer as Serializer
from api import email2 as email2_util

from api import db, bcrypt
from database.models import User, Client
from database.model_aux import add_client, add_client_report_url

from api.logging_config import get_logger
logger = get_logger(__name__)


def create_account(data):

    firstName       = data.get('firstName')
    lastName        = data.get('lastName')
    email           = data.get('email')
    password        = data.get('password')
    companyName     = data.get('companyName')
    phone           = data.get('phone', '')
    address         = data.get('address', '')
    aum             = data.get('aum', '')
    primaryInterest = data.get('primaryInterest', '')

    print(f"create_account: {firstName} {lastName} {email} {companyName}")

    # check email format and if exists in db
    check_email(email)

    # check if email exists in db
    user = get_user_by_email(email)
    if user:
        raise Exception(f'Your email has been already registered in our database: {email}')

    # password
    if not password or password=='':
        password = generate_random_password(15)
    hashed_password = bcrypt.generate_password_hash(password).decode('utf-8')

    # create client
    client_id, exists = add_client({'client_name': companyName})
    if exists == False:
        add_client_report_url(client_id)

    # set additional client fields
    client = db.session.get(Client, client_id)
    client.address          = address
    client.aum              = aum
    client.primary_interest = primaryInterest
    db.session.commit()

    user = User(username=email, email=email, approval=0, role='user', password=hashed_password, client_id=client_id,
                firstname=firstName, lastname=lastName, phone=phone, webdashboard_login=email)

    db.session.add(user)
    db.session.commit()
    print(f'new user {user.user_id} has been created')

    email_new_user_notification(user)
    
def approve_user(user_id):
    user = User.query.filter_by(user_id=user_id).first()
    if user and user.approval != 1:
        user.approval = 1
        db.session.commit()    
        send_activation_email(user)

def forget_password(data):
    email = data.get('email')
    
    check_email(email)
    
    # check if email is in the database    
    user = get_user_by_email(email)
    
    if not user:
        raise Exception(f'We can not find your email in our database: {email}')

    send_reset_password_email(user)
        
def change_password(username, data):
    
    old_password = data.get('oldPassword')
    new_password = data.get('newPassword')  
    
    # verify the old password
    user = User.query.filter_by(username=username).first()
    if user:
        if bcrypt.check_password_hash(user.password, old_password) == False:
            raise Exception('old password is incorrect!')
    else:
        raise Exception('can not find {username} in the database!')
        
    # update new password
    hashed_password = bcrypt.generate_password_hash(new_password).decode('utf-8')
    user.password = hashed_password
    db.session.commit()
    print(f'password for {user.email} has been changed!')


######################################################################################################
# auxilary
def check_email(email, new_user=True):
    if email is None or email=='':
        raise Exception('email is empty')

    # Define the regular expression pattern for a valid email address
    email_pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    
    # Use re.match to check if the email matches the pattern
    if not re.match(email_pattern, email):
        raise Exception(f'invalid email: {email}')

def get_user_by_email(email):
    
    user = User.query.filter_by(email=email).first()
    
    return user

def generate_random_password(length=10):
    characters = string.ascii_letters + string.digits + string.punctuation
    password = ''.join(random.choice(characters) for i in range(length))
    return password    


def get_reset_token(user):
        s = Serializer(app.config['SECRET_KEY'])
        return s.dumps({'user_id': user.user_id}, salt='password-reset')
    
def verify_reset_token(token, expires_sec=1800):
    s = Serializer(app.config['SECRET_KEY'])
    try:
        data = s.loads(token, salt='password-reset', max_age=expires_sec)
        user_id = data['user_id']
    except:
        return None
        
    return db.session.get(User, user_id)

def reset_password(token, password):
    user = verify_reset_token(token)
    if user is None:
        raise Exception('This is an invalid or expired token')

    hashed_password = bcrypt.generate_password_hash(password).decode('utf-8')
    user.password = hashed_password
    
    if not user.activation_completed:
        email2_util.send_activation_completed_email(user)
        user.activation_completed = True
    
    db.session.commit()
    print(f'password for {user.email} has been reset!')

    
def test_reset_token():
    email = 'test1@trg.com'
    user = User.query.filter_by(email=email).first()
    token = get_reset_token(user)
    user = verify_reset_token(token)
    
    password = 'xxxx'
    reset_password(token, password)
    
def send_activation_email(user):
    token = get_reset_token(user)
    activation_link = reset_password_link(token)
    username = f'{user.firstname} {user.lastname}'
    receiver_emails = [user.email]
    
    email2_util.send_activation_email(username, receiver_emails, activation_link)
    
                
 
def send_reset_password_email(user):
    token = get_reset_token(user)
    activation_link = reset_password_link(token)
    username = f'{user.firstname} {user.lastname}'
    receiver_emails = [user.email]

    email2_util.send_reset_password_email(username, receiver_emails, activation_link)

def reset_password_link(token):
    host='https://tailriskglobal.com'
    link = f'{host}/resetPassword/{token}'
    return link
    

######################################
def email_new_user_notification(user):
    client = user.client

    subject = "New User Notification"
    receiver_emails = config['SUPPORT_EMAILS']

    # read_email_template
    template_file = config['TEMPLATES_DIR'] / 'new_user_email.html'
    try:
        with open(template_file, "r") as file:
            content = file.read()
    except FileNotFoundError:
        raise Exception(f"Error: The template file {template_file} was not found.")

    content = content.replace("[user_id]"          , f'{user.user_id}')
    content = content.replace("[firstname]"        , user.firstname or '')
    content = content.replace("[lastname]"         , user.lastname or '')
    content = content.replace("[email]"            , user.email)
    content = content.replace("[phone]"            , user.phone or '')
    content = content.replace("[company_name]"     , client.client_name)
    content = content.replace("[address]"          , client.address or '')
    content = content.replace("[aum]"              , client.aum or '')
    content = content.replace("[primary_interest]" , client.primary_interest or '')

    email2_util.send_email(receiver_emails, subject, "", content, cc=[], bcc=[])
    
    

#############################################################################################    
def test():
    firstName = 'Michael'
    lastName  = 'Ding'
    email     = 'mgding@yahoo.com'
    companyName = 'TRG'
    
    data = {
        'firstName': firstName,
        'lastName' : lastName,
        'email': email,
        'companyName': companyName,
    }

    try:
        create_account(data)
    except Exception as err:
        print(err)


    # approve user
    user_id = 1053
    approve_user(user_id)
    
    # test send new user email
    user = get_user_by_email('test1@trg.com')
    email_new_user_notification(user)

    # Test cases
    print(check_email("test@example.com"))  # Should have no exception
    print(check_email("invalid-email"))     # Should have exception    

    # Test random password        
    generate_random_password()
    
    
    # reset_password email
    email='mgding@gmail.com'
    user = get_user_by_email(email)
    send_reset_password_email(user)

def test_token():
    user = db.session.get(User, 1)
    token = get_reset_token(user)
    
    user1 = verify_reset_token(token, expires_sec=1800)
    print(user1)
