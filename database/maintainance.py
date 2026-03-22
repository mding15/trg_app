# -*- coding: utf-8 -*-
"""
Created on Fri Nov  1 22:01:29 2024

@author: mgdin
"""

import pandas as pd
import datetime
from api import app, db, bcrypt
from database.models import (User, Client, Portfolio_Info, Portfolio_Group, User_Entitilement, Approval, Roles,
                             user_report_mapping_table)

from database.models import pbi_report_url
from database.models import pbi_current_report_url

def drop_table():
    with app.app_context():
        Client.__table__.drop(db.engine)
        db.session.commit()
        
# def drop_all():
#     with app.app_context():
#         db.drop_all()
#         db.session.commit()
        
def create_all():
    with app.app_context():
        db.create_all()

def sql():
    with app.app_context():
        results = pbi_report_url.query.all()
    for x in results:
        print(x)
    
def list_users():
    with app.app_context():
        users = User.query.all()
    for user in users:
        print(user)

def add_pbi_current_report_url():
    data = {
        'report_url': 'https://dashboard.tailriskglobal.com/en/report?workspaceId=3915&reportId=11710',
        'version': 'v1.0',
        'is_active': 1,
        }

    with app.app_context():
        url = pbi_current_report_url.query.filter_by(is_active=data['is_active']).first()
        if url:
            print(f'de-active version: {url.version}')
            url.is_active = 0
        
        curl = pbi_current_report_url(**data)
        db.session.add(curl)
        db.session.commit()
    
    

def add_client(name):
    data = {
        'client_name': 'test client1',
        'address': '123, Abc Street, NY',
        'contact_person': 'John Smith',
        'contact_phone': '212-456-7890'
        }

    with app.app_context():
        client = Client.query.filter_by(client_name=data['client_name']).first()
        if client:
            print(f'client: {name} exisits')
        else:
            client = Client(**data)
            db.session.add(client)
            db.session.commit()

def add_user():
    data = {
        'username': 'mding',
        'email': 'mding@trg.com',
        'password': 'xxxxxx',
        'approval': 1,
        'role': 'superadmin',
        'client_id': 1
        }

    # encript password
    if 'password' in data:
        hashed_password = bcrypt.generate_password_hash(data['password']).decode('utf-8')
        data['password'] = hashed_password
    
    with app.app_context():
        user = User.query.filter_by(username=data['username']).first()
        if user:
            print(f'updating user: {user.username}')
            copy_data(user, data)
        else:
            user = User(**data)
            db.session.add(user)
        db.session.commit()

ROLES=['superadmin','support','developer','admin','user']
def change_user_role(username, role):
    # username = 'mding'
    # role = 'support'
    if role not in ROLES:
        raise Exception(f'Error: role {role} is not allowed!')
    
    with app.app_context():
        user = User.query.filter_by(username=username).first()
        if user:
            user.role = role
            print(f'updating user: {user.username}, role: {user.role}')
            db.session.commit()
        else:
            print(f'Error: can not find user: {username}')
            
def associate_user_to_client():    
    username = 'mding'
    
    # associate with client
    cname = 'test client1'
    with app.app_context():
        client = Client.query.filter_by(client_name=cname).first()
        if client:
            user = User.query.filter_by(username=username).first()
            user.client_id = client.client_id
            db.session.commit()
        else:
            print(f'Error: client {cname} not found!')
    
def test_user():
    username = 'mding'
    cname = 'test client1'
    with app.app_context():
        client = Client.query.filter_by(client_name=cname).first()
        for user in client.users:
            print(user.username)

    with app.app_context():
        user = User.query.filter_by(username=username).first()
        client = user.client
        print(client.client_name)
        

def add_portfolio_group():
    group_name = 'Portfolio_1'
    cname = 'test client1'
    with app.app_context():
        client = Client.query.filter_by(client_name=cname).first()
        pgroup = Portfolio_Group.query.filter_by(group_name=group_name).first()
        if pgroup:
            print(f'group: {group_name} exisits')
        else:
            pgroup = Portfolio_Group(group_name=group_name, client_id=client.client_id)
            db.session.add(pgroup)
            db.session.commit()

    # list all port_groups    
    with app.app_context():
        client = Client.query.filter_by(client_name=cname).first()
        for pgroup in client.port_groups:
                print(pgroup.group_name)
            
    with app.app_context():
        client = db.session.get(Client, 2)
        print(client.client_name)
        for pgroup in client.port_groups:
                print(pgroup.group_name)
    
    with app.app_context():
        pgroup = Portfolio_Group.query.filter_by(client_id=2, group_name='Group_1').first()
        print(pgroup.pgroup_id)
        
def gen_files():
    files = []
    with app.app_context():
        client = db.session.get(Client, 2)
        print(client.client_name)
        for pgroup in client.port_groups:
            print(pgroup.group_name)
            filenames = [p.port_name for p in pgroup.portfolios]
            files.append((pgroup.__dict__, filenames))
    
    for p, names in files:
        print(p['group_name'], names)


def get_portfolio_info(pgroup_id, port_name):
    with app.app_context():
        port = Portfolio_Info.query.filter_by(port_group_id=pgroup_id, port_name=port_name).first()
    return port

def add_portfolio():
    group_name = 'Group_1'
    
    data = {
        'port_name': 'Model_1',
        'filename':  'Model_1.xlsx',
        'created_by': 'mding',
        'status': 'pending'
        }
    
    
    with app.app_context():
        
        pgroup = Portfolio_Group.query.filter_by(group_name=group_name).first()
        
        data['port_group_id'] = pgroup.pgroup_id
        data['update_date'] = datetime.date.today()
        port = get_portfolio_info(data['port_group_id'], data['port_name'])
        
        if port:
            copy_data(port, data)
        else:
            port = Portfolio_Info(**data)
            db.session.add(port)
        db.session.commit()

    # list all portfolios
    with app.app_context():
        pgroup = Portfolio_Group.query.filter_by(group_name=group_name).first()
        for port in pgroup.portfolios:
                print(port.port_name)
            
def add_entitlement():
    data = {
        'user_id': 2,
        'port_group_id': 2,
        'permission': 'download'
        }
    
    with app.app_context():
        entitle = User_Entitilement.query.filter_by(**data).first()
        if entitle:
            print('entitlement exisits')
        else:
            entitle = User_Entitilement(**data)
            db.session.add(entitle)
            db.session.commit()

    with app.app_context():
        user_id = 2
        group_id = 2
        entitlements = User_Entitilement.query.filter_by(user_id=user_id, port_group_id=group_id).all()
        
        for ent in entitlements:
            print(ent.permission)

    with app.app_context():    
        user = User.query.filter_by(username='user2').first()
        entitlements = user.entitlements
        print('len: ', len(entitlements))
        for e in entitlements:
            print(e.permission)
        if len(entitlements) > 0 :
            print(entitlements[0].port_group_id)
        
        
        
#########################################################################################################
def portfolio_to_df(portfolios):
    group_name = 'Portfolio_1'
    with app.app_context():
        pgroup = Portfolio_Group.query.filter_by(group_name=group_name).first()
        portfolios = pgroup.portfolios
    
    # Extract column names from the table
    column_names = [column.name for column in Portfolio_Info.__table__.columns]
    # Extract data as a list of dictionaries
    data = [{col: getattr(port, col) for col in column_names} for port in portfolios]
    # Create DataFrame
    df = pd.DataFrame(data, columns=column_names)
    return df

def df_to_portfolio(df):
    
    obj_list = []
    for i in range(len(df)):
        row = df.iloc[i].to_dict()
        obj = Portfolio_Info(**row)
        obj_list.append(obj)
    
    return obj_list

# copy data to db object
def copy_data(obj, data):
    for key, value in data.items():
        setattr(obj, key, value)

#################################################################################
# Export data
#
from sqlalchemy import inspect
def export_table(ModelClass, filename):
    with app.app_context():
        # Query all objects from the table represented by the ModelClass
        objects = ModelClass.query.all()
    
    # Extract column names from the table
    column_names = [column.name for column in inspect(ModelClass).c]
    # Extract data as a list of dictionaries
    data = [{col: getattr(x, col) for col in column_names} for x in objects]
    # Create DataFrame
    df = pd.DataFrame(data, columns=column_names)
    
    # Save DataFrame to CSV
    df.to_csv(filename, index=False)

    
################################
# Import data
from sqlalchemy.exc import IntegrityError

def import_table(ModelClass, filename):
    with app.app_context():
        # Read the CSV file into a DataFrame
        df = pd.read_csv(filename)

        # convert date columns data type
        for col in df.columns:
            if 'DATE' in col.upper():
                df[col] = pd.to_datetime(df[col])
        
        # Convert each row in the DataFrame to a dictionary and map it to the model class
        for index, row in df.iterrows():
            # Create an instance of the model class with data from the row
            record = ModelClass(**row.to_dict())
            
            # Add the record to the session
            db.session.add(record)
        
        try:
            # Commit the session to save all records to the database
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            raise ValueError(f"Data import failed due to an integrity error at row {index}.")


# Usage example:
def export_import():

    export_table(User,              'test_data\\db\\User.csv')
    export_table(Client,            'test_data\\db\\Client.csv')
    export_table(Portfolio_Info,    'test_data\\db\\Portfolio_Info.csv')
    export_table(Portfolio_Group,   'test_data\\db\\Portfolio_Group.csv')
    export_table(User_Entitilement, 'test_data\\db\\User_Entitilement.csv')
    export_table(Approval,          'test_data\\db\\Approval.csv')
    export_table(Roles,             'test_data\\db\\Roles.csv')
    
    


    import_table(User,              'test_data\\db\\User.csv')
    import_table(Client,            'test_data\\db\\Client.csv')
    import_table(Portfolio_Info,    'test_data\\db\\Portfolio_Info.csv')
    import_table(Portfolio_Group,   'test_data\\db\\Portfolio_Group.csv')
    import_table(User_Entitilement, 'test_data\\db\\User_Entitilement.csv')
    import_table(Approval,          'test_data\\db\\Approval.csv')
    import_table(Roles,             'test_data\\db\\Roles.csv')
    



