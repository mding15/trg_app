# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 09:42:58 2024

@author: mgdin
"""
from datetime import datetime, date
from database import db, models, db_aux
from database.models import Portfolio_Info, Client

################################################################################################
# methods

# copy data to db object
def copy_data(obj, data):
    for key, value in data.items():
        setattr(obj, key, value)
        
# data = {
#     'client_name': 'test client1',
#     'address': '123, Abc Street, NY',
#     'contact_person': 'John Smith',
#     'contact_phone': '212-456-7890'
#     }
def add_client(data):
    
    if 'client_name' not in data:
        raise Exception('add_client: client_name is required')

    # assign unknown value
    for key in ['address', 'contact_person', 'contact_phone']:
        data[key] = 'unknown'
    
    exists = False
    client = None

    if data['client_name']:
        client = models.Client.query.filter_by(client_name=data['client_name']).first()
    
    if client: # update 
        # copy_data(client, data)
        exists = True
    else: # insert
        client_name = data['client_name']
        if not client_name: # assign "Unknown Company 1"
            client_name = get_unknown_company_name()
            data['client_name'] = client_name

        print(f'Create new client: {client_name}')
        client = models.Client(**data)
        db.session.add(client)
    db.session.commit()
    
    return client.client_id, exists

def get_unknown_company_name():
    unknown_clients = db.session.query(Client).filter(Client.client_name.like('Unkonwn Company%')).all()
    
    unknown_cnames = [row.client_name for row in unknown_clients]

    version = 1
    while f'Unkonwn Company {version}' in unknown_cnames:
        version += 1
    client_name = f'Unkonwn Company {version}'        
    return client_name 
    
def get_client_by_id(client_id):
    client = models.Client.query.filter_by(client_id=client_id).first()
    return client

def delete_client(client):
    if client:
        db.session.delete(client)
        db.session.commit()
    
def get_current_report_url():
    url = models.pbi_current_report_url.query.filter_by(is_active=1).first()
    #print(url.report_url)
    return url
    
# client_id=1018
def add_client_report_url(client_id):
    url = models.pbi_report_url.query.filter_by(client_id=client_id, is_active=1).first()
    if url is None:
        print(f'add new client url for client: {client_id}')
        current_url = get_current_report_url().report_url
        client_url = models.pbi_report_url(client_id=client_id, report_url=current_url, is_active=1)
        db.session.add(client_url)
        db.session.commit()
    
# group_name = 'Portfolio_1'
# client_id = 2
def add_portfolio_group(user, group_name):
    client = user.client
    pgroup = models.Portfolio_Group.query.filter_by(client_id=client.client_id, group_name=group_name).first()
    if pgroup:
        raise Exception(f'group: {group_name} exisits, please use a different name')

    pgroup = models.Portfolio_Group(group_name=group_name, client_id=client.client_id)
    db.session.add(pgroup)
    db.session.commit()

    # add permissions
    for permission in ['view', 'upload', 'download']:
        save_entitlements(user.user_id, pgroup.pgroup_id, permission)
    
    # add default limits
    db_aux.add_limit_var(pgroup.pgroup_id)
    db_aux.add_limit_concentration(pgroup.pgroup_id)
    
    return pgroup

def test_add_portfolio_group():
    user = get_user('test1@trg.com')
    group_name = 'test port group'
    add_portfolio_group(user, group_name)
    
# return the first group
def get_port_group(user):
    
    client = user.client
    if not client.port_groups:
        # create a group
        pgroup = add_portfolio_group(user, 'Group 1')        
    else:    
        # the first group
        pgroup = client.port_groups[0]

    return pgroup

def get_client_from_pgroup(pgroup_id):
    pgroup = get_pgroup_by_id(pgroup_id)
    client_id = pgroup.client_id

    client = get_client_by_id(client_id)
    return client

# data = {
#     'port_name': 'Model_1',
#     'filename':  'Model_1.xlsx',
#     'created_by': 'mding',
#     'status': 'pending',
#     'port_group_id': 1
#     }


    
def add_portfolio(data, replace=False):

    data['update_date'] = date.today()
    group_id  = data['port_group_id']
    port_name = data['port_name']
    is_batch = data['is_batch']

    if is_batch:
        return insert_portfolio_info(data)
    else:
        if replace:
            return add_portfolio_replace(data, group_id, port_name)
        else:
            return add_portfolio_rename(data, group_id, port_name)
        

def insert_portfolio_info(data):
    port = Portfolio_Info(**data)
    db.session.add(port)
    db.session.commit()

    return port    
    

# group_id = 13
# base_name = 'PC MS Portfolio'
       
def add_portfolio_rename(data, group_id, port_name):

    existing_ports = (db.session.query(Portfolio_Info)
                      .filter(Portfolio_Info.port_group_id == group_id)
                      .filter(Portfolio_Info.port_name.like(f'{port_name}%'))
                      .all()
                      )
    
    existing_names = [row.port_name for row in existing_ports]
    existing_names.append('Demo') # 'Demo' is a reserved name
    
    # Determine the next available name
    if port_name not in existing_names:
        new_name = port_name
    else:
        version = 1
        while f"{port_name}-v{version}" in existing_names:
            version += 1
        new_name = f"{port_name}-v{version}"
    
    data['port_name'] = new_name
    
    port = Portfolio_Info(**data)
    db.session.add(port)
    db.session.commit()

    return port    

        
def add_portfolio_replace(data, group_id, port_name):
    
    port = get_portfolio_info(group_id, port_name)
    if port:
        copy_data(port, data)
    else:
        port = Portfolio_Info(**data)
        db.session.add(port)
    db.session.commit()
    return port

def get_portfolio_by_id(port_id):
    port = db.session.get(Portfolio_Info, port_id)
    return port
    
def get_portfolio_info(pgroup_id, port_name):
    port = Portfolio_Info.query.filter_by(port_group_id=pgroup_id, port_name=port_name).first()
    return port

def get_port_by_account(account_id, as_of_date):
    port = Portfolio_Info.query.filter_by(account_id=account_id, is_batch=True, as_of_date=as_of_date).first()
    return port
    
def update_portfolio_status(port_id, port_name=None, status=None, report_id=None, message=None, 
                            as_of_date=None, market_value=None, tail_measure=None, risk_horizon=None, benchmark=None):

    pi = db.session.get(Portfolio_Info, port_id)
    
    # if port_name:
    #     pi.port_name = port_name
    #     # delete existing one with the same name
    #     pi2 = get_portfolio_info(pi.port_group_id, port_name)
    #     if pi2.port_id != pi.port_id:
    #         db.session.delete(pi2)
    
    
    if port_name:
        pi.port_name = port_name
    
    if status:
        pi.status = status
    
    if report_id:
        pi.report_id = report_id
    if message:
        pi.message = message[:200] # 200 max
        
    if as_of_date:
        pi.as_of_date = as_of_date
    if market_value:
        pi.market_value = market_value
    if tail_measure:
        pi.tail_measure = tail_measure
    if risk_horizon:
        pi.risk_horizon = risk_horizon
    if benchmark:
        pi.benchmark = benchmark
    pi.update_date = date.today()
        
    db.session.commit()

# delete rows from table portfolio_info
# port_id_list = [1,2,3,8, 433]
def delete_portfolios(port_id_list):
    # retrieve all portfolios in the list
    portfolios_to_delete = db.session.query(Portfolio_Info).filter(Portfolio_Info.port_id.in_(port_id_list)).all()
    
    portfolios_deleted = []
    for p in portfolios_to_delete:
        print(f'delete portfolio: {p.port_id}, {p.port_group_id}, {p.filename}')
        portfolios_deleted.append((p.port_id, p.port_group_id, p.filename, p.report_id))
        db.session.delete(p)
        
    db.session.commit()
    
    return portfolios_deleted

############## Porfolio_Group functions ###################        
def get_pgroup_by_id(group_id):
    if group_id is None:
        return None
    
    group_id = int(group_id)
    pgroup = models.Portfolio_Group.query.filter_by(pgroup_id=group_id).first()
    return pgroup

def get_group_name(group_id):
    if group_id is None:
        return ""
    
    group_id = int(group_id)
    pgroup = models.Portfolio_Group.query.filter_by(pgroup_id=group_id).first()
    if pgroup:
        return pgroup.group_name
    else:
        return ""
    
def get_pgroup(client_id, group_name):
    if group_name is None:
        return None
    pgroup = models.Portfolio_Group.query.filter_by(client_id=client_id, group_name=group_name).first()
    return pgroup


def get_first_group_id(username):
    group_id = None    
    
    user = models.User.query.filter_by(username=username).first()
    if user.role == 'admin':
        client = user.client
        pgroups = client.port_groups
        if len(pgroups) > 0:
            return pgroups[0].pgroup_id
    else:
        entitlements = user.entitlements
        if len(entitlements) > 0:
            group_id = entitlements[0].port_group_id

    return group_id

def delete_pgroup(pgroup_id):
    pgroup = get_pgroup_by_id(pgroup_id)
    if pgroup:
        db.session.delete(pgroup)
        db.session.commit()
        
############## user functions ###################        
def get_user(username):
    user = models.User.query.filter_by(username=username).first()
    return user

def get_user_by_id(user_id):
    user = models.User.query.filter_by(user_id=user_id).first()
    return user

def get_user_by_port_id(port_id):
    port = get_portfolio_by_id(port_id)
    return get_user_by_id(port.created_user_id)
    
def delete_user(user):
    if user:
        db.session.delete(user)
        db.session.commit()

############## Account functions ###################        
def add_account(user, account_name, client_id):
    """Create new account record"""
    account = models.Account(
        account_name=account_name,
        owner_id=user.user_id,
        client_id=client_id,
        create_time=datetime.now()
    )
    db.session.add(account)
    db.session.commit()
    return account

def get_port_by_account(account_id, as_of_date):
    """Get existing portfolio by account_id and as_of_date"""
    try:
        if as_of_date:
            port = models.Portfolio_Info.query.filter_by(
                account_id=account_id, 
                as_of_date=as_of_date
            ).first()
        else:
            port = models.Portfolio_Info.query.filter_by(
                account_id=account_id
            ).order_by(models.Portfolio_Info.created_time.desc()).first()
        return port
    except Exception as e:
        print(f'Error getting port by account: {str(e)}')
        return None
############## Entitlement functions ###################        
def get_entitlements(user_id, group_id):
    entitlements = models.User_Entitilement.query.filter_by(user_id=user_id, port_group_id=group_id).all()
    return [ent.permission for ent in entitlements]

def save_entitlements(user_id, group_id, permission):
    data = {
        'user_id': user_id,
        'port_group_id': group_id,
        'permission': permission
        }
    
    entitle = models.User_Entitilement.query.filter_by(**data).first()
    if entitle:
        print('entitlement exisits')
    else:
        entitle = models.User_Entitilement(**data)
        db.session.add(entitle)
        db.session.commit()
            
def remove_entitlements(user_id, group_id, permission):
    data = {
        'user_id': user_id,
        'port_group_id': group_id,
        'permission': permission
        }
    
    entitle = models.User_Entitilement.query.filter_by(**data).first()
    if entitle:
        db.session.delete(entitle)
        db.session.commit()
        
        

##########################################################
# TEST
def test():
    username = 'test1@trg.com'
    user = get_user(username)
    client = user.client
    client.users
    
    pgroup_id = 21
    port_name = 'Test2'
    port = get_portfolio_info(pgroup_id, port_name)
    
    account_id = 1001
    as_of_date = '2025-05-16'
    port= get_port_by_account(account_id, as_of_date)
