# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 17:50:43 2023

@author: mgdin
"""
from datetime import datetime, date
from database import db
from flask_login import UserMixin


# when you define a class, please make sure the class is same as the structure of data table in database.

class User(db.Model):
    __tablename__ = 'user' # select table in database
    #__table_args__ = {'schema':'public'} # select schema
    #when adding data into the table, if id (primary key) is autoincrement which means you don't need to specify the value 
    #then set autoincrement = true
    #also make sure this column in database is already set to autoincrement
    user_id = db.Column(db.Integer, primary_key=True, autoincrement=True) 
    username = db.Column(db.String(120), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password = db.Column(db.String(60), nullable=False)
    approval = db.Column(db.Integer, nullable=False, default=0)
    phone = db.Column(db.String(20))
    client_id = db.Column(db.Integer, db.ForeignKey('client.client_id'), nullable=False, default=0)
    approval = db.Column(db.Integer, db.ForeignKey('approval.id'), nullable=False, default=0)
    role     = db.Column(db.String(20))
    create_date = db.Column(db.Date, nullable=False, default=datetime.now)
    firstname = db.Column(db.String(100))
    lastname = db.Column(db.String(100))
    activation_completed = db.Column(db.Boolean, default=False)
    webdashboard_login = db.Column(db.String(120), unique=False, nullable=False)
    
    entitlements = db.relationship('User_Entitilement', backref='User', lazy=True)
    
    def get_id(self):
        return self.user_id

    def __repr__(self):
        return f"User('{self.user_id}', '{self.username}', '{self.approval}', '{self.role}', '{self.client_id}')"

class Client(db.Model):
    __tablename__ = 'client'
    client_id   = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_name      = db.Column(db.String(100), unique=True, nullable=False)
    address          = db.Column(db.String(200), nullable=True)
    contact_person   = db.Column(db.String(100), nullable=True)
    contact_phone    = db.Column(db.String(20), nullable=True)
    aum              = db.Column(db.String(50), nullable=True)
    primary_interest = db.Column(db.String(100), nullable=True)
    create_date      = db.Column(db.Date, nullable=False, default=datetime.now)
    
    port_groups = db.relationship('Portfolio_Group', backref='client', lazy=True)
    users = db.relationship('User', backref='client', lazy=True)
    
    def __repr__(self):
        return f"('{self.client_id}', '{self.client_name}','{self.create_date}')"
    
class Portfolio_Group(db.Model):
    __tablename__ = 'portfolio_group'
    pgroup_id   = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_id   = db.Column(db.Integer, db.ForeignKey('client.client_id'), nullable=False)
    group_name  = db.Column(db.String(100))
    create_date = db.Column(db.Date, nullable=False, default=datetime.now)
    
    portfolios  = db.relationship('Portfolio_Info', backref='portfolio_group', lazy=True)
    entitlements = db.relationship('User_Entitilement', backref='portfolio_group', lazy=True)
    
    def __repr__(self):
        return f"('{self.pgroup_id}', '{self.client_id}', '{self.group_name}', '{self.create_date}')"

    
class Portfolio_Info(db.Model):
    __tablename__ = 'portfolio_info'
    port_id         = db.Column(db.Integer, primary_key=True, autoincrement=True)
    port_name  = db.Column(db.String(100), nullable=False)
    filename   = db.Column(db.String(100), nullable=False)
    status     = db.Column(db.String(20))
    report_id  = db.Column(db.String(20))
    created_by = db.Column(db.String(50))
    create_date = db.Column(db.Date, nullable=False, default=datetime.now)
    update_date = db.Column(db.Date, nullable=False, default=datetime.now)
    message = db.Column(db.String(200))
    
    port_group_id = db.Column(db.Integer, db.ForeignKey('portfolio_group.pgroup_id'), nullable=False)
    as_of_date = db.Column(db.Date)
    market_value = db.Column(db.Float)
    tail_measure = db.Column(db.String(20))
    risk_horizon = db.Column(db.String(20))
    benchmark    = db.Column(db.String(100))
    created_user_id = db.Column(db.Integer)
    account_id = db.Column(db.Integer)
    is_batch = db.Column(db.Boolean)

class User_Entitilement(db.Model):
    __tablename__ = 'user_entitilement'
    id            = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_id       = db.Column(db.Integer, db.ForeignKey('user.user_id'), nullable=False, default=0)
    port_group_id = db.Column(db.Integer, db.ForeignKey('portfolio_group.pgroup_id'), nullable=False, default=0)
    permission    = db.Column(db.String(20))    
    update_date = db.Column(db.Date, nullable=False, default=datetime.now)

class Approval(db.Model):
    __tablename__ = 'approval'
    id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.String(20))
    
class Roles(db.Model):
    __tablename__ = 'roles'  
    id = db.Column(db.Integer, primary_key=True)
    role = db.Column(db.String(20)) 

class user_report_mapping_table(db.Model):
    __tablename__ = 'user_report_mapping_table'  
    id          = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_id   = db.Column(db.Integer)
    pgroup_id   = db.Column(db.Integer)
    report_id   = db.Column(db.String(20), nullable=False)
    username    = db.Column(db.String(20), nullable=False)
    email       = db.Column(db.String(120), nullable=False)

class pbi_report_url(db.Model):
    __tablename__ = 'pbi_report_url'  
    id          = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_id   = db.Column(db.Integer, nullable=False)
    pgroup_id   = db.Column(db.Integer)
    report_url  = db.Column(db.String(200), nullable=False)
    is_active   = db.Column(db.Integer)
    create_date = db.Column(db.Date, nullable=False, default=datetime.now)

class pbi_current_report_url(db.Model):
    __tablename__ = 'pbi_current_report_url'  
    id          = db.Column(db.Integer, primary_key=True, autoincrement=True)
    report_url  = db.Column(db.String(200), nullable=False)
    version     = db.Column(db.String(20))
    is_active   = db.Column(db.Integer)
    create_date = db.Column(db.Date, nullable=False, default=datetime.now)

class Account(db.Model):
    __tablename__ = 'account'
    account_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    account_name = db.Column(db.String(100), nullable=False)
    owner_id = db.Column(db.Integer, db.ForeignKey('user.user_id'), nullable=False)
    client_id = db.Column(db.Integer, db.ForeignKey('client.client_id'), nullable=False)
    create_time = db.Column(db.DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"Account('{self.account_id}', '{self.account_name}', '{self.owner_id}', '{self.client_id}')"


########################################################################################
# engine tables
class SecurityInfo(db.Model):
    __tablename__ = 'security_info' # select table in database
    __table_args__ = {'schema':'public'} # select schema
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    SecurityID = db.Column(db.String(20))
    SecurityName = db.Column(db.String(1000), nullable=False)
    Currency = db.Column(db.String(20))
    AssetClass = db.Column(db.String(20))
    AssetType = db.Column(db.String(20))
    DataSource = db.Column(db.String(100))
    DateAdded = db.Column(db.Date, nullable=False, default=datetime.now)
    def __repr__(self):
        return f"SecurityInfo('{self.id}', '{self.SecurityID}', '{self.SecurityName}', '{self.AssetClass}', '{self.AssetType}', '{self.DataSource}', '{self.DateAdded}')"
    
    def __eq__(self, other):
        if isinstance(other, SecurityInfo):
            return (self.SecurityID == other.SecurityID and
                    self.SecurityName == other.SecurityName and
                    self.Currency == other.Currency and
                    self.AssetClass == other.AssetClass and
                    self.AssetType == other.AssetType and
                    self.DataSource == other.DataSource
                    )
        else:
            return False
        
    def assign(self, value):
        if isinstance(value, SecurityInfo):
            self.SecurityID   = value.SecurityID
            self.SecurityName = value.SecurityName
            self.Currency     = value.Currency
            self.AssetClass   = value.AssetClass
            self.AssetType    = value.AssetType
            self.DataSource   = value.DataSource
            
        
class SecurityXref(db.Model):
    __tablename__ = 'security_xref' # select table in database
    __table_args__ = {'schema':'public'} # select schema
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    REF_ID = db.Column(db.String(200), nullable=False)
    REF_TYPE = db.Column(db.String(20), nullable=False)
    SecurityID = db.Column(db.String(20), nullable=False)
    DataSource = db.Column(db.String(100))
    DateAdded = db.Column(db.Date, nullable=False, default=datetime.now)
    def __repr__(self):
        return f"User('{self.id}', '{self.REF_ID}', '{self.REF_TYPE}', '{self.SecurityID}', '{self.DataSource}', '{self.DateAdded}')"
    
class ClientInfo(db.Model):
    __tablename__ = 'client_info' # select table in database
    __table_args__ = {'schema':'public'} # select schema
    client_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_name = db.Column(db.String(120), unique=True, nullable=False)
    create_date = db.Column(db.Date, nullable=False, default=datetime.now)

    def __repr__(self):
        return f"SecurityXref('{self.client_id}', '{self.client_name}', '{self.create_date}')"
    

class ClientModel(db.Model):
    __tablename__ = 'client_model' # select table in database
    __table_args__ = {'schema':'public'} # select schema
    client_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    model_id  = db.Column(db.String(20), nullable=False)
    update_date = db.Column(db.Date, nullable=False, default=datetime.now)

    def __repr__(self):
        return f"ClientModel('{self.client_id}', '{self.model_id}', '{self.update_date}')"
    

class MktDataInfo(db.Model):
    __tablename__ = 'mkt_data_info' # select table in database
    __table_args__ = {'schema':'public'} # select schema
    id         = db.Column(db.Integer, primary_key=True, autoincrement=True)
    SecurityID	    = db.Column(db.String(20))
    Category        = db.Column(db.String(20))
    SecurityName	= db.Column(db.String(1000))
    AssetClass	    = db.Column(db.String(20))
    AssetType	    = db.Column(db.String(20))
    DataSource      = db.Column(db.String(20))
    StartDate	    = db.Column(db.Date, nullable=False)
    EndDate	        = db.Column(db.Date, nullable=False)
    Length	        = db.Column(db.Integer)
    MaxValue	    = db.Column(db.Float)
    MinValue	    = db.Column(db.Float)
    AverageValue	=db.Column(db.Float)
    StdValue	    = db.Column(db.Float)
    LastUpdate	    = db.Column(db.Date, nullable=False, default=datetime.now)
    
    def __repr__(self):
        return "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}'".format(
            self.SecurityID, self.SecurityName, self.AssetClass, self.AssetType, 
            self.StartDate, self.EndDate, self.Length, self.MaxValue, 
            self.MinValue, self.AverageValue, self.StdValue, self.LastUpdate)
    
