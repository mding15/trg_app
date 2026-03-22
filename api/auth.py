# -*- coding: utf-8 -*-
"""
Created on Sat Nov  2 15:39:24 2024

@author: mgdin

API Authentication:
    - authenticate()
    - token_required()
    
    
"""
from flask import request, jsonify
import jwt
import datetime
from functools import wraps
from api import app, bcrypt
from database.models import User

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.args.get('token')

        if not token:
            print('Error: token is missing!')
            return jsonify({'message': 'Token is missing!'}), 401

        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            username = data['username']
            
        except jwt.ExpiredSignatureError as e:
            print("Exception:", e)
            return jsonify({'message': 'Token has expired!'}), 401

        except jwt.exceptions.InvalidTokenError as e:
            print("Exception:", e)
            return jsonify({'message': 'Token is invalid!'}), 401


        return f(*args, username, **kwargs)

    return decorated

def authenticate():
    
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    print(f"Login Request: {username}")
    # get user from database
    user = User.query.filter_by(username=username).first()

    # Check if the provided credentials match the expected ones
    if user:
        if user.approval == 1:
            if user and bcrypt.check_password_hash(user.password, password):
                # Generate JWT token
                token = jwt.encode({'username': username, 'exp': datetime.datetime.utcnow() + datetime.timedelta(days=1)}, app.config['SECRET_KEY'])
                return token, user
            else:
                raise Exception('Invalid email or password')
        else:
            raise Exception('Your account is pending for approval!')
    else:
        raise Exception('Login Unsuccessful. Please check username and password!')







