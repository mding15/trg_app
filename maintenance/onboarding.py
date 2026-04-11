# -*- coding: utf-8 -*-
"""
Created on Fri Nov 15 10:41:32 2024

@author: mgdin
"""

from api.create_account import approve_user, create_account


def account_activation():

    user_id = 1051
    approve_user(user_id)


def manual_create_account():
    data = {
        'firstName': 'test',
        'lastName' : 'test',
        'email' : 'test@trg.com',
        'password' : 'test',
        'companyName' : 'test',
        }

    create_account(data)
