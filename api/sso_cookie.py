# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 15:06:22 2025

@author: mgdin
"""
from flask import jsonify, make_response
import requests

from trg_config import config
from api import app
from flask_cors import CORS
from database import model_aux
# CORS(app, supports_credentials=True)
CORS(app, supports_credentials=True) 

WDB_SSO_KEY = config['WDB_SSO_KEY']

# logger
from api.logging_config import get_logger
logger = get_logger(__name__)

def request(username):
    # username = 'test1@trg.com'
    user = model_aux.get_user(username)
    webdashboard_login = user.webdashboard_login

    logger.info(f"request sso_cookie from user: {webdashboard_login}")
    
    headers = {
    "Referer": "https://www.tailriskglobal.com"
    }

    params = {
        "PortalId": "1200",
        "SecretKey": WDB_SSO_KEY,
        "UserName": webdashboard_login,
        "ReplyUrl": "www.tailriskglobal.com"
    }

    auth_link="https://devapi.tailriskglobal.com/api/Authentication/SetSSOCookie"
    sso_response = requests.get(auth_link, params=params, headers=headers)

    # Log response for debugging
    logger.info(f"Web Dashboard API response: {sso_response.status_code} {sso_response.text}")
    logger.info(f"Set-Cookie: {sso_response.headers.get('Set-Cookie')}")

        # If successful, set the SSO cookie in the response
    if sso_response.status_code == 200:
        
        logger.info(f'sso cookies: {sso_response.cookies}')
        for cookie in sso_response.cookies:
            logger.info(f'{cookie.name} = {cookie.value}')

        response = make_response(jsonify(sso_response.json()))
        
        # Forwarding cookies
        webdashboard_cookie = sso_response.cookies.get('webdashboard')
        if webdashboard_cookie:
            #response.headers['Set-Cookie'] = f"webdashboard={webdashboard_cookie}; Path=/; HttpOnly; Secure; SameSite=None"
            response.set_cookie(
                "webdashboard",                  # 🍪 Cookie name
                webdashboard_cookie,          # Cookie value from the SSO response
                path="/",                     # Cookie is valid for all paths
                secure=True,                  # ⚠️ Must be False for local testing, or browsers will reject the cookie
                httponly=False,                # Cookie can't be accessed by JavaScript (security best practice)
                samesite="Lax",               # ✅ Allows cookie to be sent with top-level navigation (cross-site supported in many cases)
                domain=".tailriskglobal.com" # ✅ Must be set precisely for production (not used on localhost)
            )

        else:
            logger.info('cookies not found in sso_response')

        logger.info("forward cookies ...")
        return response
    else:
        return jsonify({"error": "SSO authentication failed"}), 500
    
def test():
    username = 'test1@trg.com'
    
    headers = {
    "Referer": "https://www.tailriskglobal.com"
    }

    params = {
        "PortalId": "1200",
        "SecretKey": WDB_SSO_KEY,
        "UserName": username,
        "ReplyUrl": "www.tailriskglobal.com"
    }

    auth_link="https://devapi.tailriskglobal.com/api/Authentication/SetSSOCookie"
    sso_response = requests.get(auth_link, params=params, headers=headers)   
    print(sso_response.json())
    for cookie in sso_response.cookies:
        print("🍪", cookie.name, "=", cookie.value)
        
    
def test2():
    username = 'test1@trg.com'
    sso_response = request(username)
    print(sso_response.headers)
