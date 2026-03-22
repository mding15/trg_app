# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 16:33:55 2024

"""

from flask import Flask
from flask_bcrypt import Bcrypt

# proxy server middleware
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_cors import CORS

from trg_config import config
from database import db, DATABASE_URI

from flasgger import Swagger
import yaml

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB
CORS(app, resources={r"/api/*": {"origins": "*", "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"], "allow_headers": ["Content-Type", "Authorization"]}})

# Trust proxy headers
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)

# import secrets
# secrets.token_hex(16)
app.config['SECRET_KEY'] = config['SECRET_KEY']

# swagger
swg_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": 'apispec_1',
            "route": '/api/apispec_1.json',
            "rule_filter": lambda rule: True,
            "model_filter": lambda tag: True,
        }
    ],
    "static_url_path": "/api/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/api/apidocs/",
    "ui_params": { # didn't work, don't know why?
        "apisSorter": "none",  # Options: "alpha" or "method" or disable
        "operationsSorter": "none"  # Options: "alpha" or "method" or disable
        }
}

# load apidoc
file_path = config['SRC_DIR'] / 'api' / 'apidoc.yml'
with open(file_path, "r") as f:
    swagger_template = yaml.safe_load(f)
    
swagger = Swagger(app, config=swg_config, template=swagger_template)
# swagger = Swagger(app, config=swg_config)

app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI

bcrypt = Bcrypt()

# Bind SQLAlchemy to the API app within the application context
with app.app_context():
    db.init_app(app)
    # db.create_all()  # Create tables if they don't exist

from api import routes

    
