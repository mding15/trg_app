# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 18:22:03 2024

@author: mgdin
"""

from flask import Flask, jsonify
from flasgger import Swagger
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Trust proxy headers
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)

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
    "specs_route": "/api/apidocs/"
}

swagger = Swagger(app, config=swg_config)

@app.route('/api/hello', methods=['GET'])
def hello():
    """
    A simple hello world API endpoint.
    ---
    responses:
      200:
        description: A successful response
        schema:
          type: object
          properties:
            message:
              type: string
              example: "Hello, World!"
    """
    return jsonify(message="Hello, World!")

if __name__ == '__main__':
    app.run(debug=False, port=5050)

