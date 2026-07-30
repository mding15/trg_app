# -*- coding: utf-8 -*-
"""
dispatch.py — Thin dispatchers shared by routes.py and ops_routes.py: given a
route name, look up the matching handler in request_handler.py (or
request_handler_ft.py) and return its response as a Flask JSON response.
"""
from flask import jsonify

from api import request_handler, request_handler_ft


def api_request(route, username, request=None):
    if request:
        input_data = request.json
    else:
        input_data = {}

    response, status = request_handler.get_response(route, username, input_data)
    return jsonify(response), status


ft_user_routes = ['risk_calculator']


def api_request_ft(route, username, request):
    if route in ft_user_routes:
        response, status = request_handler_ft.get_response(route, username, request.json)
    else:
        status = 403  # Permission denied
        response = {
            'Status': 'Failed',
            'Error': 'Execution permission denied'
        }
    return jsonify(response), status
