# -*- coding: utf-8 -*-
""" Authentication module for EpiCollect 5 """
""" Taken from https://github.com/fitoprincipe/pyepicollect/tree/07b668a8a86dc925677bf117994c48756e6a4c67 """

import requests
from datetime import datetime

TOKEN_REQ_URL = 'https://five.epicollect.net/api/oauth/token'
REQ_URL = 'https://five.epicollect.net/api'


def pyep_request_token(client_id, client_secret):
    """ Get token. Each token is valid for 2 hours

    :return: A dictionary with the following keys:
    :rtype: dict
    """
    request = requests.post(TOKEN_REQ_URL, data={
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    })
    token_data = request.json()

    time_now = datetime.now()
    token_data['request_time'] = time_now

    return token_data



