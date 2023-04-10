""" Authentication helper methods for MetaRepo2, assuming IDM """

import configparser
import time
import requests

from fastapi import HTTPException

config = configparser.ConfigParser()
config.read('repo.conf')

def authenticate(token):
    """ Given a token, get the associated user """
    if token is None:
        return None
    if not token.startswith("Bearer "): # Make sure our bearer token starts with the prefix
        token = "Bearer " + token
    url = config.get('AUTHSERVICE', 'admin_url') + config.get('AUTHSERVICE', 'checkAuth_endpoint')
    ses = requests.session()
    res = ses.post(url, headers={"Authorization": token})
    if res.status_code == 200:
        return res.json()
    return None

def get_groups(user_info):
    """ Get a list of groups belong to an authenticated user """
    if user_info is None:
        return []
    groups = user_info['ownerGroups']
    return groups

def in_group(user_info, group, sso_allowed=False):
    """ Check if a user belongs to a group. If sso_allowed is True,
    then treat the user's SSO as if it's a group they belong to """
    groups = get_groups(user_info)
    for user_group in groups:
        if group == user_group['idmGroupId']:
            return True
    if sso_allowed and user_info["username"] == group:
        return True
    return False

def check_authorization(authorization):
    """ Make sure the authorization is current """
    # expiresAt is in ms, time.time() is in second
    if authorization is None or int(authorization["expiresAt"]) < time.time()*1000:
        raise HTTPException(status_code=401)
