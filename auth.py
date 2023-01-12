import configparser
import requests

config = configparser.ConfigParser()
config.read('metarepo.conf')

def authenticate(token):
    # TODO: Does logging in as SSO properly update expiresAt time?
    if token is None: return None
    if not token.startswith("Bearer "): # We're using bearer tokens, it may or may not come with the prefix
        token = "Bearer " + token
    url = config.get('AUTHSERVICE', 'admin_url') + config.get('AUTHSERVICE', 'checkAuth_endpoint')
    s = requests.session()
    m = s.post(url, headers={"Authorization": token})
    if m.status_code == 200:
        return m.json()
    return None

def getGroups(userInfo):
    if userInfo is None: return []
    groups = userInfo['ownerGroups']
    return groups

def inGroup(userInfo, group, sso_allowed=False):
    groups = getGroups(userInfo)
    for usrGroup in groups:
        if group == usrGroup['idmGroupId']:
            return True
    if sso_allowed and userInfo["username"] == group:
        return True
    return False

