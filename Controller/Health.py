import falcon
import json
import requests
import helper
import re

def apis():
    return [
        ('/health', healthcheck()),
        ('/nifi/health', nifiHealthcheck()),
        ('/health/timezone', timezoneCheck()),
        ('/version', versioncheck()),
    ]

class healthcheck(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200

        # checking spark condition        
        res = requests.get(f'http://{req.host}:8080/json/')
        res = json.dumps(res.json())

        with open('CHANGELOG.md', 'r') as f:
            changelog_str = f.read()

        version_str = re.search(r'(\[(\d+\.)?(\d+\.)?(\*|\d+)\])', changelog_str) \
            .group() \
            .replace('[', '').replace(']', '')

        if 'activeapps' not in res:
            resp.status =  falcon.HTTP_500
        else:
            data = {'status': 'success', 'content': 'OK', 'enum': '1', 'version': version_str}
        
        resp.body = json.dumps(data)

class nifiHealthcheck(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        conf = helper.getConfNifi()

        head_tok = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': '*/*',
        }
    
        data_tok = {
            'username': conf['USER'],
            'password': conf['PASS'],
        }
    
        res_tok = requests.post(f"https://{conf['HOST']}/nifi-api/access/token", headers=head_tok, data=data_tok)
        token = res_tok.text
    
        headers = {
            # 'Accept-Encoding': 'gzip, deflate, sdch, br',
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
        }
    
        res = requests.get(f"https://{conf['HOST']}/nifi-api/system-diagnostics", headers=headers)
        resp.status = res.status_code
        resp.body = json.dumps(res.json())

class versioncheck(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200

        with open('CHANGELOG.md', 'r') as f:
            changelog_str = f.read()

        version_str = re.search(r'(\[(\d+\.)?(\d+\.)?(\*|\d+)\])', changelog_str) \
            .group() \
            .replace('[', '').replace(']', '')
        
        data = {'version': version_str}
        
        resp.body = json.dumps(data)

class timezoneCheck(object):
    def on_post(self, req, resp):
        pass

    def on_get(self, req, resp):
        from datetime import datetime
        import pytz

        now     = datetime.now()
        casted  = datetime.now().astimezone(tz=pytz.timezone('Asia/Jakarta'))

        resp.status = falcon.HTTP_200
        resp.body   = json.dumps({
            'now': now,
            'casted': casted
        }, default=str)
