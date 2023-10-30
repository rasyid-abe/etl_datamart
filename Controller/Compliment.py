import falcon
import json
from datetime import datetime
from Models import ComplimentMart

class sequencePerMin(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        seq = req.get_param_as_int('sequence') if (req.get_param_as_int('sequence')) else 1
        content=ComplimentMart.processComplimentSequencePerMin(seq)
        data = {'status':'success','content':content,'enum':'1'}
        resp.body = json.dumps(data)

class sequence5min(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        content=ComplimentMart.processComplimentSequence5Min()
        data = {'status':'success','content':content,'enum':'1'}
        resp.body = json.dumps(data)

class sequence45min(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        content=ComplimentMart.processComplimentSequence45Min()
        data = {'status':'success','content':content,'enum':'1'}
        resp.body = json.dumps(data)

class sequence120min(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        content=ComplimentMart.processComplimentSequence120Min()
        data = {'status':'success','content':content,'enum':'1'}
        resp.body = json.dumps(data)

        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

# with parameter idStore, idOutlet, start, end
class withParam(object):
    def on_get(self, req, resp):

        resp.status = falcon.HTTP_200
        #idStore = req.get_param_as_int('id_store')
        sEmail = req.get_param('email')
        idOutlet = req.get_param_as_int('id_outlet') if (req.get_param_as_int('id_outlet')) else 0
        #start_time updatedate trx
        stime = req.get_param('start_time')
        #end_time   updatedate trx
        etime = req.get_param('end_time')

        data = ComplimentMart.processComplimentParam("pyspark-sales-addon-param", sEmail, idOutlet, stime, etime)
        # data = {'status': 'success', 'content': content, 'enum': '1'}
        resp.body = json.dumps(data)

    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

# dari backend -> akan input start & end time (updatedate) proses nya di periode tersebut
# dari nifi -> akan input end time & period -> hal ini akan mengecek dulu terakhir proses yg terjadi di table history kolom end.

class withParameter(object):
    def on_get(self, req, resp):

        resp.status = falcon.HTTP_200
        #start_time [backend]
        stime = req.get_param('start_time')
        #end_time   [backend]   [nifi]
        etime = req.get_param('end_time')
        #period                 [nifi]
        period = req.get_param('period')

        flag_check_date_format=False
        flag_check_period_format=False
        if('start_time' in req.params):
            try:
                if(stime!=None and len(stime)==19):
                    datetime.strptime(stime, '%Y-%m-%d %H:%M:%S')
                else:
                    flag_check_date_format=True
            except ValueError:
                flag_check_date_format=True

        if('end_time' in req.params):
            try:
                if(etime!=None and len(etime)==19):
                    datetime.strptime(etime, '%Y-%m-%d %H:%M:%S')
                else:
                    flag_check_date_format=True
            except ValueError:
                flag_check_date_format=True

        if('period' in req.params):
            try:
                if(period!=None ):
                    int(period)
                else:
                    flag_check_period_format=True
            except ValueError:
                flag_check_period_format=True

        if('start_time' in req.params and 'end_time' in req.params):
            if(flag_check_date_format):
                data = {'status': 'error', 'message': 'check your date format YYYY-MM-DD HH:II:SS', 'enum': '901'}
                resp.status=falcon.HTTP_400
            else:
                content=ComplimentMart.processComplimentParamBegEndTime('pyspark-postgresql-start-end-time-api',datetime.strptime(stime, '%Y-%m-%d %H:%M:%S'),datetime.strptime(etime, '%Y-%m-%d %H:%M:%S'),{'fn': 'startendtime', 'start_time':str(stime),'end_time': str(etime)})
                data = {'status':'success','content':content,'enum':'1'}
        elif('end_time' in req.params and 'period' in req.params):
            if(flag_check_date_format):
                data = {'status': 'error', 'message': 'check your date format YYYY-MM-DD HH:II:SS', 'enum': '901'}
                resp.status=falcon.HTTP_400
            elif(flag_check_period_format):
                data = {'status': 'error', 'message': 'check your period format must be an integer', 'enum': '902'}
                resp.status=falcon.HTTP_400
            else:
                content = ComplimentMart.processComplimentParamEndTimePeriod(datetime.strptime(etime, '%Y-%m-%d %H:%M:%S'),int(period))
                data = {'status': 'success', 'content': content, 'enum': '1'}
        else:
            data = {'status': 'error', 'message': 'check your parameter', 'enum': '903'}
            resp.status=falcon.HTTP_400
        resp.body = json.dumps(data)

    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)
