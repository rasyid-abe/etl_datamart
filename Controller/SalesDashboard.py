import json, pytz, falcon
from datetime import datetime
from Models import SalesDashboardMart

class sequencePerMinPandas(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        seq = req.get_param_as_int('sequence') if (req.get_param_as_int('sequence')) else 1
        insert = req.get_param_as_int('size_insert') if (req.get_param_as_int('size_insert')) else 1000
        delete = req.get_param_as_int('size_delete') if (req.get_param_as_int('size_delete')) else 1000
        delay = req.get_param_as_int('delay') if (req.get_param_as_int('delay')) else 300
        sIDStore = req.get_param('idStore')
        content= SalesDashboardMart.processSequencePerMin(seq, sIDStore = sIDStore , n_size_insert= insert, n_size_delete= delete, by ="pandas", delayInSecs= delay)
        data = {'status':'success','content':content,'enum':'1'}
        resp.body = json.dumps(data)


class sequencePerMin(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        seq = req.get_param_as_int('sequence') if (req.get_param_as_int('sequence')) else 1
        insert = req.get_param_as_int('size_insert') if (req.get_param_as_int('size_insert')) else 1000
        delete = req.get_param_as_int('size_delete') if (req.get_param_as_int('size_delete')) else 1000
        core = req.get_param_as_int('spec_core')
        driver_memory = req.get_param_as_int('driver_memory')
        executor_memory = req.get_param_as_int('executor_memory')
        delay = req.get_param_as_int('delay') if (req.get_param_as_int('delay')) else 300
        sIDStore = req.get_param('idStore')
        content= SalesDashboardMart.processSequencePerMin(seq, sIDStore = sIDStore , n_size_insert= insert, n_size_delete= delete,
                                                                core_spec=core,\
                                                                driver_memory_spec=driver_memory,\
                                                                executor_memory_spec=executor_memory,\
                                                                delayInSecs=delay)
        data = {'status':'success','content':content,'enum':'1'}
        resp.body = json.dumps(data)

class sequence5min(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        insert = req.get_param_as_int('size_insert') if (req.get_param_as_int('size_insert')) else 1000
        delete = req.get_param_as_int('size_delete') if (req.get_param_as_int('size_delete')) else 1000
        content= SalesDashboardMart.processSequencePerMin(5, n_size_insert = insert, n_size_delete = delete)
        data = {'status':'success','content':content,'enum':'1'}
        resp.body = json.dumps(data)

class sequence45min(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        insert = req.get_param_as_int('size_insert') if (req.get_param_as_int('size_insert')) else 1000
        delete = req.get_param_as_int('size_delete') if (req.get_param_as_int('size_delete')) else 1000
        content= SalesDashboardMart.processSequencePerMin(45, n_size_insert = insert, n_size_delete = delete)
        data = {'status':'success','content':content,'enum':'1'}
        resp.body = json.dumps(data)

class sequence120min(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        insert = req.get_param_as_int('size_insert') if (req.get_param_as_int('size_insert')) else 1000
        delete = req.get_param_as_int('size_delete') if (req.get_param_as_int('size_delete')) else 1000
        content= SalesDashboardMart.processSequencePerMin(120, n_size_insert = insert, n_size_delete = delete)
        data = {'status':'success','content':content,'enum':'1'}
        resp.body = json.dumps(data)

class withParam(object):
    def on_get(self, req, resp):

        resp.status = falcon.HTTP_200
        #idStore = req.get_param_as_int('id_store')
        sEmail = req.get_param('email')
        idOutlet = req.get_param_as_int('id_outlet') if (req.get_param_as_int('id_outlet')) else None
        #start_time updatedate trx
        stime = req.get_param('start_time')
        #end_time   updatedate trx
        etime = req.get_param('end_time')

        try:
            stime = datetime.strptime(stime, '%Y-%m-%d %H:%M:%S').astimezone(tz=pytz.timezone('Asia/Jakarta'))
            etime = datetime.strptime(etime, '%Y-%m-%d %H:%M:%S').astimezone(tz=pytz.timezone('Asia/Jakarta'))
        except Exception as e:
            resp.status=falcon.HTTP_400
            resp.body = json.dumps({'status':'failed', 'message':f'Error while parsing dates. {e}'})
            return

        content = SalesDashboardMart.processParam("pyspark-dashboard-sales-param", sEmail, stime, etime, idCabang = idOutlet)
        data = {'status': 'success', 'content': content, 'enum': '1'}
        resp.body = json.dumps(data)

    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

class withParamPandas(object):
    def on_get(self, req, resp):

        resp.status = falcon.HTTP_200
        #idStore = req.get_param_as_int('id_store')
        sEmail = req.get_param('email')
        idOutlet = req.get_param_as_int('id_outlet') if (req.get_param_as_int('id_outlet')) else None
        #start_time updatedate trx
        stime = req.get_param('start_time')
        #end_time   updatedate trx
        etime = req.get_param('end_time')

        try:
            stime = datetime.strptime(stime, '%Y-%m-%d %H:%M:%S').astimezone(tz=pytz.timezone('Asia/Jakarta'))
            etime = datetime.strptime(etime, '%Y-%m-%d %H:%M:%S').astimezone(tz=pytz.timezone('Asia/Jakarta'))
        except Exception as e:
            resp.status=falcon.HTTP_400
            resp.body = json.dumps({'status':'failed', 'message':f'Error while parsing dates. {e}'})
            return

        content = SalesDashboardMart.processParamWithPandas(sEmail, stime, etime, idCabang = idOutlet)
        data = {'status': 'success', 'content': content, 'enum': '1'}
        resp.body = json.dumps(data)

    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

class Kafka(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200

        max_message = req.get_param_as_int('max_message') if (
            req.get_param_as_int('max_message')) else 0
        timeout = req.get_param_as_int('timeout') if (
            req.get_param_as_int('timeout')) else 10
        topic_name = req.get_param('topic_name')

        content = SalesDashboardMart.processSalesDashboardSequenceKafka(topic_name, max_message, timeout)
        data = {'status': 'success', 'content': content, 'enum': '1'}
        resp.body = json.dumps(data)
