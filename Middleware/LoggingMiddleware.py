from datetime import datetime
import pytz
from logger import Logger
import falcon

class LoggingMiddleware(object):
    def __init__(self):
        self.__logger = Logger()

    def process_request(self, req, res):
        req.context.start_time = datetime.now(pytz.timezone("Asia/Jakarta"))

    async def process_request_async(self, req, res):
        req.context.start_time = datetime.now(pytz.timezone("Asia/Jakarta"))


    def process_resource(self, req, resp, resource, params):
        req.context.classname = resource

    # uncomment these lines for info & warning logs
    # def process_response(self, req, resp, resource, req_succeeded):
    #     if(req_succeeded):
    #         # if succeed only since unsucceeded response should be handled by generic_error_handler
    #         method = "on_get" if req.method == "GET" else "on_post"
    #         log_type , message = ("info", "success") if resp.status == falcon.HTTP_200 else ("warning", "unavailable service")
            
    #         self.__logger.log_access(log_type, message, req, resp.body, req_time = req.context.start_time, 
    #                 res_time = datetime.now(pytz.timezone("Asia/Jakarta")), package_name=req.context.classname, function_name=method)