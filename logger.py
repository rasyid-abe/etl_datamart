"""  
    Version: 2
    
    How to use:
    1. Import Logger
    2. change the default DEFAULT_REPO global variable accordingly
       - you can set with setRepo("reponame"), you just need to call it once
    3. Happy logging
    
    ex: 
        from logger import Logger
        
        logger = Logger()
        
        #basic usage
        logger.warning("message")


        logger.error("message", params={"request_time"=datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%f %z")})
        # put our custom "log attributes" on params argument since the package only provide few items
            # Provided item: https://github.com/madzak/python-json-logger#example-output
        # please refer to the Majoo's log standarization documents 
    
        #dry
        logger.log_access(type, message, params, request , response)
    
    
    Requirements:
        - python-json-logger==2.0.2
            - pip install python-json-logger
            - https://github.com/madzak/python-json-logger
        - pytz
            - pip install pytz
    
    Author: Akbar Noto P.B.
    
"""

DEFAULT_REPO = "svc-datamart"


import logging
from pythonjsonlogger import jsonlogger
from datetime import datetime
import json
import inspect
import pytz, uuid
import traceback

class StandarizedFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        """
            This function define custom keys based on given attributes of the package
            https://github.com/madzak/python-json-logger#example-output
        """
        super(StandarizedFormatter, self).add_fields(log_record, record, message_dict)
        
        if not log_record.get('time'):
            # this doesn't use record.created, so it is slightly off
            now = datetime.now(pytz.timezone("Asia/Jakarta")).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
            log_record['time'] = now

        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname

        
        if not log_record.get("handler"):
            log_record["handler"]   = record.funcName
        
        if not log_record.get("service_name"):
            log_record["service_name"] = DEFAULT_REPO
                
        
  
class Logger(object):
    __instance  = None
    __logger    = None
    __repo      = DEFAULT_REPO
    
    @staticmethod
    def getInstance():
        """ Static access method. """
        if Logger.__instance == None:
            Logger()
        return Logger.__instance

    def __init__(self):
        """ Init Logger Singleton """
        Logger.__instance    = self
        Logger.__logger      = self.initiate_logger()
    
    def warning(self, message, params = None):
        """ Log with WARN level """
                    
        params = self.add_default_params(params)
        Logger.__logger.warning(message, extra=params)
    
    def error(self, message, params = None):
        """ Log with ERROR level """
            
        params = self.add_default_params(params)
        Logger.__logger.error(message, extra=params)


    def info(self, message, params = None):
        """ Log with INFO level """
      
        params = self.add_default_params(params)
        Logger.__logger.info(message, extra=params)
        
    
    def set_repo(self, name):
        """ change default service_name """
        Logger.__repo = name
        
    def add_default_params(self, params):
        """ set your default value here """
        if(params == None):
            params = {}
        
        params["service_name"] = Logger.__repo
        return params

    def initiate_logger(self):
        """
            This function will generate the json python logger if does not exist
            If it already exists on our logger system, then return that particular logger.
        """
        
        logger = logging.getLogger()
        logger.setLevel("INFO")
        
        if(logger.hasHandlers()):
            for handler in logger.handlers:
                # check if our standarized logger exist
                if(type(handler.formatter) == StandarizedFormatter ):
                    # if yes then return
                    return logger
    
        #if no our standarized formatted logger does not exist, then create logger
        #put the default attribute on the string as follows, if the attribute is not set then it will return Null
        formatter   = StandarizedFormatter("%(time)s %(level)s %(handler)s %(service_name)s %(message)s %(package)s %(unique_id)s %(uri)s %(error_code)d %(request_time)s %(response_time)s %(processing_time)f %(request_param)s %(data)s")
        logHandler  = logging.StreamHandler()
        
        logHandler.setFormatter(formatter)
        logger.addHandler(logHandler)

        return logger
    
    def get_class_from_frame(self, fr):
        args, _, _, value_dict = inspect.getargvalues(fr)
        # we check the first parameter for the frame function is
        # named 'self'
        if len(args) and args[0] == 'self':
            # in that case, 'self' will be referenced in value_dict
            instance = value_dict.get('self', None)
            if instance:
                # return its class
                return getattr(instance, '__class__', None)
        # return None otherwise
        return None
    
    def log_access(self, type:str, message, req, body, req_time:datetime = None, res_time:datetime = None, logs:dict = {} , 
                    stack = 1 , traceback = None, error_code = None, package_name = None , function_name = None ):
        """ 
            This function will log http access on controller 
            for incidental error / warn please refer to basic usage
        """

        if Logger.__instance == None:
            Logger()

        if(isinstance(message, Exception)):
            stacks = message.__traceback__
            while stacks.tb_next != None:
                stacks = stacks.tb_next #find root cause

            logs["package"] = stacks.tb_frame.f_code.co_filename
            logs["handler"] = stacks.tb_frame.f_code.co_name
            logs["line"]    = stacks.tb_lineno

        elif package_name != None and function_name != None: 
            # try manual input first since the performance of manual > traceback > inspect
            # ref: https://gist.github.com/JettJones/c236494013f22723c1822126df944b12
            logs["package"] = package_name
            logs["handler"] = function_name

        else:
            try: 
                # try fecth class caller 
                # please avoid using inspect << slow!
                inspect_stacks          = inspect.stack(0)
                logs["package"]         = self.get_class_from_frame(inspect_stacks[stack][0])
                #try fecth function caller
                logs["handler"]         = inspect_stacks[stack][3]

                """ 
                example:
                    def level1():
                        print(stacks) -> [ 0 : level1, 1 : level2 , 2 : level3, ... ]
                    
                    def level2():
                        print(stacks) -> [ 0 : level2, 1 : level3, ...]
                        level1()
                    
                    def level3():
                        print(stacks) -> [ 0 : level3, 1 : main, ... ]
                        level2()

                    if __main__:
                        level3()
                stacks:
                """

            except Exception as e:
                # if doesnot support show warning (developed using 3.10)
                self.warning(e)
        
        # hide traceback / encapsulate
        # logs["traceback"]           = traceback
        logs["unique_id"]           = str(uuid.uuid4())
        logs["request_param"]       = json.dumps(req.params)
        logs["response_message"]    = body
        logs["data"]                =   {
                                            "http_method"       : req.method,
                                            "request_header"    : req.headers,
                                            "merchant_id"       : req.get_param_as_int('id_store')  if (req.get_param_as_int('id_store')) else None,
                                            "outlet_id"         : req.get_param_as_int('id_outlet') if (req.get_param_as_int('id_outlet')) else None,
                                            "remote_ip"         : req.get_param("access_route"),
                                            "host"              : req.host,
                                        }
        logs["uri"]                 = req.relative_uri

        if(req_time != None and res_time != None):
            logs["request_time"]    = req_time.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
            logs["processing_time"] = (res_time - req_time).total_seconds() * 1000

        if res_time != None: # in case only res time only, without req time (uncaught error)
            logs["response_time"]   = res_time.strftime('%Y-%m-%dT%H:%M:%S.%f %z')
        
        if error_code != None:
            logs["error_code"]  = error_code
        
        if(type == "info"):
            self.info(message, params=logs)
        elif(type == "warning"):
            self.warning(message, params=logs)
        elif(type == "error"):
            logs["response_message"] = json.dumps({"title": "500 Internal Server Error"})
            self.error(message, params=logs)
        

"""
    Changelog:
        [29/3/2022] - Init
        [30/3/2022] - Singleton Instance
                    - set Level to Info for future usage
        [31/3/2022] - Add more details in order to keep source code DRY (Don't Repeat Yourself)
        [1/4/2022 ] - optimizing inspect performance issue
                    - log attribute body -> data
                    - response message on error
"""