from threading import Timer
import requests, json, logging, falcon

LOGGER = logging.getLogger()
LOGGER.setLevel("INFO")

class SpaceNotifier:
    def __init__(self, webhook, debounce_time = 10, env="stagging", silent = 10):
        self.notifications = {}
        self.__debounce_time = debounce_time
        self.__webhook  = webhook
        self.__env      = env
        self.__silent = {}
        self.__silent_threshold = silent
        self.__silent_general = []
    
    def __getMessage(self, etl):
        notif_type = {}
        for type in self.notifications[etl]['message']: #check how many time each type error happened
            if type not in notif_type:
                notif_type[type] = 1
            else:
                notif_type[type] += 1
        message = f"Error occured on {etl} *{self.__env}*, error: "

        for type in notif_type:
            message = f"{message}\n\t- {type} : {notif_type[type] if notif_type[type] < self.__silent_threshold else f'{self.__silent_threshold-1}++'} time(s) raised."

            if(notif_type[type] >= self.__silent_threshold): # if one of error equal or more than silent threshold, then add it to silenced alert
                if etl not in self.__silent:
                    self.__silent[etl] = []
                
                self.__silent[etl].append(type)
                message = f"{message} \n\t\t- *Occured too often, will be silenced till restart!*"
        
        return f"{message}\nFor more detail, please check ELK."

    def send(self, etl, message):
        job = {
            "message" : [message],
            "thread"  : None,
        }
        

        if "with-param" in etl:
            uris = etl.split("?")
            uri = uris[0]
            query_param = uris[1] # keep param -> send email maybe ?

            etl = uri

        if etl in self.__silent:
            if message in self.__silent[etl]:
                # if error is silenced, then do not do anything
                return

        if etl in self.notifications and 'thread' in self.notifications[etl]:
            # if etl already scheduled for alerting, then cancel and debounce
            self.notifications[etl]['thread'].cancel()
            
            job['message'] = self.notifications[etl]['message'] + job["message"] # append eror message  
            self.notifications[etl] = {}   
        
        self.notifications[etl] = job

        def __debounce(etl,  message):
            try:
                self.notifications[etl] = {}
                response = requests.post(self.__webhook, data=json.dumps({"text":message}))
                if response.status_code != 200:
                    LOGGER.error(str(e))
            except Exception as e:
                LOGGER.error(str(e))

        self.notifications[etl]['thread'] = Timer(self.__debounce_time, __debounce,args=[etl, self.__getMessage(etl)] )
        self.notifications[etl]['thread'].start()



    def __getTypedMessage(self, type):
        etl_counts = {}
        for etl in self.notifications[type]['etls']: #check how many time each type error happened
            if etl not in etl_counts:
                etl_counts[etl] = 1
            else:
                etl_counts[etl] += 1

        message = f"Error occured while doing *{type.upper()}* on *{self.__env}* : "

        for etl in etl_counts:
            message = f"{message}\n\t- {etl} : {etl_counts[etl] if etl_counts[etl] < self.__silent_threshold else f'{self.__silent_threshold-1}++'} time(s) raised."

            if(etl_counts[etl] >= self.__silent_threshold): # if one of error equal or more than silent threshold, then add it to silenced alert
                if etl not in self.__silent_general:
                    self.__silent_general.append(etl)
                
                self.__silent_general.append(etl)
                message = f"{message} \n\t\t- *Occured too often, will be silenced till restart!*"
        
        return f"{message}\nFor more detail, please check ELK."
    
    def sendTyped(self, etl, type="ETL" , debounce = None):
        """
            This function will debounce an error space alert. 
            The error is categorized as two categories, ETL and Non-ETL (recoveries:red)
            If no input until debounce is expired then it'll send message to Space
        """

        # remove unnecessary query parameter, take only endpoint
        uris = etl.split("?") 
        etl = uris[0]

        jobs = {
            "etls" : [etl],
            "thread" : None,
        }


        if etl in self.__silent_general:
            return # if silenced then do nothing
        
        if type in self.notifications and 'thread' in self.notifications[type]:
            # if already scheduled, then debounce (cancel and restart)
            self.notifications[type]['thread'].cancel()

            jobs['etls'].extend(self.notifications[type]['etls'])
            self.notifications[type] = {} 
        
        self.notifications[type] = jobs

        def __debounce(type,  message):
            try:
                self.notifications[type] = {}
                response = requests.post(self.__webhook, data=json.dumps({"text":message}))
                if response.status_code != 200:
                    LOGGER.error(str(e))
            except Exception as e:
                LOGGER.error(str(e))

        self.notifications[type]['thread'] = Timer(self.__debounce_time if debounce == None else debounce, __debounce,args=[type, self.__getTypedMessage(type)] )
        self.notifications[type]['thread'].start()
    

class test(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        content = str(1/0)
        data = {'status': 'success', 'content': content, 'enum': '1'}
        resp.body = json.dumps(data)