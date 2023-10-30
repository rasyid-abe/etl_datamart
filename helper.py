from dotenv import dotenv_values
from pyspark.conf import SparkConf
import pytz, datetime, math, os

config = dotenv_values(".env.exampletest")

#url_config = "/app/config/"
#url_config = "/Users/macbookpro/Documents/svc-datamart/Config/"

def getConfMySQLGCP():
    confSQL_GCP = {
        "HOST": str(config['HOST_GCP']),
        "USER": str(config['USER_GCP']),
        "PASS": str(config['PASS_GCP']),
        "PORT": str(config['PORT_GCP']),
        "DBNAME": str(config['DBNAME_GCP'])
    }

    return confSQL_GCP

def getConfPostgresMart():
    # jsonFileMart = url_config+"postgresql_source.json"
    # with open(jsonFileMart, "r") as configFile:
    #     return  json.load(configFile)
    confPGSQL_Mart = {
        "HOST": str(config['HOST_TARGET']),
        "USER": str(config['USER_TARGET']),
        "PASS": str(config['PASS_TARGET']),
        "PORT": str(config['PORT_TARGET']),
        "DBNAME": str(config['DBNAME_TARGET'])
    }

    return confPGSQL_Mart

def getConfMySQLMayang():
    # jsonFileMart = url_config+"mysql_source.json"
    # with open(jsonFileMart, "r") as configFile:
    #     return  json.load(configFile)
    confSQLMayang = {
        "HOST": str(config['HOST_SOURCE']),
        "USER": str(config['USER_SOURCE']),
        "PASS": str(config['PASS_SOURCE']),
        "PORT": str(config['PORT_SOURCE']),
        "DBNAME": str(config['DBNAME_SOURCE'])
    }

    if os.environ.get('USE_MASTER'):
        confSQLMayang = {
            "HOST": str(config['HOST_MASTER']),
            "USER": str(config['USER_MASTER']),
            "PASS": str(config['PASS_MASTER']),
            "PORT": str(config['PORT_MASTER']),
            "DBNAME": str(config['DBNAME_MASTER'])
        }

    return confSQLMayang

def getConfMySQLInventory():
    confSQLInventory = {
        "HOST": str(config['HOST_SOURCE_COGS']),
        "USER": str(config['USER_SOURCE_COGS']),
        "PASS": str(config['PASS_SOURCE_COGS']),
        "PORT": str(config['PORT_SOURCE_COGS']),
        "DBNAME": str(config['DBNAME_SOURCE_COGS'])
    }

    return confSQLInventory

def getConfMySQLSlave():
    # jsonFileMart = url_config+"mysql_source.json"
    # with open(jsonFileMart, "r") as configFile:
    #     return  json.load(configFile)
    confSQLSlave = {
        "HOST": str(config['HOST_SLAVE']),
        "USER": str(config['USER_SLAVE']),
        "PASS": str(config['PASS_SLAVE']),
        "PORT": str(config['PORT_SLAVE']),
        "DBNAME": str(config['DBNAME_SLAVE'])
    }

    return confSQLSlave

def getConfMySQLCogs():
    # jsonFileMart = url_config+"mysql_source.json"
    # with open(jsonFileMart, "r") as configFile:
    #     return  json.load(configFile)
    confSQLMayang = {
        "HOST": str(config['HOST_SOURCE_COGS']),
        "USER": str(config['USER_SOURCE_COGS']),
        "PASS": str(config['PASS_SOURCE_COGS']),
        "PORT": str(config['PORT_SOURCE_COGS']),
        "DBNAME": str(config['DBNAME_SOURCE_COGS'])
    }

    return confSQLMayang

def getConfMySQLAkunting():
    # jsonFileMart = url_config+"mysql_source_akunting.json"
    # with open(jsonFileMart, "r") as configFile:
    #     return  json.load(configFile)
    confSQLMayang = {
        "HOST": str(config['HOST_SOURCE_AKUNTING']),
        "USER": str(config['USER_SOURCE_AKUNTING']),
        "PASS": str(config['PASS_SOURCE_AKUNTING']),
        "PORT": str(config['PORT_SOURCE_AKUNTING']),
        "DBNAME": str(config['DBNAME_SOURCE_AKUNTING'])
    }

    return confSQLMayang

def getConfMySQLBiller():
    # jsonFileMart = url_config+"mysql_source.json"
    # with open(jsonFileMart, "r") as configFile:
    #     return  json.load(configFile)
    confSQLBiller = {
        "HOST": str(config['HOST_SOURCE_BILLER']),
        "USER": str(config['USER_SOURCE_BILLER']),
        "PASS": str(config['PASS_SOURCE_BILLER']),
        "PORT": str(config['PORT_SOURCE_BILLER']),
        "DBNAME": str(config['DBNAME_SOURCE_BILLER'])
    }

    return confSQLBiller

def getConfMySQLCRM():
    # jsonFileMart = url_config+"mysql_source.json"
    # with open(jsonFileMart, "r") as configFile:
    #     return  json.load(configFile)
    confSQLCRM = {
        "HOST": str(config.get('HOST_SOURCE_CRM','localhost')),
        "USER": str(config.get('USER_SOURCE_CRM','forge')),
        "PASS": str(config.get('PASS_SOURCE_CRM','')),
        "PORT": str(config.get('PORT_SOURCE_CRM','3306')),
        "DBNAME": str(config.get('DBNAME_SOURCE_CRM','forge'))
    }

    return confSQLCRM


def getConfMySQLCRMDelivery():
    # jsonFileMart = url_config+"mysql_source.json"
    # with open(jsonFileMart, "r") as configFile:
    #     return  json.load(configFile)
    confSQLCRMDelivery = {
        "HOST": str(config.get('HOST_SOURCE_CRM_DELIVERY', 'localhost')),
        "USER": str(config.get('USER_SOURCE_CRM_DELIVERY', 'forge')),
        "PASS": str(config.get('PASS_SOURCE_CRM_DELIVERY', '')),
        "PORT": str(config.get('PORT_SOURCE_CRM_DELIVERY', '3306')),
        "DBNAME": str(config.get('DBNAME_SOURCE_CRM_DELIVERY', 'forge'))
    }

    return confSQLCRMDelivery

def getConfNifi():
    confNifi = {
        "HOST": str(config['NIFI_HOST']),
        "USER": str(config['NIFI_USER']),
        "PASS": str(config['NIFI_PASS'])
    }

    return confNifi

def generate_mysql_connection_url(json_config):
    return "jdbc:mysql://{}:{}/{}?rewriteBatchedStatements=true&enabledTLSProtocols=TLSv1.2&useSSL=false".format(json_config["HOST"], json_config["PORT"], json_config["DBNAME"])

def generate_postgres_connection_url(json_config):
    return "jdbc:postgresql://{}:{}/{}?rewriteBatchedStatements=true&enabledTLSProtocols=TLSv1.2&useSSL=false".format(json_config["HOST"], json_config["PORT"], json_config["DBNAME"])

# def get_spark_config(appName , master = "spark://spark-master:7077", driver_memory:float=1000, executor_memory:float=2500, executor_core:int=2):
#     conf = SparkConf()
#
#     if os.environ.get('DEBUG'):
#         conf.setMaster("local[*]")
#     else:
#         conf.set("spark.driver.bindAddress", "spark-master")
#         conf.set("spark.driver.host", "spark-master")
#         conf.set("spark.driver.bindAddress", "spark-master")
#         conf.set("spark.driver.host", "spark-master")
#         conf.set("spark.driver.memory", "{}m".format(driver_memory))
#         conf.set("spark.executor.memory", "{}m".format(executor_memory))
#         conf.set("spark.driver.maxResultSize", "4g")
#         conf.set("spark.executor.cores", "{}".format(executor_core))
#         conf.setMaster(master)
#
#     conf.set("spark.logConf", "true") #enabling log conf
#     conf.set("spark.dynamicAllocation.enabled", "false")
#     conf.set("spark.ui.showConsoleProgress", "false") # surpress "stage" log
#     conf.setAppName(appName)
#
#     return conf

def getKafkaBrokerConf(consumer):
    conf = {
        'bootstrap.servers': str(config['KAFKA_BROKER_HOST']),
        'group.id': consumer,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False,
        'max.poll.interval.ms':3600000
    }

    return conf

def get_spark_config(appName , master = "local[*]", driver_memory:int=8, executor_memory:int=8, executor_core:int=4):
    conf = SparkConf()
    conf.set("spark.logConf", "true") #enabling log conf
    conf.set("spark.executor.memory", str(executor_memory)+"g")
    conf.set("spark.driver.memory", str(driver_memory)+"g")
    conf.set("spark.executor.cores",str(executor_core))
    conf.set("spark.ui.showConsoleProgress", "false") # surpress "stage" log
    conf.setMaster(master)
    conf.setAppName(appName)

    return conf

def getSpaceWebhook():
    return str(config['SPACE_WEBHOOK']) if "SPACE_WEBHOOK" in config else None

def getENV():
    return str(config['ENV']) if 'ENV' in config else "stagging"

def getElasticAPMConfig():
    local_env_servicename = os.environ.get("ELASTIC_APM_SERVICE_NAME", config["ELASTIC_APM_SERVICE_NAME"] if "ELASTIC_APM_SERVICE_NAME" in config else None)
    local_env_url         = os.environ.get("ELASTIC_APM_SERVER_URL", config["ELASTIC_APM_SERVER_URL"] if "ELASTIC_APM_SERVER_URL" in config else None)


    if local_env_servicename == None or local_env_servicename == "":
        # if both .env & docker-compose.yaml service name is not provided
        raise Exception("APM Service Name is not provided!")


    configs = {
        "elasticapm_service_name" : local_env_servicename,
        "service_name" : local_env_servicename,
        "service.name" : local_env_servicename,
    }

    if local_env_url == None or local_env_url == "":
        # if both .env & docker-compose.yaml service url is not provided
        raise Exception("APM Service URL is not provided!")

    configs["server_url"] = local_env_url


    if "ELASTIC_APM_HOSTNAME" in config and config["ELASTIC_APM_HOSTNAME"] != None  and config["ELASTIC_APM_SERVER_URL"] != "":
        configs["hostname"] = config["ELASTIC_APM_HOSTNAME"]

    if "ELASTIC_APM_SECRET_TOKEN" in config and config["ELASTIC_APM_SECRET_TOKEN"] != None  and config["ELASTIC_APM_SERVER_URL"] != "":
        configs["secret_token"] = config["ELASTIC_APM_SECRET_TOKEN"]

    if "ELASTIC_APM_API_KEY" in config and config["ELASTIC_APM_API_KEY"] != None  and config["ELASTIC_APM_SERVER_URL"] != "":
        configs["api_key"] = config["ELASTIC_APM_API_KEY"]

    configs["environment"] = os.environ.get("ELASTIC_APM_ENVIRONMENT", config["ELASTIC_APM_ENVIRONMENT"] if "ELASTIC_APM_ENVIRONMENT" in config else None)

    return configs
