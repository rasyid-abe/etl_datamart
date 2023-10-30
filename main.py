from falcon_multipart.middleware import MultipartMiddleware
from falcon_cors import CORS
from waitress import serve
from Controller import \
    Compliment, \
    Health, \
    SalesDashboard, \
    TaxReport, \
    Utilization
import falcon
import traceback
from logger import Logger
from datetime import datetime
import pytz
import json
from Middleware.LoggingMiddleware import LoggingMiddleware
from Middleware.ElasticAPMMiddleware import ElasticAPMMiddleware
from notification import SpaceNotifier, test as SpaceNotifierTester
import helper, sys

LOGGER = Logger()
WEBHOOKS = helper.getSpaceWebhook()
if WEBHOOKS != None:
    SPACENOTIFICER = SpaceNotifier(WEBHOOKS, debounce_time=30, env=helper.getENV())

# os.environ['TZ'] = 'Asia/Jakarta'
# time.tzset()

cors = CORS(allow_all_origins=True, allow_all_headers=True, allow_all_methods=True)
#---------------------
#method : get post put
#origins :
#headers : plain,json,

apmMiddleware = None
middlewares = [cors.middleware, MultipartMiddleware(), LoggingMiddleware()]
try:
    apmMiddleware = ElasticAPMMiddleware(**helper.getElasticAPMConfig())
    middlewares.append(apmMiddleware)
except Exception as e:
    LOGGER.info(f"Cannot start APM, {e}")

api = falcon.API(middleware=middlewares)

def generic_error_handler(ex, req, resp, params):
    if (apmMiddleware != None):
        # if we have apm service running then submit exception
        apmMiddleware.client.capture_exception()

    additional_message = f"Error recovering {req.relative_uri}" if "with-param" in req.relative_uri else ""
    LOGGER.log_access("error", f"{additional_message} {ex}" if "with-param" in req.relative_uri else ex, req, json.dumps({"title":"500 Internal Server Error"}),
                    res_time = datetime.now(pytz.timezone("Asia/Jakarta")),
                    traceback="".join(traceback.format_exc()),
                    stack = 2)
    if WEBHOOKS != None:
        SPACENOTIFICER.sendTyped(req.relative_uri, type = "RECOVERY" if "with-param" in req.relative_uri else "ETL", debounce= 300 if "with-param" in req.relative_uri else 120)
    raise falcon.HTTPInternalServerError(title="500 Internal Server Error") #exit

api.add_error_handler(Exception, generic_error_handler)


for endpoint, handler in Health.apis():
    api.add_route(endpoint, handler)

api.add_route('/mart/compliment/compliment-sequence-min', Compliment.sequencePerMin())
api.add_route('/mart/compliment/compliment-sequence-5min', Compliment.sequence5min())
api.add_route('/mart/compliment/compliment-mart-parameter', Compliment.withParameter())
api.add_route('/mart/compliment/compliment-sequence-45min', Compliment.sequence45min())
api.add_route('/mart/compliment/compliment-sequence-120min', Compliment.sequence120min())
api.add_route('/mart/compliment/compliment-with-param', Compliment.withParam())

api.add_route('/mart/sales/dashboard-sequence-min', SalesDashboard.sequencePerMin())
api.add_route('/mart/sales/dashboard-sequence-5min', SalesDashboard.sequence5min())
api.add_route('/mart/sales/dashboard-sequence-45min', SalesDashboard.sequence45min())
api.add_route('/mart/sales/dashboard-sequence-120min', SalesDashboard.sequence120min())
api.add_route('/mart/sales/dashboard-with-param', SalesDashboard.withParam())
api.add_route('/mart/sales/dashboard-with-kafka', SalesDashboard.Kafka())

api.add_route('/mart/report/tax-sequence-min', TaxReport.sequencePerMin())
api.add_route('/mart/report/tax-sequence-5min', TaxReport.sequence5min())
api.add_route('/mart/report/tax-sequence-45min', TaxReport.sequence45min())
api.add_route('/mart/report/tax-sequence-120min', TaxReport.sequence120min())
api.add_route('/mart/report/tax-with-param', TaxReport.withParam())
api.add_route('/mart/report/tax-with-kafka', TaxReport.Kafka())

api.add_route('/mart/service/utilization-mart-parameter', Utilization.withParameter())
api.add_route('/mart/service/utilization-sequence-min', Utilization.sequencePerMin())
api.add_route('/mart/service/utilization-sequence-5min', Utilization.sequence5min())
api.add_route('/mart/service/utilization-sequence-45min', Utilization.sequence45min())
api.add_route('/mart/service/utilization-sequence-120min', Utilization.sequence120min())
api.add_route('/mart/service/utilization-with-param', Utilization.withParam())

for endpoint, handler in AkuntingHutangPiutang.apis():
    api.add_route(endpoint, handler)

api.add_route('/test/alerts-with-param', SpaceNotifierTester())
api.add_route('/test/alerts', SpaceNotifierTester())

serve(api, host='0.0.0.0', port=8001, threads=1000)
