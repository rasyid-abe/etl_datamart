from .rawKafka import RawKafka
from .raw import Raw
from .daily import Daily
from .monthly import Monthly
from .yearly import Yearly

#define all the service here, while each logic on respective files -> to make easier to import
class RawServiceKafka(RawKafka):
    pass

class RawService(Raw):
    pass

class DailyService(Daily):
    pass

class MonthlyService(Monthly):
    pass

class YearlyService(Yearly):
    pass
