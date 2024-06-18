import logging
from datetime import datetime
import pytz
import threading

thread_local = threading.local()

class ListLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()

    def emit(self, record):
        if not hasattr(thread_local, 'log_list'):
            thread_local.log_list = []
        log_entry = self.format(record)
        thread_local.log_list.append(log_entry)

class KSTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        kst = pytz.timezone('Asia/Seoul')
        ct = datetime.fromtimestamp(record.created, kst)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s,%03d" % (s, record.msecs)
        return s

def setup_logger():
    handler = ListLogHandler()
    handler.setFormatter(KSTFormatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - \n%(message)s'))

    logger = logging.getLogger(__name__)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    return logger
