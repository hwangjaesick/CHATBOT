import logging
import pytz
from datetime import datetime
from threading import local
from azure.storage.blob import BlobServiceClient
from io import StringIO
import os

# 스레드 로컬 데이터를 저장할 변수 생성
thread_local = local()

class ListLogHandler(logging.Handler):
    def __init__(self, preprocess_type):
        super().__init__()
        self.preprocess_type = preprocess_type

    def update_data_type(self, new_preprocess_type):
        self.preprocess_type = new_preprocess_type

    def emit(self, record):
        if not hasattr(thread_local, 'log_list'):
            thread_local.log_list = []
        log_entry = self.format(record)

        log_entry_with_data_type = f"{self.preprocess_type} - {log_entry}"
        thread_local.log_list.append(log_entry_with_data_type)

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
    
class UTCFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        utc = pytz.utc
        ct = datetime.fromtimestamp(record.created, utc)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s,%03d" % (s, record.msecs)
        return s

def setup_logger(preprocess_type):
    logger = logging.getLogger(__name__)
    
    if logger.handlers:
        for handler in logger.handlers:
            if isinstance(handler, ListLogHandler):
                handler.update_data_type(preprocess_type)
    else:
        handler = ListLogHandler(preprocess_type)
        handler.setFormatter(UTCFormatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - \n%(message)s'))
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

    return logger

