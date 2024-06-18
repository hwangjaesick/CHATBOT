import logging
import pytz
from datetime import datetime
from threading import local

thread_local = local()

class ListLogHandler(logging.Handler):
    def __init__(self, chat_id, chat_session_id, rag_order):
        super().__init__()
        self.chat_id = chat_id
        self.chat_session_id = chat_session_id
        self.rag_order = rag_order

    def update_ids(self, new_chat_id, new_session_id, rag_order):
        self.chat_id = new_chat_id
        self.chat_session_id = new_session_id  
        self.rag_order = rag_order  

    def emit(self, record):
        if not hasattr(thread_local, 'log_list'):
            thread_local.log_list = []
        log_entry = self.format(record)

        log_entry_with_ids = f"{self.chat_id}_{self.chat_session_id}_{self.rag_order} - {log_entry}"
        thread_local.log_list.append(log_entry_with_ids)

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
    
def setup_logger(chat_id, chat_session_id, rag_order):
    logger = logging.getLogger(__name__)
    
    if logger.handlers:
        for handler in logger.handlers:
            if isinstance(handler, ListLogHandler):
                handler.update_ids(chat_id, chat_session_id, rag_order)
    else:
        handler = ListLogHandler(chat_id, chat_session_id, rag_order)
        handler.setFormatter(UTCFormatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - \n%(message)s'))
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

    return logger
