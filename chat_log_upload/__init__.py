import azure.functions as func
import os
import logging  

import numpy as np
import pandas as pd
import traceback

from datetime import datetime, timedelta, timezone
# 추가 packages
from configobj import ConfigObj
from os import path
from common import db_api, cosmos_api
from urllib.parse import quote_plus
import pytz
import time
from azure.storage.blob import BlobServiceClient, ContentSettings

# 환경설정
_env = 'PROD'
# _env = 'DEV'

# 현재 절대경로
_rootDir = './'
# .env 파일 경로
_configFile = os.path.join(_rootDir, 'config/.env')
# .env 파일 읽기
_config = ConfigObj(_configFile)

# MySQL DB 정보 가져오기
_db_config = _config['PROD_DB']
# _db_config = _config['DEV_DB']

_myhost = _db_config['HOST']
_myuser = _db_config['USER']
_mypw = _db_config['PW']
_mydb = _db_config['DB']
_myport = int(_db_config['PORT'])

# Cosmos DB 정보 가져오기
_cos_config = _config['PROD_COSMOSDB']

_cos_url = _cos_config['URL']
_cos_key = _cos_config['KEY']
_cos_db = _cos_config['DB']
_cos_cont = _cos_config['CONT']
_cos_rag_cont = _cos_config['RAG_CONT']

_as_config = _config['PROD_STORAGE']
# _as_config = _config['DEV_STORAGE']

_as_key = _as_config['KEY']
_as_name = _as_config['NAME']
_as_str = _as_config['STR']


def main(mytimer: func.TimerRequest) -> None:

    from log import setup_logger, thread_local

    formatted_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    api_type = 'chatlog'
    data_type = formatted_date +'-'+ api_type

    logger = setup_logger(data_type)
    logger.info('===== Chat log data summary start!')
    logger.info("=" * 30 + " POD_NAME " + "=" * 30)
    logger.info(dict(os.environ)['HOSTNAME'])

    '''
    now = datetime.now()
    start_time = now - timedelta(hours=10)
    start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time_str = now.strftime('%Y-%m-%d %H:%M:%S')
    '''

    try :
        cos_db = cosmos_api.get_db(logger, _cos_url, _cos_key, _cos_db)
        conn = db_api.db_conn(logger, _myhost, _myport, _myuser, _mypw, _mydb)

        # # 지정일자 및 시간을 돌릴 때 사용
        # init_start_time = datetime(2024, 6, 2, 0, 0, 0)

        # 스케쥴용 시간
        init_start_time = datetime.now(timezone.utc)

        # 지정일자 및 시간을 돌릴때는 range 값 조정, 스케쥴링일 때는 range(0,1)
        for i in range(0,1):

            # # 지정일자 및 시간을 돌릴 때 사용
            # start_time = init_start_time + timedelta(days=i)
            # # 지정일자 및 시간을 돌릴 때 사용
            # end_time = init_start_time + timedelta(days=i+1)

            # # 스케쥴용 Start Time
            start_time = init_start_time + timedelta(hours=-6)
            # # 스케쥴용 End Time
            end_time = init_start_time

            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
            end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

            logger.info(f'===== Summary from : {start_time_str}  to : {end_time_str}')

            cos_cont = cosmos_api.get_container(logger, cos_db, _cos_cont)
            
            chat_ids = get_chat_ids(logger, cos_cont, start_time_str, end_time_str)

            if len(chat_ids) > 0:
                
                # conn = db_api.db_conn(_myhost, _myport, _myuser, _mypw, _mydb)
                make_tmp_chat_log_data(logger, conn, cos_cont, chat_ids)

                cos_cont = cosmos_api.get_container(logger, cos_db, _cos_rag_cont)

                # conn = db_api.db_conn(_myhost, _myport, _myuser, _mypw, _mydb)
                make_tmp_chat_rag_data(logger, conn, cos_cont, chat_ids)

                # 프로시저 호출

                logger.info(f'===== Summary Procedure start')
    
                # 2024.06.03 수정
                end_dt_str = end_time.strftime('%Y-%m-%d')
                # 2024.06.03 수정
                conn = db_api.db_conn(logger, _myhost, _myport, _myuser, _mypw, _mydb)
                db_api.call_proc(logger, conn, 'SP_IF_CHAT_LOG_SUMMARY', 'PART', end_dt_str)
    
                logger.info(f'===== Summary Procedure end')

        logger.info('===== Chat log data summary end!')
        
    except Exception as e :
        error = traceback.format_exc()
        logger.error(error)
    
    filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
    logs_str = "\n".join(filter_logs_data_type)
    formatted_date = datetime.now().strftime('%Y-%m-%d')
    log_file_path = f"chatlogapi/{formatted_date}/upload_chatlog.log"
    upload_log(logger, _as_str, logs_str, file_path=log_file_path)

    return None


def upload_log(logger, _as_str, data, file_path, content_type=None):

    logger.info("###### Function Name : upload_log")
    blob_service_client = BlobServiceClient.from_connection_string(_as_str)
    container_name = "logs"
    container_client = blob_service_client.get_container_client(container_name)
    try:
        content_settings = None
        if content_type is not None:
            content_settings = ContentSettings(content_type=content_type)

        # BlobClient를 사용하여 append blob 관련 작업을 수행합니다.
        blob_client = container_client.get_blob_client(blob=file_path)

        # Append blob이 존재하지 않으면 새로 생성합니다.
        if not blob_client.exists():
            blob_client.create_append_blob(content_settings=content_settings)
            logger.info("Created new append blob.")

        # 데이터를 append blob에 추가합니다.
        blob_client.append_block(data)
        return logger.info(f"File upload completed, Path: {blob_client.url}")
    except Exception as e:
        logger.error(f"Failed to upload file: {str(e)}")
        return None

# # Logger 세팅
# def setup_logger():
#     log_folder = create_daily_log_folder()
#     log_filename = os.path.join(log_folder, 'upload_chatlog.log')
    
#     handler = logging.FileHandler(log_filename)
#     handler.setFormatter(logging.Formatter('%(asctime)s:%(message)s'))

#     logger = logging.getLogger()
#     logger.setLevel(logging.INFO)
    
#     if logger.handlers:
#         for h in logger.handlers:
#             logger.removeHandler(h)
    
#     logger.addHandler(handler)
#     logger.addHandler(logging.StreamHandler())
    
#     # azure cosmosDB package에서 발생되는 로그들 error 외에 제외
#     azure_logger = logging.getLogger('azure')
#     azure_logger.setLevel(logging.ERROR)

#     return logger

# DTC 적용 기간 여부(서머타임)
def is_dst(dt=None, tz='UTC'):
    tz = pytz.timezone(tz)
    if dt is None:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        tz_aware_dt = tz.localize(dt, is_dst=None)
    else:
        tz_aware_dt = dt.astimezone(tz)
    return tz_aware_dt.tzinfo._dst.seconds != 0

# 국가별 시간 조회
def get_local_time(dt, tz_str, chat_id=None):

    tz = pytz.timezone(tz_str)
    py_dt = dt.to_pydatetime()

    utc_dt = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, tzinfo=timezone.utc)

    #local_dt = tz.localize(dt, is_dst=None)
    local_dt = utc_dt.astimezone(tz)
    local_dt_str = local_dt.strftime('%Y-%m-%d %H:%M:%S')

    return local_dt_str

# 국가-타임존 리스트 생성
def get_country_timezone_list():
    timezone_list = []
    
    for country_cd in pytz.country_timezones:
        tmp_dic = {}
        tmp_dic['country_cd'] = country_cd
        timezones = pytz.country_timezones[country_cd]
        tmp_dic['timezone'] = timezones[0]

        timezone_list.append(tmp_dic)
    
    timezone_list.append({'country_cd':'UK', 'timezone':'Europe/London'})
    timezone_list.append({'country_cd':'DG', 'timezone':'Europe/Berlin'})
    
    return timezone_list

# Locale 정보 가져오기
def get_locale_data(logger, conn, params=None):

    logger.info('=============== Get locale data start!')

    query = f"""

        SELECT code_cd AS locale_cd
            , attribute2 AS country_cd
            , CAST(attribute5 AS UNSIGNED) AS gmt_offset
        FROM tb_code_mst
        WHERE 1=1
        AND group_cd = 'B00007'

    """

    try:

        # conn = db_api.db_conn(_myhost, _myport, _myuser, _mypw, _mydb)
        results = db_api.select(logger, conn, query, params)

        logger.info('=============== Get locale data end!')

        return results
    
    except Exception as e:
        logger.error(f"=============== Error getting locale data : {e}")
        return None

# 전체 Chat Log Raw 의 Chat id 가져오기
def get_chat_ids(logger, container, start_time, end_time):

    logger.info(f'=============== Get cosmosDB chat ids start! from : {start_time}')

    query = f"""

        SELECT 
            DISTINCT VALUE c.chatid
        FROM c
        WHERE 1=1
            AND c.timestamp >= '{start_time}'
            AND c.timestamp <= '{end_time}'
            -- AND c.event_cd NOT IN ('LANGUAGE_CHANGE', 'LIVE_CHAT_LOG')

    """

    try:

        # list 로 변환 후 리턴
        items = list(container.query_items(query, enable_cross_partition_query=True))
        
        logger.info(f'=============== Get cosmosDB chat ids({len(items)}) end!')

        return items
    
    except Exception as e:
        logger.error(f"================= Error getting cosmosDB chat ids  : {e}")
        return None
    
# Event-Intent Mapping 정보 가져오기
def get_event_intent_map(logger, conn, params=None):

    logger.info('=============== Get event-intent mapping start!')

    query = f"""

        SELECT user_action, event_type, event_cd, act_event_code, intent, intent_type
        FROM tb_event_intent_map
        WHERE 1=1

    """

    try:
        
        # conn = db_api.db_conn(_myhost, _myport, _myuser, _mypw, _mydb)
        results = db_api.select(logger, conn, query, params)

        logger.info('=============== Get event-intent mapping end!')

        return results
    
    except Exception as e:
        logger.error(f"=============== Error getting event-intent mapping : {e}")
        return None


# 전체 Chat Log Raw 데이터 가져오기
def get_chat_log_raw_data(logger, container, chat_ids):

    logger.info(f'=============== Get cosmosDB chat raw log data start!')

    query = f"""

        SELECT 
            c.chatid AS chat_id
            , c.chat_session_id
            , c.user_action
            , c.chat_role
            , c.timestamp AS log_time
            , c.chat_order
            , c.rag_order
            , c.message AS chat_msg
            , c.event_cd
            , c.act_event_code
            , c.event_type
            , c.country_cd
            , c.language_cd
            , c.platform
            , c.chat_like
            , c.intent
            , c.client_ip
            , c.tracking_country AS access_country
            , c.chat_eval.chat_rating AS chat_rating
            , c.chat_eval.chat_help_yn AS chat_help_yn
            , c.chat_eval.chat_comment AS chat_comment
        FROM c
        WHERE 1=1
            AND c.chatid in ({chat_ids})
            AND c.country_cd != '?LOCALE'
            AND c.chat_order >= 0
            -- AND c.event_cd NOT IN ('LANGUAGE_CHANGE', 'LIVE_CHAT_LOG')

    """

    # print(query)

    try:

        # list 로 변환 후 리턴
        items = list(container.query_items(query, enable_cross_partition_query=True))
        
        logger.info(f'=============== Get cosmosDB chat raw log data({len(items)}) end!')

        return items
    
    except Exception as e:
        logger.error(f"=============== Error getting cosmosDB chat raw log data : {e}")
        return None

# Chat Log Raw 데이터에 대한 후처리
def post_proc_raw_data(logger, raw_data, timezone_list):


    logger.info('=============== Post proc chat raw log data start!')

    raw_df = pd.DataFrame.from_dict(raw_data)

    logger.info('==================== Fill country & language code for null data')

    raw_df['country_cd'] = raw_df.groupby(['chat_id'])['country_cd'].fillna(method = 'ffill')
    raw_df['language_cd'] = raw_df.groupby(['chat_id'])['language_cd'].fillna(method = 'ffill')

    ## Timezone을 활용해 local time 계산 및 chat_time에 적용

    logger.info('==================== Apply chat_time as locale time ')

    timezone_df = pd.DataFrame.from_dict(timezone_list)

    raw_df = pd.merge(raw_df, timezone_df[['country_cd','timezone']].drop_duplicates(), how='left', on = 'country_cd')

    raw_df['chat_time'] = raw_df.apply(lambda x : get_local_time(pd.to_datetime(x['log_time']), x['timezone'], x['chat_id']), axis=1)

    raw_df = raw_df.sort_values(['chat_id','chat_session_id','chat_time'])
    
    '''
    logging.info('==================== Update intent ')

    ## Intent loading
    event_intent_map_df = pd.DataFrame.from_dict(event_intent_map)
    event_intent_map_df = event_intent_map_df.fillna("")
    event_intent_map_df.columns = [i.lower() for i in event_intent_map_df.columns]

    ## Intent Update
    raw_df = pd.merge(raw_df, event_intent_map_df, how = 'left', on = ['user_action','event_type','event_cd','act_event_code', 'intent'])
    #raw_df['intent_type'] = raw_df.apply(lambda x : 'FALLBACK' if x['intent']=='FIX' else x['intent_type'], axis=1)
    #raw_df['intent_type'] = raw_df.apply(lambda x : 'LIVE_CHAT_CONNECT' if x['user_action']=='live_chat_connect' else x['intent_type'], axis=1)
    #raw_df['intent_type'] = raw_df.apply(lambda x : 'CHANGE_LANGUAGE' if x['user_action']=='language_change' or x['user_action']=='languge_change' else x['intent_type'], axis=1)
    raw_df['intent_type'] = raw_df['intent_type'].fillna('UNKNOWN')

    ## Language Change의 경우 추후 Livechat에서 발생한 부분은 삭제하기 위해 동일 chat_id 내 바로 이전 event_cd를 추가
    raw_df['pre_event_cd'] = np.where((raw_df['user_action']=='language_change') | (raw_df['user_action']=='languge_change'), raw_df['event_cd'].shift(), None)

    ## Livechat 진행하면서 발생한 로그는 삭제
    raw_df = raw_df.drop(raw_df[(raw_df.event_cd == 'LIVE_CHAT_LOG') | (raw_df.pre_event_cd == 'LIVE_CHAT_LOG') | (raw_df.pre_event_cd == 'LIVE_CHAT_CONNECT')].index)
    '''

    ## DB 저장을 위한 후처리
    logger.info('==================== Make create & update info')

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    raw_df['create_date'] = now_str
    raw_df['create_user'] = 'ADMIN'
    raw_df['update_date'] = now_str
    raw_df['update_user'] = 'ADMIN'

    logger.info('==================== Refine column data type')

    raw_df['log_time'] = raw_df['log_time'].astype(str)
    raw_df = raw_df.replace(np.nan, None)

    raw_df['chat_msg'] = raw_df['chat_msg'].apply(lambda x: '<br><br>'.join(filter(None,x)) if isinstance(x, list) and len(x) > 0 else None )

    logger.info('=============== Post proc chat raw log data end!')

    return raw_df

# tmp_if_chat_log_raw 테이블 삭제/생성
def create_table_tmp_chat_log_raw(logger, conn):

    logger.info('=============== Create tmp_if_chat_log_raw table start!')

    conn = db_api.db_conn(logger, _myhost, _myport, _myuser, _mypw, _mydb)
    db_api.drop_table(logger, conn, 'tmp_if_chat_log_raw')

    conn = db_api.db_conn(logger, _myhost, _myport, _myuser, _mypw, _mydb)

    query = f"""

        CREATE TABLE tmp_if_chat_log_raw (
            my_row_id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
            CHAT_ID VARCHAR(40) NULL DEFAULT NULL,
            CHAT_SESSION_ID INT(10) NULL DEFAULT NULL,
            USER_ACTION VARCHAR(30) NULL DEFAULT NULL,
            CHAT_ROLE VARCHAR(10) NULL DEFAULT NULL,
            CHAT_TIME DATETIME NULL DEFAULT NULL,
            LOG_TIME DATETIME NULL DEFAULT NULL,
            TIMEZONE VARCHAR(30) NULL DEFAULT NULL,
            CHAT_ORDER INT(10) NULL DEFAULT NULL,
            RAG_ORDER INT(10) NULL DEFAULT NULL,
            CHAT_MSG VARCHAR(8000) NULL DEFAULT NULL,
            EVENT_CD VARCHAR(50) NULL DEFAULT NULL,
            ACT_EVENT_CODE VARCHAR(30) NULL DEFAULT NULL,
            PRE_EVENT_CD VARCHAR(30) NULL DEFAULT NULL,
            EVENT_TYPE VARCHAR(20) NULL DEFAULT NULL,
            COUNTRY_CD VARCHAR(10) NULL DEFAULT NULL,
            LANGUAGE_CD VARCHAR(30) NULL DEFAULT NULL,
            PLATFORM VARCHAR(200) NULL DEFAULT NULL,
            CHAT_LIKE VARCHAR(2) NULL DEFAULT NULL,
            INTENT VARCHAR(30) NULL DEFAULT NULL,
            INTENT_TYPE VARCHAR(50) NULL DEFAULT NULL,
            CLIENT_IP VARCHAR(50) NULL DEFAULT NULL,
            ACCESS_COUNTRY VARCHAR(10) NULL DEFAULT NULL,
            CHAT_RATING INT(10) NULL DEFAULT NULL,
            CHAT_HELP_YN CHAR(1) NULL DEFAULT NULL,
            CHAT_COMMENT TEXT NULL DEFAULT NULL,
            CREATE_DATE DATETIME NULL DEFAULT NULL,
            CREATE_USER VARCHAR(20) NULL DEFAULT NULL,
            UPDATE_DATE DATETIME NULL DEFAULT NULL,
            UPDATE_USER VARCHAR(20) NULL DEFAULT NULL,
            PRIMARY KEY (my_row_id)
        )

    """

    # conn = db_api.db_conn(_myhost, _myport, _myuser, _mypw, _mydb)
    db_api.create_table(logger, conn, query)

    logger.info('=============== Create tmp_if_chat_log_raw table end!')

# tmp_if_chat_log_raw 테이블 Insert
def insert_tmp_chat_log_raw(logger, conn, params):

    logger.info('=============== Insert tmp_if_chat_log_raw table start!')

    query = f"""

        INSERT INTO tmp_if_chat_log_raw
        (
            CHAT_ID, CHAT_SESSION_ID, USER_ACTION, CHAT_ROLE, CHAT_TIME, LOG_TIME, TIMEZONE
            , CHAT_ORDER, RAG_ORDER, CHAT_MSG, EVENT_CD, ACT_EVENT_CODE, EVENT_TYPE, COUNTRY_CD, LANGUAGE_CD
            , PLATFORM, CHAT_LIKE, INTENT, CLIENT_IP, ACCESS_COUNTRY, CHAT_RATING, CHAT_HELP_YN, CHAT_COMMENT
            , CREATE_DATE, CREATE_USER, UPDATE_DATE, UPDATE_USER
        )
        VALUES
        (
            %(chat_id)s, %(chat_session_id)s, %(user_action)s, %(chat_role)s, %(chat_time)s, %(log_time)s, %(timezone)s
            , %(chat_order)s, %(rag_order)s, %(chat_msg)s, %(event_cd)s, %(act_event_code)s, %(event_type)s, %(country_cd)s, %(language_cd)s            
            , %(platform)s, %(chat_like)s, %(intent)s, %(client_ip)s, %(access_country)s, %(chat_rating)s, %(chat_help_yn)s, %(chat_comment)s 
            , %(create_date)s, %(create_user)s, %(update_date)s, %(update_user)s 
        )          

    """

    row_cnt = 0
    # conn = db_api.db_conn(_myhost, _myport, _myuser, _mypw, _mydb)
    row_cnt = db_api.insert(logger, conn, query, params)

    res = {
            'save_cnt':row_cnt
        }
    
    logger.info('=============== Insert tmp_if_chat_log_raw table end!')

    return res


# 전체 Chat Rag Raw 데이터 가져오기
def get_chat_rag_raw_data(logger, container, chat_ids):

    logger.info(f'=============== Get cosmosDB chat RAG raw data start!')

    query = f"""

        SELECT    
            c.chatid AS chat_id,
            c.chat_session_id,
            c.rag_order,
            c.total_result.originalQuery AS query_ori,
            c.result.gpt AS gpt_answer,
            c.result.detectLanguage,
            c.result.userSelectLanguage,
            c.result.indexName,
            c.result.additionalInfo.call1PromptToken,
            c.result.additionalInfo.call1CompletionToken,
            c.result.additionalInfo.call2PromptToken,
            c.result.additionalInfo.call2CompletionToken,
            c.result.additionalInfo.time.Refinement AS ragRefinementTime,
            c.result.additionalInfo.time.ir AS ragIrTime,
            c.result.additionalInfo.time.answer AS ragAnswerTime,
            c.result.additionalInfo.time.total AS ragTotalTime,
            c.result.additionalInfo.refinedQuery,
            c.total_result.additionalInfo.contexts,
            c.total_result.contentIds[0] AS contentId1,
            c.total_result.contentIds[1] AS contentId2,
            c.total_result.contentIds[2] AS contentId3,
            c.total_result.itemIds[0] AS itemId1,
            c.total_result.itemIds[1] AS itemId2,
            c.total_result.itemIds[2] AS itemId3,
            c.total_result.youtubeIds[0] AS youtubeId1,
            c.total_result.youtubeIds[1] AS youtubeId2,
            c.total_result.youtubeIds[2] AS youtubeId3,
            c.total_result.additionalInfo.refDocScore[0] AS refDocScore1,
            c.total_result.additionalInfo.refDocScore[1] AS refDocScore2,
            c.total_result.additionalInfo.refDocScore[2] AS refDocScore3,
            c.total_result.additionalInfo.refDocUrl[0] AS refDocUrl1,
            c.total_result.additionalInfo.refDocUrl[1] AS refDocUrl2,
            c.total_result.additionalInfo.refDocUrl[2] AS refDocUrl3
        FROM c
        WHERE 1=1
            AND c.chatid in ({chat_ids})
            AND c.result.eventCd = 'RAG'

    """

    try:

        # list 로 변환 후 리턴
        items = list(container.query_items(query, enable_cross_partition_query=True))
        
        logger.info(f'=============== Get cosmosDB chat RAG raw data({len(items)}) end!')

        return items
    
    except Exception as e:
        logger.error(f"=============== Error getting cosmosDB chat RAG raw data : {e}")
        return None

# tmp_if_chat_rag_raw 테이블 삭제/생성
def create_table_tmp_chat_rag_raw(logger, conn):

    logger.info('=============== Create tmp_if_chat_rag_raw table start!')

    db_api.drop_table(logger, conn, 'tmp_if_chat_rag_raw')
    conn = db_api.db_conn(logger, _myhost, _myport, _myuser, _mypw, _mydb)

    query = f"""

        CREATE TABLE tmp_if_chat_rag_raw (
            my_row_id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
            CHAT_ID VARCHAR(40) NULL DEFAULT NULL,
            CHAT_SESSION_ID INT(10) NULL DEFAULT NULL,
            RAG_ORDER INT(10) NULL DEFAULT NULL,
            QUERY_ORI TEXT NULL DEFAULT NULL,
            GPT_ANSWER TEXT NULL DEFAULT NULL,
            DETECT_LANGUAGE VARCHAR(20) NULL DEFAULT NULL,
            USER_SELECT_LANGUAGE VARCHAR(20) NULL DEFAULT NULL,
            INDEX_NAME VARCHAR(20) NULL DEFAULT NULL,
            CALL1_PROMPT_TOKEN DECIMAL(10,5) NULL DEFAULT NULL,
            CALL1_COMPLETION_TOKEN DECIMAL(10,5) NULL DEFAULT NULL,
            CALL2_PROMPT_TOKEN DECIMAL(10,5) NULL DEFAULT NULL,
            CALL2_COMPLETION_TOKEN DECIMAL(10,5) NULL DEFAULT NULL,
            RAG_REFINEMENT_TIME DECIMAL(10,5) NULL DEFAULT NULL,
            RAG_IR_TIME DECIMAL(10,5) NULL DEFAULT NULL,
            RAG_ANSWER_TIME DECIMAL(10,5) NULL DEFAULT NULL,
            RAG_TOTAL_TIME DECIMAL(10,5) NULL DEFAULT NULL,
            REFINED_QUERY TEXT NULL DEFAULT NULL,
            CONTEXTS TEXT NULL DEFAULT NULL,
            CONTENT_ID1 VARCHAR(20) NULL DEFAULT NULL,
            CONTENT_ID2 VARCHAR(20) NULL DEFAULT NULL,
            CONTENT_ID3 VARCHAR(20) NULL DEFAULT NULL,
            ITEM_ID1 VARCHAR(20) NULL DEFAULT NULL,
            ITEM_ID2 VARCHAR(20) NULL DEFAULT NULL,
            ITEM_ID3 VARCHAR(20) NULL DEFAULT NULL,
            YOUTUBE_ID1 VARCHAR(20) NULL DEFAULT NULL,
            YOUTUBE_ID2 VARCHAR(20) NULL DEFAULT NULL,
            YOUTUBE_ID3 VARCHAR(20) NULL DEFAULT NULL,
            REF_DOC_SCORE1 TEXT NULL DEFAULT NULL,
            REF_DOC_SCORE2 TEXT NULL DEFAULT NULL,
            REF_DOC_SCORE3 TEXT NULL DEFAULT NULL,
            REF_DOC_URL1 TEXT NULL DEFAULT NULL,
            REF_DOC_URL2 TEXT NULL DEFAULT NULL,
            REF_DOC_URL3 TEXT NULL DEFAULT NULL,
            CREATE_DATE DATETIME NULL DEFAULT NULL,
            CREATE_USER VARCHAR(20) NULL DEFAULT NULL,
            UPDATE_DATE DATETIME NULL DEFAULT NULL,
            UPDATE_USER VARCHAR(20) NULL DEFAULT NULL,
            PRIMARY KEY (my_row_id)
        )

    """

    # conn = db_api.db_conn(_myhost, _myport, _myuser, _mypw, _mydb)
    db_api.create_table(logger, conn, query)

    logger.info('=============== Create tmp_if_chat_rag_raw table end!')

# tmp_if_chat_log_raw 테이블 Insert
def insert_tmp_chat_rag_raw(logger, conn, params):

    logger.info('=============== Insert tmp_if_chat_rag_raw table start!')

    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')

    query = f"""

        INSERT INTO tmp_if_chat_rag_raw
        (
            chat_id, chat_session_id, rag_order, query_ori, gpt_answer, detect_language, user_select_language, index_name
            , call1_prompt_token, call1_completion_token, call2_prompt_token, call2_completion_token, rag_refinement_time, rag_ir_time, rag_answer_time, rag_total_time
            , refined_query, content_id1, content_id2, content_id3, item_id1, item_id2, item_id3, youtube_id1, youtube_id2, youtube_id3, ref_doc_url1, ref_doc_url2, ref_doc_url3
            , ref_doc_score1, ref_doc_score2, ref_doc_score3
            , create_date, create_user, update_date, update_user
        )
        VALUES
        (
            %(chat_id)s, %(chat_session_id)s, %(rag_order)s, %(query_ori)s, %(gpt_answer)s, %(detectLanguage)s, %(userSelectLanguage)s, %(indexName)s
            , %(call1PromptToken)s, %(call1CompletionToken)s, %(call2PromptToken)s, %(call2CompletionToken)s, %(ragRefinementTime)s, %(ragIrTime)s, %(ragAnswerTime)s, %(ragTotalTime)s        
            , %(refinedQuery)s, %(contentId1)s, %(contentId2)s, %(contentId3)s, %(itemId1)s, %(itemId2)s, %(itemId3)s, %(youtubeId1)s, %(youtubeId2)s, %(youtubeId3)s, %(refDocUrl1)s, %(refDocUrl2)s, %(refDocUrl3)s 
            , %(refDocScore1)s, %(refDocScore2)s, %(refDocScore3)s 
            , %(create_date)s, %(create_user)s, %(update_date)s, %(update_user)s 
        )  

    """

    row_cnt = 0
    # conn = db_api.db_conn(_myhost, _myport, _myuser, _mypw, _mydb)
    row_cnt = db_api.insert(logger, conn, query, params)

    res = {
            'save_cnt':row_cnt
        }
    
    logger.info('=============== Insert tmp_if_chat_rag_raw table end!')

    return res

def make_tmp_chat_log_data(logger, conn, cos_cont, chat_ids, start_time=None):

    logger.info('========= Make tmp_if_chat_log_raw table start!')

    chat_ids_str = "'" + "', '".join(str(e) for e in chat_ids) + "'"

    chat_log_raw = get_chat_log_raw_data(logger, cos_cont, chat_ids_str)
    # chat_log_raw_df = pd.DataFrame.from_dict(chat_log_raw)


    timezone_list = get_country_timezone_list()

    post_chat_log_raw_df = post_proc_raw_data(logger, chat_log_raw, timezone_list)
    
    #conn = db_api.db_conn(_myhost, _myport, _myuser, _mypw, _mydb)
    create_table_tmp_chat_log_raw(logger, conn)

    conn = db_api.db_conn(logger, _myhost, _myport, _myuser, _mypw, _mydb)
    # logger.info(f"{post_chat_log_raw_df.to_dict('records')}")
    insert_tmp_chat_log_raw(logger, conn, post_chat_log_raw_df.to_dict('records'))

    logger.info('========= Make tmp_if_chat_log_raw table end!')



def make_tmp_chat_rag_data(logger, conn, cos_cont, chat_ids, start_time=None):

    logger.info('========= Make tmp_if_chat_rag_raw table start!')

    chat_ids_str = "'" + "', '".join(str(e) for e in chat_ids) + "'"

    chat_rag_raw = get_chat_rag_raw_data(logger, cos_cont, chat_ids_str)

    chat_rag_raw_df = pd.DataFrame.from_dict(chat_rag_raw)

    chat_rag_raw_df = chat_rag_raw_df.replace(np.nan, None)
    
    chat_rag_raw_df['contexts'].apply(lambda x: '//'.join(x) if isinstance(x, list) and len(x) > 0 else None )

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    chat_rag_raw_df['create_date'] = now_str
    chat_rag_raw_df['create_user'] = 'ADMIN'
    chat_rag_raw_df['update_date'] = now_str
    chat_rag_raw_df['update_user'] = 'ADMIN'

    # conn = db_api.db_conn(_myhost, _myport, _myuser, _mypw, _mydb)
    create_table_tmp_chat_rag_raw(logger, conn)

    conn = db_api.db_conn(logger, _myhost, _myport, _myuser, _mypw, _mydb)
    insert_tmp_chat_rag_raw(logger, conn, chat_rag_raw_df.to_dict('records'))

    logger.info('========= Make tmp_if_chat_rag_raw table end!')

def filter_logs_by_data_type(logs, data_type):
    filter_chat = str(data_type)
    return [log for log in logs if filter_chat in log]
