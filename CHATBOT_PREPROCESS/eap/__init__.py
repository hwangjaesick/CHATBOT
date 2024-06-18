import azure.functions as func
import requests
from datetime import datetime, timedelta
import pytz

import pandas as pd
import json
import logging
import os
import traceback
import time

def main(mytimer: func.TimerRequest) -> None:

    from log import setup_logger, thread_local
    from database import MySql, AzureStorage

    formatted_date = datetime.now().strftime('%Y-%m-%d')
    api_type = 'eap'
    data_type = formatted_date +'-'+ api_type

    logger = setup_logger(data_type)
    logger.info('===== Chat log data summary start!')
    as_instance_log = AzureStorage(container_name='logs', storage_type='datast')

    try :

        sql_instajce = MySql()
        corp_mst = sql_instajce.get_table(query = """
                                        SELECT * FROM TB_CODE_MST
                                        WHERE GROUP_CD = 'B00008' AND USE_YN = 'Y';
                                        """)
        corp_mst = pd.DataFrame(corp_mst)
        corp_mst = corp_mst.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        corp_cd_list = corp_mst['ATTRIBUTE1'].unique().tolist()

        start_utc_now = datetime.now(pytz.utc)
        input_timestamp_utc = start_utc_now.strftime("%Y-%m-%dT%H:%M:%S")

        start_creation_date_utc = (start_utc_now-timedelta(days=1)).strftime("%Y%m%d")
        end_creation_date_utc = start_utc_now.strftime("%Y%m%d")

        logger.info(f"input : {input_timestamp_utc}")
        logger.info(f"start : {start_creation_date_utc}")
        logger.info(f"end : {end_creation_date_utc}")
        
        url = {url}

        for corp_cd in corp_cd_list:

            parameters = (input_timestamp_utc, corp_cd, start_creation_date_utc, end_creation_date_utc)

            logger.info(f"############## CORP_CD : {corp_cd}")
            logger.info(f"############## TIMESTAMP : {input_timestamp_utc}")
            logger.info(f"############## START : {start_creation_date_utc}")
            logger.info(f"############## END : {end_creation_date_utc}")

            query = """
                    select D.EAP_CORP_CD 												AS corporation_code
                        , D.CREATION_DATE 												AS creation_date
                        , 'C05' 														AS contact_channel_code
                        , 'Chatbot'														AS contact_channel_name
                        , 'Chatbot'														AS system_name
                        , 'Chatbot'														AS input_user_id
                        , %s					                                        AS input_timestamp
                        -- , CAST(D.TOTAL_COUNT AS DOUBLE) 								AS total_count -- 주석 컬럼
                        -- , CAST(D.TOTAL_EXCLUDE_SESSION AS DOUBLE) 						AS total_exclude_session -- 주석 컬럼
                        -- , CAST(D.LIVECHAT_SESSION AS DOUBLE) 							AS livechat_session -- 주석 컬럼
                        , CAST(D.TOTAL_EXCLUDE_SESSION - D.LIVECHAT_SESSION AS DOUBLE) 	AS inbound_count
                        -- , NULL					                						AS outbound_count
                        , CAST(D.TOTAL_EXCLUDE_SESSION - D.LIVECHAT_SESSION AS DOUBLE) 	AS answered_count
                        -- , NULL								                			AS sum_answer_speed
                        , CAST(D.SUM_TALK_TIME AS DOUBLE) 								AS sum_talk_time
                        -- , CAST(D.SUM_TALK_TIME / D.TOTAL_COUNT AS DOUBLE) 				AS avg_talk_time -- 주석 컬럼
                        -- , NULL									                		AS sum_call_work
                        -- , NULL									                		AS sum_hold_time
                        , CAST(D.SURVEY_SOLVED_SESSION AS DOUBLE) 						AS sum_q1_answer
                        , CAST(D.SURVEY_SESSION AS DOUBLE) 								AS sum_q1_response
                        -- , CAST(D.SURVEY_SOLVED_SESSION / D.SURVEY_SESSION AS DOUBLE) 	AS counseling_complet_rate -- 주석 컬럼
                        , CAST(D.SUM_CHAT_RATING AS DOUBLE) 							AS sum_q2_answer
                        , CAST(D.SURVEY_SESSION2 AS DOUBLE) 							AS sum_q2_response
                        -- , CAST(D.SUM_CHAT_RATING / D.SURVEY_SESSION2 AS DOUBLE) 		AS satisfaction_score -- 주석 컬럼
                        -- , NULL              											AS sum_waiting_time
                        -- , NULL			                								AS repeat_call_count
                        -- , NULL							                				AS last_agent_conn_count
                        -- , NULL					                						AS Ivr_call_count
                        -- , NULL								                			AS answered_20s_count
                        -- , NULL						                					AS auto_unmanned_ivr_count
                        -- , NULL								                			AS agent_login_count
                        -- , NULL							                				AS agent_inbound_count
                        -- , NULL								                			AS agent_outbound_count
                        -- , NULL								                			AS agent_inbound_outbound_count
                        -- , NULL								                			AS agent_inbound_lt_5call_count
                    from (
                        select C.EAP_CORP_CD
                            , C.CREATION_DATE
                            , COUNT(C.CHAT_ID) AS TOTAL_COUNT
                            , SUM(CASE WHEN C.TYPE_SEESION_YN = 'Y' THEN 1 ELSE 0 END) AS TOTAL_EXCLUDE_SESSION
                            , SUM(CASE WHEN C.TYPE_LIVECHAT_YN = 'Y' THEN 1 ELSE 0 END) AS LIVECHAT_SESSION
                            , SUM(C.CHAT_TIME) AS SUM_TALK_TIME
                            , SUM(CASE WHEN C.TYPE_SURVEY_YN = 'Y' THEN 1 ELSE 0 END) AS SURVEY_SESSION
                            , SUM(CASE WHEN C.TYPE_SOLVED_YN = 'Y' THEN 1 ELSE 0 END) AS SURVEY_SOLVED_SESSION
                            , SUM(C.CHAT_RATING) AS SUM_CHAT_RATING
                            , SUM(CASE WHEN C.CHAT_RATING > 0 THEN 1 ELSE 0 END) AS SURVEY_SESSION2
                        from (
                            select A.*, B.ATTRIBUTE1 as EAP_CORP_CD, DATE_FORMAT(A.CHAT_DT, '%Y%m%d') as CREATION_DATE
                            from tb_chat_session_info A
                            left join tb_code_mst B
                            on 1=1
                                and B.GROUP_CD = 'B00008'
                                and A.COUNTRY_CD = B.CODE_CD
                        ) C
                        group by C.EAP_CORP_CD, C.CREATION_DATE
                        having 1=1
                        -- and C.EAP_CORP_CD IS NOT NULL -- 전체 조회 조건
                        and C.EAP_CORP_CD =  %s -- 조회조건
                        and C.CREATION_DATE >= %s AND C.CREATION_DATE < %s -- 조회조건
                    ) D
                    order by 1, 2;
                    """
            
            sql_instajce = MySql()
            df = sql_instajce.get_table(query=query, parameters=parameters)
            df = pd.DataFrame(df)
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            logger.info(f"############## LEN : {len(df)}")
            
            MAX_RETRIES = 5

            for index, item in df.iterrows():

                data = json.loads(item.to_json())
                body = {"auth_code": "S10",
                    "auth_key": {auth_key},
                    "data": data
                    }
                
                logger.info(json.dumps(body, indent=4))
                
                headers = {
                'Content-Type': 'application/json',
                }

                for attempt in range(MAX_RETRIES):
                    response = requests.post(url, json=body, headers=headers)

                    if response.status_code == 200:
                        logger.info(f'Success : {response.json()}')
                        break
                    else:
                        logger.error(f'Attempt {attempt + 1} Failed : {response.status_code}, {response.text}')
                        if attempt < MAX_RETRIES - 1:
                            logger.info('Retrying...')
                        else:
                            logger.error('Max retries reached. Moving to next item.')
 
    except Exception as e :
        error = traceback.format_exc()
        logger.error(error)

    filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
    logs_str = "\n".join(filter_logs_data_type)

    formatted_date = datetime.now().strftime('%Y-%m-%d')
    log_file_path = f"eapapi/{formatted_date}/eap.log"
    as_instance_log.upload_log(logs_str, file_path=log_file_path)

    return None

def filter_logs_by_data_type(logs, data_type):
    filter_chat = str(data_type)
    return [log for log in logs if filter_chat in log]
