import logging
import json
import azure.functions as func
import pycountry
import credential
import time
import pytz
from datetime import datetime, timezone
import pandas as pd

def main(req: func.HttpRequest) -> func.HttpResponse:

    from database import AzureStorage, MySql
    from log_others import setup_logger, thread_local
    logger = setup_logger()

    as_instance_log = AzureStorage(container_name='logs', storage_type='datast')
    if not credential.keyvault_values:
        credential.fetch_keyvault_values()
    
    from model import Translator

    logging.info("Start User Question Translate")

    sql_instance = MySql()
    LANGUAGE_MST = sql_instance.get_table("""
                                        select
                                        CODE_CD, CODE_NAME
                                        from
                                        tb_code_mst
                                        where
                                        group_cd = "B00006"
                                        """)
    LANGUAGE_MST = pd.DataFrame(LANGUAGE_MST)
    LANGUAGE_MST = LANGUAGE_MST.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    # Receive input
    data = req.get_json()
    logger.info(f"######### input data : {json.dumps(data,indent=4,ensure_ascii=False)}")
    language_code_from = data['languageFrom']
    language_code_to = data['languageTo']
    texts = data['userQuestion']
    
    from tools import Authorization, AuthorizationError
    from credential import keyvault_values
    auth_header = req.headers.get('Authorization')
    auth = Authorization(value=keyvault_values['Authorization'], key=auth_header)

    if auth != "DEV" :
            
        logger.error(f"####### Authorizaion Error : {auth} != DEV")
        raise AuthorizationError(f"####### Authorizaion Error : {auth} != DEV")

    try :

        # Translate User Question
        translator_instance = Translator()
        if language_code_from == 'no':
            language_code_from = 'nb'
        if language_code_to == 'no':
            language_code_to = 'nb'
        if language_code_from is not None :
            languageFrom = LANGUAGE_MST[LANGUAGE_MST['CODE_CD'] == language_code_from]['CODE_NAME']
            languageTo = LANGUAGE_MST[LANGUAGE_MST['CODE_CD'] == language_code_to]['CODE_NAME']
            response = translator_instance.translate_multi_question(language_from=language_code_from, language_to=language_code_to, texts=texts)
            result = {'input' : data,
                        'output' :{'languageCodeFrom': language_code_from,
                                    'languageCodeTo': language_code_to,
                                    'languageFrom': "" if len(languageFrom) == 0 else languageFrom.iloc[0],
                                    'languageTo': "" if len(languageFrom) == 0 else languageTo.iloc[0],
                                    'beforeTranslate' : [item for item in texts],
                                    'afterTranslate': [item['translations'][0]['text'] for item in response]}}
        else :
            response = translator_instance.translate_multi_question(language_to=language_code_to, texts=texts)
            languageTo = LANGUAGE_MST[LANGUAGE_MST['CODE_CD'] == language_code_to]['CODE_NAME']
            languageFrom = [LANGUAGE_MST[LANGUAGE_MST['CODE_CD'] == item['detectedLanguage']['language']]['CODE_NAME'].iloc[0] for item in response]
            result = {'input' : data,
            'output' :{'languageCodeFrom': [item['detectedLanguage']['language'] for item in response],
                        'languageCodeTo': language_code_to,
                        'languageFrom': ["" if len(item) == 0 else item for item in languageFrom],
                        'languageTo': "" if len(languageFrom) == 0 else languageTo.iloc[0],
                        'beforeTranslate' : [item for item in texts],
                        'afterTranslate': [item['translations'][0]['text'] for item in response]}}

        output = json.dumps(result, ensure_ascii=False)
        logger.info(f"######### output data : {json.dumps(result,indent=4,ensure_ascii=False)}")
        korea_timezone = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_timezone)
        formatted_date = now.strftime('%Y%m%d')
        formatted_datetime = now.strftime('%Y%m%d_%H%M')
        logs_str = "\n".join(thread_local.log_list)
        log_file_path = f"translateapi/log/{formatted_date}/{formatted_datetime}.log"
        as_instance_log.upload_log(logs_str, file_path=log_file_path)
    
    except Exception as e :

        languageTo = LANGUAGE_MST[LANGUAGE_MST['CODE_CD'] == language_code_to]['CODE_NAME']
        languageFrom = LANGUAGE_MST[LANGUAGE_MST['CODE_CD'] == language_code_from]['CODE_NAME']
        result = {'input' : data,
        'output' :{'languageCodeFrom': language_code_from,
                            'languageCodeTo': language_code_to,
                            'languageFrom': "",
                            'languageTo': "",
                            'beforeTranslate' : [item for item in texts],
                            'afterTranslate': ["","",""] }}
        output = json.dumps(result, ensure_ascii=False)
        logger.error(str(e))
        korea_timezone = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_timezone)
        formatted_date = now.strftime('%Y%m%d')
        formatted_datetime = now.strftime('%Y%m%d_%H%M')
        logs_str = "\n".join(thread_local.log_list)
        log_file_path = f"translateapi/error_log/{formatted_date}/{formatted_datetime}.log"
        as_instance_log.upload_log(logs_str, file_path=log_file_path)


    return func.HttpResponse(body=output,
                    mimetype="application/json",
                    status_code=200)    
    
