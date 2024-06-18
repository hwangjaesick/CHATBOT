import logging
import json
import azure.functions as func
import pycountry
import credential
import time
import pytz
from datetime import datetime, timezone

def main(req: func.HttpRequest) -> func.HttpResponse:

    from database import AzureStorage
    from log_others import setup_logger, thread_local
    logger = setup_logger()

    as_instance_log = AzureStorage(container_name='logs', storage_type='datast')
    if not credential.keyvault_values:
        credential.fetch_keyvault_values()
    
    from database import MySql, AzureStorage
    from model import Translator

    logging.info("Start User Language Detection")


    # Receive input
    data = req.get_json()

    from tools import Authorization, AuthorizationError
    from credential import keyvault_values
    auth_header = req.headers.get('Authorization')
    auth = Authorization(value=keyvault_values['Authorization'], key=auth_header)

    if auth != "DEV" :
            
        logger.error(f"####### Authorizaion Error : {auth} != DEV")
        raise AuthorizationError(f"####### Authorizaion Error : {auth} != DEV")

    try :
        
        logger.info(f"######### input data : {json.dumps(data,indent=4,ensure_ascii=False)}")
        body = [{'text': data['userQuestion'] }]
        logger.info(f"######### input data : {data}")

        # Get Master Table ( Language )
        sql_instance = MySql()
        language_mst = sql_instance.get_table(  """
                                                SELECT m.CODE_CD,
                                                    m.CODE_NAME
                                                FROM tb_code_mst m,
                                                    tb_group_code_mst g
                                                WHERE 1=1
                                                    AND m.group_cd = g.group_cd
                                                    AND g.GROUP_NAME  = 'Language'
                                                """ )
        
        # Detect language
        translator_instance = Translator()
        response = translator_instance.detect_language(body=body)
        logger.info(f"{response}")
        language_code = response[0]['language'].split('-')[0]

        if language_code == 'nb':
            language_code = 'no'
        
        language = [item['CODE_NAME'] for item in language_mst if item['CODE_CD'] == language_code]
        
        # Make Result
        result = {'question' : data['userQuestion'],
                'languageCode': language_code,
                'language': language[0],
                'score': response[0]['score']}

        output = json.dumps(result, ensure_ascii=False)
        logger.info(f"######### output data : {json.dumps(result,indent=4,ensure_ascii=False)}")
        korea_timezone = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_timezone)
        formatted_date = now.strftime('%Y%m%d')
        formatted_datetime = now.strftime('%Y%m%d_%H%M')
        logs_str = "\n".join(thread_local.log_list)
        log_file_path = f"detectapi/log/{formatted_date}/{formatted_datetime}.log"
        as_instance_log.upload_log(logs_str, file_path=log_file_path)
    
    except Exception as e :
        logger.error(str(e))
        # Make Result
        result = {'question' : data['userQuestion'],
                'languageCode': "",
                'language': "",
                'score': ""}
        
        korea_timezone = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_timezone)
        formatted_date = now.strftime('%Y%m%d')
        formatted_datetime = now.strftime('%Y%m%d_%H%M')
        logs_str = "\n".join(thread_local.log_list)
        log_file_path = f"detectapi/error_log/{formatted_date}/{formatted_datetime}.log"
        as_instance_log.upload_log(logs_str, file_path=log_file_path)

    return func.HttpResponse(body=output,
                    mimetype="application/json",
                    status_code=200)    
