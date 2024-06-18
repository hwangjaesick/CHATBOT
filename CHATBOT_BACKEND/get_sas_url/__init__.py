import logging
import json
import azure.functions as func
import credential
import time
import pytz
from datetime import datetime, timezone
import urllib.parse

def main(req: func.HttpRequest) -> func.HttpResponse:

    from database import AzureStorage
    from tools import extract_data
    from log_others import setup_logger, thread_local
    as_instance_log = AzureStorage(container_name='logs', storage_type='datast')
    logger = setup_logger()

    if not credential.keyvault_values:
        credential.fetch_keyvault_values()

    logger.info("Start Get Sas Url")
    data = req.get_json()
    logger.info(f"######### input data : {json.dumps(data,indent=4,ensure_ascii=False)}")

    if data['expiryTime'] == "True":
        expiryTime = True
    else :
        expiryTime = False  
    
    from tools import Authorization, AuthorizationError
    from credential import keyvault_values
    auth_header = req.headers.get('Authorization')
    auth = Authorization(value=keyvault_values['Authorization'], key=auth_header)

    if auth != "DEV" :
            
        logger.error(f"####### Authorizaion Error : {auth} != DEV")
        raise AuthorizationError(f"####### Authorizaion Error : {auth} != DEV")

    try :
        
        as_instance = AzureStorage(container_name=data['containerName'], storage_type=data['storageType'])
        sas_url = as_instance.get_sas_url_container(expiry_time=expiryTime)

        st = sas_url.split(data['containerName'])[1].split('&')[0]
        se = sas_url.split(data['containerName'])[1].split('&')[1]

        st_time = extract_data(st, start='st=')[0]
        se_time = extract_data(se, start='se=')[0]

        decoded_timestamp = urllib.parse.unquote(st_time)
        dt_object = datetime.strptime(decoded_timestamp, "%Y-%m-%dT%H:%M:%SZ")
        utc_dt = pytz.utc.localize(dt_object)
        korea_dt = utc_dt.astimezone(pytz.timezone("Asia/Seoul"))
        formatted_time_st = korea_dt.strftime("%Y-%m-%d %H:%M:%S")

        decoded_timestamp = urllib.parse.unquote(se_time)
        dt_object = datetime.strptime(decoded_timestamp, "%Y-%m-%dT%H:%M:%SZ")
        utc_dt = pytz.utc.localize(dt_object)
        korea_dt = utc_dt.astimezone(pytz.timezone("Asia/Seoul"))
        formatted_time_se = korea_dt.strftime("%Y-%m-%d %H:%M:%S")

        result = {"sas_url" : sas_url,
                  "start_time" : formatted_time_st,
                  "end_time" : formatted_time_se }

        
        output = json.dumps(result, ensure_ascii=False)
        logger.info(f"######### output data : {json.dumps(result,indent=4,ensure_ascii=False)}")
        korea_timezone = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_timezone)
        formatted_date = now.strftime('%Y%m%d')
        formatted_datetime = now.strftime('%Y%m%d_%H%M')
        logs_str = "\n".join(thread_local.log_list)
        log_file_path = f"sasurlapi/log/{formatted_date}/{formatted_datetime}={data['containerName']}.log"
        as_instance_log.upload_log(logs_str, file_path=log_file_path)
    
    except Exception as e :

        output = json.dumps(result, ensure_ascii=False)
        logger.error(str(e))
        korea_timezone = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_timezone)
        formatted_date = now.strftime('%Y%m%d')
        formatted_datetime = now.strftime('%Y%m%d_%H%M')
        logs_str = "\n".join(thread_local.log_list)
        log_file_path = f"sasurlapi/error_log/{formatted_date}/{formatted_datetime}-{data['containerName']}.log"
        as_instance_log.upload_log(logs_str, file_path=log_file_path)


    return func.HttpResponse(body=output,
                    mimetype="application/json",
                    status_code=200)    
    
