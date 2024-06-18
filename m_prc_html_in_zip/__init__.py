import json  
import logging  
import azure.functions as func
import credential
import pandas as pd
import os
from datetime import datetime
import pytz

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    if not credential.keyvault_values:
        credential.fetch_keyvault_values()

    from preprocessing import Preprocessing
    from database import AzureStorage, MySql
    from log import setup_logger, thread_local
    from tools import clean_string, calc_stats, run_indexer, filter_logs_by_data_type
    from ai_search import AISearch
    
    korea_timezone = pytz.timezone('Asia/Seoul')
    now = datetime.now(korea_timezone)
    today_date_str = now.strftime('%Y-%m-%d')
    formatted_date = now.strftime('%Y-%m-%d')
    
    as_instance_log = AzureStorage(container_name='logs', storage_type='datast')
    as_instance_raw = AzureStorage(container_name='raw-data', storage_type='datast')
    preprocess_instance = Preprocessing()
    
    data_type = 'manual'
    logger = setup_logger(data_type)
    logger.info("=" * 30 + " Start Manual Preprocessing " + "=" * 30)
    
    mst_blob_paths = {
        'inspection_pdf_metadata': 'Contents_Manual_List_Mst_Data/applied_manual_related_table/inspection_pdf_metadata_v1.csv',
        'manual_list_pdf_metadata': 'Contents_Manual_List_Mst_Data/applied_manual_related_table/manual_list_pdf_metadata_v1.csv',
        'cover_title': 'Contents_Manual_List_Mst_Data/applied_manual_related_table/cover_title_list_v1.csv',
        'section_group_mapping': 'Contents_Manual_List_Mst_Data/applied_manual_related_table/product_group_mapping_v1.csv',
        'prod_mst': 'Contents_Manual_List_Mst_Data/PRODUCT_MST.csv'
    }
    
    # DATE_STR = now.strftime("%Y%m%d") # 수행 일자
    
    data = req.get_json()
    DATE_STR = data['date_str']
    
    logger.info(f"##################### date_str : {DATE_STR}")

    # DATE_STR = "20240603" # 폴더명에 맞춰서
    UNZIP_FOLDER_NAME = "manual_unzip" # [2024.04.30] 수정
    TEMP_MODIFY = False # [2024.04.24] 현재 LGEPL 법인으로 관리되는 데이터(LGECZ, LGESK)들을 반영하기 위해 사용. LGECZ 및 LGESK가 개별로 관리하게 되면 False로 변경해야함.
    
    TARGET_FOLDER_MAPP = {
        "LGEBN" : ['fr-FR', 'nl-NL']
        , "LGEDG" : ['de-DE', 'fr-FR']
        , "LGEES" : ['es-ES']
        , "LGEFS" : ['fr-FR']
        , "LGEHS" : ['el-GR']
        , "LGEIS" : ['it-IT']
        , "LGELA" : ['et-EE', 'lv-LV', 'lt-LT']
        , "LGEMK" : ['bg-BG', 'hr-HR', 'hu-HU', 'sr-RS']
        # , "LGESK" : ['sk-SK'] # 얘네 둘이 현재는 PL인데 원래는 CZ..
        , "LGECZ" : ['cs-CZ', 'sk-SK'] # 얘네 둘이 현재는 PL인데 원래는 CZ..
        , "LGEPT" : ['pt-PT']
        , "LGERO" : ['ro-RO']
        , "LGESW" : ['da-DK', 'fi-FI', 'no-NO', 'sv-SE']
        , "LGEUK" : ['en-US']
    }
    if TEMP_MODIFY :
        TARGET_FOLDER_MAPP["LGEPL"] = ['pl-PL', 'sk-SK', 'cs-CZ']
    else :
        TARGET_FOLDER_MAPP["LGEPL"] = ['pl-PL']    

    try :
        #==================================================
        # html 파일 처리 (운영 리소스 적재)
        #==================================================
        # -- csv 파일 load
        html_csv_bname = f"{UNZIP_FOLDER_NAME}/TMP_CSV/{DATE_STR}/html_file_list.csv"
        html_files_df = as_instance_raw.read_file(html_csv_bname)
        
        if len(html_files_df) == 0:
            return func.HttpResponse("실행 완료 - HTML 정보 없음")

        # -- 챕터별 html 취합 및 section 텍스트 추출
        preprocess_instance.prc_html_and_section(logger, html_files_df, TARGET_FOLDER_MAPP, temp_modify=TEMP_MODIFY)
        
        filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
        logs_str = "\n".join(filter_logs_data_type)
        log_file_path = f"prcapi/log/{formatted_date}/{data_type}.log"
        as_instance_log.upload_file(logs_str, file_path=log_file_path, overwrite=True)
        
    except Exception as e:
        logger.error(str(e))
        error_contents = f"{type(e).__name__} : '{e}'"
        logger.error(error_contents)
        filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
        logs_str = "\n".join(filter_logs_data_type)
        log_file_path = f"prcapi/error_log/{formatted_date}/{data_type}.log"
        as_instance_log.upload_file(logs_str, file_path=log_file_path, overwrite=True)
        return func.HttpResponse(f"에러 발생 - {str(e)}")
    
    return func.HttpResponse("실행 완료")