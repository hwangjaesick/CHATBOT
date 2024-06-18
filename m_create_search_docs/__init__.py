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
    from tools import clean_string, calc_stats, run_indexer, filter_logs_by_data_type, update_index_count
    from ai_search import AISearch

    korea_timezone = pytz.timezone('Asia/Seoul')
    now = datetime.now(korea_timezone)
    today_date_str = now.strftime('%Y-%m-%d')
    formatted_date = now.strftime('%Y-%m-%d')

    as_instance_log = AzureStorage(container_name='logs', storage_type='datast')
    as_instance_pre = AzureStorage(container_name='preprocessed-data', storage_type='datast')
    as_instance_doc = AzureStorage(container_name='documents', storage_type='docst')
    as_instance_web = AzureStorage(container_name='$web', storage_type='docst')
    preprocess_instance = Preprocessing()

    data_type = 'manual'
    logger = setup_logger(data_type)
    logger.info("=" * 30 + " Start Manual Preprocessing " + "=" * 30)

    data = req.get_json()
    tar_date = data['tar_date']
    tar_corp = data['tar_corp']
    
    logger.info(f"##################### tar_date : {tar_date}")
    logger.info(f"##################### tar_corp : {tar_corp}")
    
    mst_blob_paths = {
        'inspection_pdf_metadata': 'Contents_Manual_List_Mst_Data/applied_manual_related_table/inspection_pdf_metadata_v1.csv',
        'manual_list_pdf_metadata': 'Contents_Manual_List_Mst_Data/applied_manual_related_table/manual_list_pdf_metadata_v1.csv',
        'cover_title': 'Contents_Manual_List_Mst_Data/applied_manual_related_table/cover_title_list_v1.csv',
        'section_group_mapping': 'Contents_Manual_List_Mst_Data/applied_manual_related_table/product_group_mapping_v1.csv',
        'prod_mst': 'Contents_Manual_List_Mst_Data/PRODUCT_MST.csv'
    }

    DATE_STR = now.strftime("%Y%m%d") # 수행 일자
    # DATE_STR = "20240503" # 임시 
    TEMP_MODIFY = False # [2024.04.24] 현재 LGEPL 법인으로 관리되는 데이터(LGECZ, LGESK)들을 반영하기 위해 사용. LGECZ 및 LGESK가 개별로 관리하게 되면 False로 변경해야함.

    try:
        #==================================================
        # 구조화 및 html 생성
        #==================================================
        # -- 대상 : 금일 업데이트된 섹션 정보 -> 구조화, html 생성
        query = f"""
            SELECT 
                *
            FROM tb_chat_manual_sect
            WHERE 1=1
                AND DATE_FORMAT(UPDATE_DATE, '%Y%m%d') = '{tar_date}'
                AND CORP_CD = '{tar_corp}'
            ;
        """
        # query = f"""
        #     SELECT 
        #         *
        #     FROM tb_chat_manual_sect
        #     WHERE 1=1
        #         AND ( DATE_FORMAT(CREATE_DATE, '%Y%m%d') = '20240503' 
        #             OR DATE_FORMAT(CREATE_DATE, '%Y%m%d') = '20240502'
        #         )
        #         AND CORP_CD = 'LGEDG'
        #         AND ITEM_ID = '20150190003565'
        #         AND PROD_GROUP_CD = 'COK'
        #         AND PROD_CD = 'MWO'
        #         AND LANGUAGE_NAME = 'German'
        #     ;
        # """
        sql_instance = MySql()
        target_chat_manual_sect  = sql_instance.load_table_as_df(query)

        logger.info(f"###### Num of data = {len(target_chat_manual_sect)}")
        if len(target_chat_manual_sect) == 0:
            return func.HttpResponse("실행 완료 - documents 생성 대상 없음")
        
        sql_instance = MySql()
        subs_info = sql_instance.get_subs_info(TEMP_MODIFY)

        # [2024.05.03] preprocess_instance.structured_and_html함수로 이동.
        # -- 수행 전 기존에 적재된 결과들 제거
        # 삭제 대상 BLOB Container :documents, $web
        # logger.info(f"###### delete prior documents and html")
        # del_infos = target_chat_manual_sect[['ITEM_ID', 'CORP_CD', 'PROD_GROUP_CD', 'PROD_CD', 'LANGUAGE_NAME']].drop_duplicates()
        # for del_idx in del_infos.index:
        #     del_x  = del_infos.loc[del_idx].to_dict()
        #     del_df = pd.DataFrame([del_x]).merge(subs_info, how='left', on = ['CORP_CD', 'LANGUAGE_NAME'])
            
        #     for ii in del_df.index:
        #         xx = del_df.loc[ii]
        #         # 1. documents내 파일 제거
        #         del_json_path = f"data/{xx['ISO_CD']}/{xx['LANGUAGE_NAME']}/manual/{xx['PROD_GROUP_CD']}/{xx['PROD_CD']}/{xx['ITEM_ID']}"
        #         logger.info(f"###### delete documents starts with {del_json_path}")
        #         as_instance_doc.delete_blobs(name_starts_with=del_json_path, backup='N')
        #         # 2. $web내 파일 제거
        #         del_html_path = f"data/{xx['ISO_CD']}/{xx['LANGUAGE_NAME']}/{xx['PROD_GROUP_CD']}/{xx['PROD_CD']}/{xx['ITEM_ID']}"
        #         logger.info(f"###### delete html and images starts with {del_html_path}")
        #         as_instance_web.delete_blobs(name_starts_with=del_html_path, backup='N')
        # logger.info(f"###### Complete - delete prior documents and html")

        # -- Search용 문서 생성
        indexer_list = preprocess_instance.structured_and_html(logger, target_chat_manual_sect, temp_modify=TEMP_MODIFY)
        
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
        log_file_path = f"prcapi/batch/error_log/{formatted_date}/{data_type}.log"
        as_instance_log.upload_file(logs_str, file_path=log_file_path, overwrite=True)      
        return func.HttpResponse(f"에러 발생 - {str(e)}")
    
    return func.HttpResponse("실행 완료")