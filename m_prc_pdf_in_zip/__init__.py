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
    as_instance_pre = AzureStorage(container_name='preprocessed-data', storage_type='datast')
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

    try :
        #==================================================
        # zip파일내 pdf 파일 검수 결과 처리
        #==================================================
        # -- csv 파일 load
        csv_bname = f"{UNZIP_FOLDER_NAME}/TMP_CSV/{DATE_STR}/pdf_file_list.csv" # 검수 결과
        ins_pdf_df = as_instance_raw.read_file(csv_bname)
        
        ins_pdf_df = ins_pdf_df.fillna(value={"USE_YN":"", "SPLIT_YN":"", "FIN_LANGUAGE":"", "PAGE_RANGE":""})
        ins_pdf_df["ITEM_ID"] = [x.split("/")[-1].split(".")[0] for x in ins_pdf_df["ORG_BLOB_NAME"]] # item_id 값이 변형되어 저장되어 해당 오류 방지
        logger.info(f"###### target datat :\n {ins_pdf_df.head()}")
        # -- use_yn = "Y"인 PDF 병합 및 Blob 적재
        y_ins_pdf_df = ins_pdf_df[ins_pdf_df['USE_YN'] == "Y"].reset_index(drop=True)
        if len(y_ins_pdf_df) == 0 :
            return func.HttpResponse("실행 완료 - USE_YN == Y인 데이터 없음")

        res_df = preprocess_instance.inspect_pdf(logger, y_ins_pdf_df)
        
        # 섹션 분할 대상 : merge 성공적으로 수행된 경우
        run_df = res_df[res_df['MERGE_STATUS'] == "Success"].reset_index(drop=True)
        
        logger.info(f"###### num of merged pdf = {len(run_df)}")
        if len(run_df) == 0:
            return func.HttpResponse("실행 완료 - merge가 성공적으로 수행된 내역 없음")
        
        #==================================================
        # 병합한 PDF -> Section 분할
        #==================================================
        # -- 섹션 분할 수행 전 기존에 적재된 결과들 제거
        # 테이블 TB_CAHT_MANUAL_SECT의 key 정보 기준으로 blob 삭제(대상 containder:preprocessed-data) 후 table 내용 삭제                
        for idx in run_df.index :
            idx_x = run_df.loc[idx].to_dict()
            select_query = f"""
                SELECT 
                    *
                FROM tb_chat_manual_sect
                WHERE 1=1
                    AND ITEM_ID = '{idx_x['ITEM_ID']}'
                    AND CORP_CD = '{idx_x['CORP_CD']}'
                    AND PROD_GROUP_CD = '{idx_x['PROD_GROUP_CD']}'
                    AND PROD_CD = '{idx_x['PROD_CD']}'
                ;
            """
            sql_instance = MySql()
            sect_infos = sql_instance.load_table_as_df(select_query)
            
            # [2024.05.02] 섹션 분할 수행 대상 중 이미 수행한 내역이 존재하는 경우, 삭제 skip
            sect_infos['TMP_UPDATE_DATE'] = sect_infos['UPDATE_DATE'].apply(lambda x : x.strftime("%Y%m%d"))
            sect_infos['TMP_CREATE_DATE'] = sect_infos['CREATE_DATE'].apply(lambda x : x.strftime("%Y%m%d"))
            if len(sect_infos) ==0 :
                run_df.loc[idx, "TODAY_RUN_YN"] = "N"
            elif len(sect_infos[(sect_infos['TMP_UPDATE_DATE']==DATE_STR) | (sect_infos['TMP_CREATE_DATE']==DATE_STR)]) == len(sect_infos) :
                run_df.loc[idx, "TODAY_RUN_YN"] = "Y"
                continue
            else :
                run_df.loc[idx, "TODAY_RUN_YN"] = "N"
            # run_df.loc[idx, "TODAY_RUN_YN"] = "N"

            del_infos = sect_infos[['ITEM_ID', 'CORP_CD', 'PROD_GROUP_CD', 'PROD_CD', 'LANGUAGE_NAME', 'BLOB_PATH']].drop_duplicates()
            # 1. blob 삭제 : 섹션 분할 결과
            for del_idx in del_infos.index:
                as_instance_pre.delete_blobs(name_starts_with=del_infos.loc[del_idx]['BLOB_PATH'], backup='Y')
            
            # 2. 테이블 정보 삭제
            delete_query = f"""
                DELETE FROM tb_chat_manual_sect
                WHERE 1=1
                    AND ITEM_ID = '{idx_x['ITEM_ID']}'
                    AND CORP_CD = '{idx_x['CORP_CD']}'
                    AND PROD_GROUP_CD = '{idx_x['PROD_GROUP_CD']}'
                    AND PROD_CD = '{idx_x['PROD_CD']}'
                ;
            """
            logger.info(f"###### delete_query = {delete_query}")
            sql_instance = MySql()
            sql_instance.commit_query(delete_query)
        # end for-loop : run_df.index
        logger.info(f"###### Delete prior section results")
        logger.info(f"###### running yn \n{run_df['TODAY_RUN_YN'].value_counts()}")
        
        # -- 섹션 분할 수행
        section_groups = as_instance_raw.read_file(mst_blob_paths['section_group_mapping'])
        logging_aoai = []
        for idx in run_df.index :
            if run_df.loc[idx, "TODAY_RUN_YN"] == 'Y':
                continue
            
            idx_x = run_df.loc[idx].to_dict()
            idx_x['LANGUAGE_NAME'] = idx_x['FIN_LANGUAGE']
            res_dict, status, status_des, res_aoai = preprocess_instance.run_split_section(logger, idx_x, aoai_res="m1", section_groups=section_groups)
            logger.info(f"###### section split - status : {status}\nstatust_des : {status_des}\n{res_aoai}")
            # -- 섹션분할 프로세스 상태 업데이트, tb_chat_manual_prc
            # zip 파일 처리 -> section_yn도 업데이트
            logging_aoai.extend(res_aoai)
            update_query = f"""
                UPDATE tb_chat_manual_prc
                SET SECTION_YN = 'Y'
                    , SECTION_STATUS = '{status}'
                    , SECTION_STATUS_DES = '{status_des}'
                    , UPDATE_DATE = '{datetime.now(korea_timezone).replace(tzinfo = None)}'
                WHERE 1=1
                    AND ITEM_ID = '{idx_x['ITEM_ID']}'
                    AND CORP_CD = '{idx_x['CORP_CD']}'
                    AND PROD_GROUP_CD = '{idx_x['PROD_GROUP_CD']}'
                    AND PROD_CD = '{idx_x['PROD_CD']}'
                ;
            """
            # AND FILE_ID = '{idx_x['FILE_ID']}'
            logger.info(f"###### update_query : {update_query}")
            sql_instance = MySql()
            sql_instance.commit_query(query=update_query)
            
        # end for-loop : run_df.index (zip파일 내 pdf 병합결과 gpt 수행 완료)
        logger.info(f"###### End for-loop - section split")

        filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
        logs_str = "\n".join(filter_logs_data_type)
        log_file_path = f"prcapi/log/{formatted_date}/{data_type}.log"
        as_instance_log.upload_file(logs_str, file_path=log_file_path, overwrite=True)

        json_logging_aoai = json.dumps(logging_aoai, ensure_ascii=False, indent=2).encode('utf-8')
        as_instance_log.upload_file(json_logging_aoai, file_path=f"prcapi/prc_info/{formatted_date}/{data_type}_zip_gpt_history.json", overwrite=True)   
        
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