import json  
import logging  
import azure.functions as func
import credential
import pandas as pd
import os, re
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
    
    TEMP_MODIFY = False # [2024.04.24] 현재 LGEPL 법인으로 관리되는 데이터(LGECZ, LGESK)들을 반영하기 위해 사용. LGECZ 및 LGESK가 개별로 관리하게 되면 False로 변경해야함.
    
    try :
        #==================================================
        # 섹션 분할
        #==================================================
        # -- 섹션 분할 대상 LOAD : 
        # 1. 섹션분할 수행 대상이지만 에러가 발생한 경우
        # 2. 섹션분할 수행 대상이고 수행한 이력이 없는 경우
        query = f"""
            SELECT 
                *
            FROM tb_chat_manual_prc
            WHERE 1=1
                AND EXT_NAME = 'pdf'
                AND SECTION_YN = 'Y'
                AND (SECTION_STATUS = 'Failed' OR SECTION_STATUS = '')
            ;
        """
        sql_instance = MySql()
        tar_run_gpt  = sql_instance.load_table_as_df(query)
        
        logger.info(f"###### Num of data = {len(tar_run_gpt)}")
        if len(tar_run_gpt) == 0:
            return func.HttpResponse("실행 완료 - 섹션 분할 수행대상 없음")
        
        # -- 섹션 분할 수행 전 기존에 적재된 결과들 제거
        # 테이블 TB_CAHT_MANUAL_SECT의 key 정보 기준으로 blob 삭제(대상 containder:preprocessed-data) 후 table 내용 삭제
        logger.info(f"###### delete prior section results")
        for idx in tar_run_gpt.index:
            idx_x = tar_run_gpt.loc[idx].to_dict()
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
            del_infos = sql_instance.load_table_as_df(select_query)
            del_infos = del_infos[['ITEM_ID', 'CORP_CD', 'PROD_GROUP_CD', 'PROD_CD', 'LANGUAGE_NAME', 'BLOB_PATH']].drop_duplicates()
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
            logger.info(f"###### delete_query - {delete_query}")
            sql_instance = MySql()
            sql_instance.commit_query(delete_query)
        # end for-loop : tar_run_gpt.index(기존 적재 결과 삭제 완료)
        
        # -- 섹션 분할 수행 
        section_groups = as_instance_raw.read_file(mst_blob_paths['section_group_mapping'])
        logging_aoai = []
        for idx in tar_run_gpt.index :
            idx_x = tar_run_gpt.loc[idx].to_dict()
            
            if ((idx_x['LANG_LIST_MULTI_YN'] == 'N') & (idx_x['METADATA_MULTI_YN'] == 'N') & (idx_x['SECTION_YN'] == 'Y')):
                # 단일본, 섹션 분할 대상
                idx_x['LANGUAGE_NAME'] = re.sub(" +", "", idx_x['LANGUAGE_LIST'].split(",")[0])
                res_dict, status, status_des, res_aoai = preprocess_instance.run_split_section(logger, idx_x, aoai_res="m2", section_groups=section_groups)
                logger.info(f"###### section split - status : {status}\nstatust_des : {status_des}\n{res_aoai}")
                logging_aoai.extend(res_aoai)
                # -- 섹션분할 프로세스 상태 DB 업데이트
                update_query = f"""
                    UPDATE tb_chat_manual_prc
                    SET SECTION_STATUS = '{status}'
                        , SECTION_STATUS_DES = '{status_des}'
                        , UPDATE_DATE = '{datetime.now(korea_timezone).replace(tzinfo = None)}'
                    WHERE 1=1
                        AND ITEM_ID = '{idx_x['ITEM_ID']}'
                        AND CORP_CD = '{idx_x['CORP_CD']}'
                        AND PROD_GROUP_CD = '{idx_x['PROD_GROUP_CD']}'
                        AND PROD_CD = '{idx_x['PROD_CD']}'
                        AND FILE_ID = '{idx_x['FILE_ID']}'
                    ;
                """
                logger.info(f"###### update query :\n{update_query}")
                sql_instance = MySql()
                sql_instance.commit_query(update_query)
                
            elif ((idx_x['METADATA_MULTI_YN'] == 'Y') & (idx_x['SECTION_YN'] == 'Y') & (idx_x['NUM_PAGES'] > 0)) :
                # 통합본, 섹션 분할 대상
                # -- 언어분리 결과 중 법인 대상언어 LOAD
                query = f"""
                    SELECT 
                        *
                    FROM tb_chat_manual_split_lang
                    WHERE 1=1
                        AND ITEM_ID = '{idx_x['ITEM_ID']}'
                        AND CORP_CD = '{idx_x['CORP_CD']}'
                        AND PROD_GROUP_CD = '{idx_x['PROD_GROUP_CD']}'
                        AND PROD_CD = '{idx_x['PROD_CD']}'
                        AND TARGET_LANGUAGE_YN = 'Y'
                    ;
                """                
                sql_instance = MySql()
                lang_target = sql_instance.load_table_as_df(query)
                
                # -- 언어별 섹션 분할 수행
                for ii in lang_target.index :
                    tmp_ii = lang_target.loc[ii].to_dict()
                    res_dict, status, status_des, res_aoai = preprocess_instance.run_split_section(logger, tmp_ii, aoai_res="m2", section_groups=section_groups)
                    logger.info(f"###### section split - status : {status}\nstatust_des : {status_des}\n{res_aoai}")
                    logging_aoai.extend(res_aoai)
                    # -- 섹션분할 프로세스 상태 DB 업데이트 (주의, 언어별 결과를 트래킹할 수 없음)
                    update_query = f"""
                        UPDATE tb_chat_manual_prc
                        SET SECTION_STATUS = '{status}'
                            , SECTION_STATUS_DES = '{status_des}'
                            , UPDATE_DATE = '{datetime.now(korea_timezone).replace(tzinfo = None)}'
                        WHERE 1=1
                            AND ITEM_ID = '{idx_x['ITEM_ID']}'
                            AND CORP_CD = '{idx_x['CORP_CD']}'
                            AND PROD_GROUP_CD = '{idx_x['PROD_GROUP_CD']}'
                            AND PROD_CD = '{idx_x['PROD_CD']}'
                            AND FILE_ID = '{idx_x['FILE_ID']}'
                        ;
                    """
                    logger.info(f"###### update query :\n{update_query}")
                    sql_instance = MySql()
                    sql_instance.commit_query(query=update_query)

                    if status == 'Failed':
                        break
                # end for-loop : lang_target(통합본 언어분리 중 법인 대상언어 섹션 분할 종료)
            # end if-else
        # end for-loop : tar_run_gpt.index(섹션 분할 완료)
        logger.info(f"###### End for-loop - section split")
        
        filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
        logs_str = "\n".join(filter_logs_data_type)
        log_file_path = f"prcapi/log/{formatted_date}/{data_type}.log"
        as_instance_log.upload_file(logs_str, file_path=log_file_path, overwrite=True)

        json_logging_aoai = json.dumps(logging_aoai, ensure_ascii=False, indent=2).encode('utf-8')
        as_instance_log.upload_file(json_logging_aoai, file_path=f"prcapi/prc_info/{formatted_date}/{data_type}_pdf_gpt_history.json", overwrite=True)

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