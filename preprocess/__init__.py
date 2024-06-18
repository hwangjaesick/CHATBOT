import json  
import logging  
import azure.functions as func
import credential
import pandas as pd
import os
from datetime import datetime
import pytz
import time
import traceback

def main(mytimer: func.TimerRequest) -> None:
    
    if not credential.keyvault_values:
        credential.fetch_keyvault_values()

    from preprocessing import Preprocessing
    from database import AzureStorage, MySql
    from log import setup_logger, thread_local
    from tools import clean_string, calc_stats, run_indexer, filter_logs_by_data_type, update_index_count
    from ai_search import AISearch
    
    search_instance = AISearch()
    as_instance_log = AzureStorage(container_name='logs', storage_type='datast')
    preprocess_instance = Preprocessing()
    korea_timezone = pytz.timezone('Asia/Seoul')
    now = datetime.now(korea_timezone)
    formatted_date = now.strftime('%Y-%m-%d')
    # formatted_date = '2024-06-01'
    preprocess_time = formatted_date

    # sql_instance = MySql()
    # index_mst = sql_instance.get_table("""
    #                                     SELECT CODE_CD, CODE_NAME
    #                                     FROM tb_code_mst
    #                                     WHERE GROUP_CD = 'B00003'
    #                                     """)
    # index_mst = pd.DataFrame(index_mst)
    # index_mst = index_mst.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    #====================================================== 
    # PROCEDURE 전처리 시작
    #======================================================

    start_time = time.time()
    data_type = 'procedure'
    logger = setup_logger(data_type)
    logger.info("=" * 30 + " Start Procedure Preprocessing " + "=" * 30)
    logger.info("=" * 30 + f" {formatted_date} " + "=" * 30)

    try :
        procedure_result = preprocess_instance.run_procedure(logger=logger)
        filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
        logs_str = "\n".join(filter_logs_data_type)
        log_file_path = f"prcapi/batch/log/{formatted_date}/{data_type}.log"
        as_instance_log.upload_file(logs_str, file_path=log_file_path, overwrite=True)
        as_instance_log.upload_file(procedure_result, file_path=f"prcapi/batch/prc_info/{formatted_date}/{data_type}.json", overwrite=True)

    except Exception as e :
        logger.error(str(e))
        logger.error(traceback.format_exc())
        error_procedure = f"{type(e).__name__} : '{e}'"
        logger.error(error_procedure)
        filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
        logs_str = "\n".join(filter_logs_data_type)
        log_file_path = f"prcapi/batch/error_log/{formatted_date}/{data_type}.log"
        as_instance_log.upload_file(logs_str, file_path=log_file_path, overwrite=True)
    
    end_time = time.time()
    PROCEDURE_TIME = end_time - start_time
    logger.info("=" * 30 + " End Procedure Preprocessing " + "=" * 30)
    logger.info("=" * 30 + f" {PROCEDURE_TIME} " + "=" * 30)
    #====================================================== 
    # SPEC 전처리 시작
    #======================================================
    
    start_time = time.time()
    data_type = 'spec'
    logger = setup_logger(data_type)
    logger.info("=" * 30 + " Start Spec Preprocessing " + "=" * 30)
    logger.info("=" * 30 + f" {formatted_date} " + "=" * 30)
    
    try :
        spec_index_list, spec_result = preprocess_instance.spec_to_storage(logger=logger, type='batch',time=preprocess_time) 
        filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
        logs_str = "\n".join(filter_logs_data_type)
        log_file_path = f"prcapi/batch/log/{formatted_date}/{data_type}.log"
        as_instance_log.upload_file(logs_str, file_path=log_file_path, overwrite=True)
        as_instance_log.upload_file(spec_result, file_path=f"prcapi/batch/prc_info/{formatted_date}/{data_type}.json", overwrite=True)

    except Exception as e :
        error = traceback.format_exc()
        logger.error(error)
        filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
        logs_str = "\n".join(filter_logs_data_type)
        log_file_path = f"prcapi/batch/error_log/{formatted_date}/{data_type}.log"
        as_instance_log.upload_file(logs_str, file_path=log_file_path, overwrite=True)

    end_time = time.time()
    SPEC_TIME = end_time - start_time
    logger.info("=" * 30 + " End Spec Preprocessing " + "=" * 30)
    logger.info("=" * 30 + f" {SPEC_TIME} " + "=" * 30)

    #====================================================== 
    # 컨텐츠 전처리 시작
    #======================================================

    start_time = time.time()
    data_type = 'contents'
    logger = setup_logger(data_type)
    logger.info("=" * 30 + " Start Contents Preprocessing " + "=" * 30)
    logger.info("=" * 30 + f" {formatted_date} " + "=" * 30)

    try :
        contents_index_list, contents_result = preprocess_instance.contents_to_storage(logger=logger, type='batch', time=preprocess_time)
        filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
        logs_str = "\n".join(filter_logs_data_type)
        log_file_path = f"prcapi/batch/log/{formatted_date}/{data_type}.log"
        as_instance_log.upload_file(logs_str, file_path=log_file_path, overwrite=True)
        as_instance_log.upload_file(contents_result, file_path=f"prcapi/batch/prc_info/{formatted_date}/{data_type}.json", overwrite=True)

    except Exception as e :
        error = traceback.format_exc()
        logger.error(error)
        filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
        logs_str = "\n".join(filter_logs_data_type)
        log_file_path = f"prcapi/batch/error_log/{formatted_date}/{data_type}.log"
        as_instance_log.upload_file(logs_str, file_path=log_file_path, overwrite=True)

    end_time = time.time()
    CONTENTS_TIME = end_time - start_time
    logger.info("=" * 30 + " End Contents Preprocessing " + "=" * 30)
    logger.info("=" * 30 + f" {CONTENTS_TIME} " + "=" * 30)

    #====================================================== 
    # microsites 전처리 시작
    #======================================================

    start_time = time.time()
    data_type = 'microsites'
    logger = setup_logger(data_type)
    logger.info("=" * 30 + " Start Microsites Preprocessing " + "=" * 30)
    logger.info("=" * 30 + f" {formatted_date} " + "=" * 30)

    try :
        microsites_index_list, microsites_result = preprocess_instance.microsites_to_storage(logger=logger, type='batch', time=preprocess_time)
        filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
        logs_str = "\n".join(filter_logs_data_type)
        log_file_path = f"prcapi/batch/log/{formatted_date}/{data_type}.log"
        as_instance_log.upload_file(logs_str, file_path=log_file_path, overwrite=True)
        as_instance_log.upload_file(microsites_result, file_path=f"prcapi/batch/prc_info/{formatted_date}/{data_type}.json", overwrite=True)

    except Exception as e :
        error = traceback.format_exc()
        logger.error(error)
        filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
        logs_str = "\n".join(filter_logs_data_type)
        log_file_path = f"prcapi/batch/error_log/{formatted_date}/{data_type}.log"
        as_instance_log.upload_file(logs_str, file_path=log_file_path, overwrite=True)
    
    #====================================================== 
    # 매뉴얼 전처리 시작
    #======================================================

    start_time = time.time()
    data_type = 'manual'
    logger = setup_logger(data_type)
    logger.info("=" * 30 + " Start Manual(batch) Preprocessing " + "=" * 30)
    logger.info("=" * 30 + f" {formatted_date} " + "=" * 30)

    try :
        #====================================================== 
        # 매뉴얼 전처리 시작
        #======================================================
        data_type = 'manual'
        logger = setup_logger(data_type)
        logger.info("=" * 30 + " Start Manual Preprocessing " + "=" * 30)
        logger.info("=" * 30 + f" {formatted_date} " + "=" * 30)
        
        as_instance_raw = AzureStorage(container_name='raw-data',storage_type='datast')

        TEMP_MODIFY = False # [2024.04.24] 현재 LGEPL 법인으로 관리되는 데이터(LGECZ, LGESK)들을 반영하기 위해 사용. LGECZ 및 LGESK가 개별로 관리하게 되면 False로 변경해야함.
        
        mst_blob_paths = {
        'inspection_pdf_metadata': 'Contents_Manual_List_Mst_Data/applied_manual_related_table/inspection_pdf_metadata_v1.csv',
        'manual_list_pdf_metadata': 'Contents_Manual_List_Mst_Data/applied_manual_related_table/manual_list_pdf_metadata_v1.csv',
        'cover_title': 'Contents_Manual_List_Mst_Data/applied_manual_related_table/cover_title_list_v1.csv',
        'section_group_mapping': 'Contents_Manual_List_Mst_Data/applied_manual_related_table/product_group_mapping_v1.csv',
        'prod_mst': 'Contents_Manual_List_Mst_Data/PRODUCT_MST.csv'}
        
        korea_timezone = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_timezone)
        today_date_str = preprocess_time # now.strftime('%Y-%m-%d')

        #====================================================== 
        # 매뉴얼 다운로드
        #====================================================== 
        # -- LOAD PRODUCT_MST 
        # [2024.04.25] 수정 - load 방식 변경
        prod_mst = as_instance_raw.read_file(mst_blob_paths['prod_mst'])
        prod_group_cd_map = prod_mst[['PROD_GROUP_CD', 'PROD_CD']].drop_duplicates().reset_index(drop=True)

        # -- 법인 대상언어 
        sql_instance = MySql()
        corp_lang_mapp = sql_instance.get_tar_language(temp_modify=TEMP_MODIFY)

        # -- 처리 대상 load
        query = f"""
                SELECT
                    A.*
                FROM (
                    SELECT 
                        *
                    FROM tb_if_manual_list
                    WHERE 1=1
                        AND DELETE_FLAG = 'N'
                        AND (DATE_FORMAT(CREATE_DATE, '%Y-%m-%d') = '{today_date_str}' OR DATE_FORMAT(UPDATE_DATE, '%Y-%m-%d') = '{today_date_str}')
                ) A
                ;
            """

        sql_instance = MySql()
        tb_if_manual_list = sql_instance.load_table_as_df(query)
        
        logger.info(f"###### Num of updated or created data = {len(tb_if_manual_list)}")
        if len(tb_if_manual_list) == 0:
            logger.info(f"###### There is no updated manual data")
            
        else :
            # [2024.05.03] 추가, PROD_CD 쉼표(,)로 이어붙어 들어오는 경우 처리 로직
            tb_if_manual_list['SPLIT_PROD_CD'] = [x.split(",") for x in tb_if_manual_list['PROD_CD']]
            tmp_if_manual_list = tb_if_manual_list.explode(column=['SPLIT_PROD_CD'],ignore_index=False)
            tmp_if_manual_list['PROD_CD'] = tmp_if_manual_list['SPLIT_PROD_CD']
            
            # -- 법인 대상언어 맵핑
            tmp_list = tmp_if_manual_list.merge(corp_lang_mapp, how='left', on=['CORP_CD'])
            tmp_list = tmp_list.dropna(subset=['TARGET_LANGUAGE_NAMES'])
            tmp_list2  = tmp_list[tmp_list.apply(lambda x: any(lang in x['LANGUAGE_LIST'] for lang in x['TARGET_LANGUAGE_NAMES']), axis=1)].reset_index(drop=True)
            # -- PROD_GROUP_CD 매핑
            tmp_list2['PROD_CD'] = [clean_string(x) for x in tmp_list2['PROD_CD']]
            tmp_list2 = tmp_list2.merge(prod_group_cd_map, how='left', on = ['PROD_CD'])
            tmp_list2['EXT_NAME'] = [os.path.splitext(x)[-1].replace(".", "").lower() for x in tmp_list2['FILE_REAL_NAME']]
            
            # [2024.05.03] 추가, Regist Date가 가장 최근인 매뉴얼 load
            down_key = tmp_list2.groupby(['CORP_CD', 'LANGUAGE_LIST', 'PROD_GROUP_CD', 'PROD_CD', 'ITEM_ID']).agg({"REGIST_DATE":"last"}).reset_index()
            down_target = down_key.merge(tmp_list2, how = 'left', on = list(down_key.columns)).reset_index(drop=True)
            down_target = down_target[['CORP_CD', 'LANGUAGE_LIST', 'PROD_GROUP_CD', 'PROD_CD', 'ITEM_ID', 'FILE_REAL_NAME', 'FILE_ID', 'EXT_NAME', 'REGIST_DATE']]\
                            .drop_duplicates().reset_index(drop=True)       
            
            # [2024.05.03] 추가, strip 처리
            strip_cols = ['ITEM_ID', 'CORP_CD', 'LANGUAGE_LIST', 'PROD_GROUP_CD', 'PROD_CD', 'FILE_ID']
            down_target[strip_cols] = down_target[strip_cols].apply(lambda x: x.str.strip())
            logger.info(f"###### Num of download manual = {len(down_target)}")
            
            # 다운로드 - TB_IF_MANUAL_BLOB 적재(update & insert)
            preprocess_instance.download_manual(logger, down_target)
            logger.info(f"###### Complete - download_manual")

            #=============================== 
            # 통합, 단일 구분
            #===============================            
            query = f"""
                    SELECT 
                        *
                    FROM tb_if_manual_blob
                    WHERE 1=1
                        AND DOWNLOAD_YN = 'Y'
                        AND DATE_FORMAT(DOWNLOAD_DATE, '%Y-%m-%d') = '{today_date_str}'
                    ;
                """
            sql_instance = MySql()
            tb_if_manual_blob = sql_instance.load_table_as_df(query)
            
            # -- 대상 매뉴얼 메타데이터 추출(용량, 페이지수:pdf인 경우만)
            tmp_target_blob = preprocess_instance.get_metadata(logger, tb_if_manual_blob)
            tmp_target_blob['REGIST_YEAR']      = [x.year for x in tmp_target_blob['REGIST_DATE']]
            tmp_target_blob['REGIST_YEAR_TYPE'] = ["since_2016" if x >= 2016 else "before_2016" for x in tmp_target_blob['REGIST_YEAR']]

            # -- 기준 통계량 계산
            std_stats = calc_stats(mst_blob_paths, as_instance_raw)
            logger.info(f"###### Complete - calc_stats")
            
            # -- 단일, 통합 로직 선정
            tmp2_target_blob = tmp_target_blob.merge(std_stats, how='left', on=['PROD_GROUP_CD', 'REGIST_YEAR_TYPE'])\
                                .reset_index(drop=True)
            tmp2_target_blob['NP_75%'] = tmp2_target_blob['NP_75%'].fillna(tmp2_target_blob['NP_75%'].min()) # [2024.04.30] 수정

            # 프로세스 선정 : 단일/통합 구분 - tb_chat_manual_prc 적재 (update & insert)
            preprocess_instance.set_process(logger, tmp2_target_blob)
            logger.info(f"###### Complete - set_process")
        # end if-else

        #====================================================== 
        # 언어 분리
        #======================================================
        # -- 금일 업데이트된 매뉴얼 BLOB 중 언어 분리 대상 LOAD
        query = f"""
            SELECT 
                *
            FROM tb_chat_manual_prc
            WHERE 1=1
                AND DOWNLOAD_YN = 'Y'
                AND DATE_FORMAT(DOWNLOAD_DATE, '%Y-%m-%d') = '{today_date_str}'
                AND EXT_NAME = 'pdf'
                AND METADATA_MULTI_YN = 'Y'
                AND NUM_PAGES > 0
            ;
        """
        sql_instance = MySql()
        tmp_target_lang_split = sql_instance.load_table_as_df(query)
        
        # 언어 분리 - tb_chat_manual_split_lang 적재 (delete, insert)
        preprocess_instance.split_lang(logger, tmp_target_lang_split, mst_blob_paths, temp_modify=TEMP_MODIFY)
        logger.info(f"###### Complete - split_lang")

        #====================================================== 
        # 압축 해제
        #======================================================
        # -- 금일 업데이트된 매뉴얼 BLOB 중 압축해제 대상 LOAD
        query = f"""
            SELECT 
                *
            FROM tb_chat_manual_prc
            WHERE 1=1
                AND DOWNLOAD_YN = 'Y'
                AND DATE_FORMAT(DOWNLOAD_DATE, '%Y-%m-%d') = '{today_date_str}'
                AND EXT_NAME = 'zip'
            ;
        """
        sql_instance = MySql()
        tmp_target_unzip = sql_instance.load_table_as_df(query)

        # 압축풀기 - csv 파일 blob으로 저장
        preprocess_instance.unzip(logger, tmp_target_unzip)
        logger.info(f"###### End - unzip")
        
        filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
        logs_str = "\n".join(filter_logs_data_type)
        log_file_path = f"prcapi/batch/log/{formatted_date}/{data_type}.log"
        as_instance_log.upload_file(logs_str, file_path=log_file_path, overwrite=True)
        
    except Exception as e :
        logger.error(str(e))
        logger.error(traceback.format_exc())
        error_manual = f"{type(e).__name__} : '{e}'"
        logger.error(error_manual)
        filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
        logs_str = "\n".join(filter_logs_data_type)
        log_file_path = f"prcapi/batch/error_log/{formatted_date}/{data_type}.log"
        as_instance_log.upload_file(logs_str, file_path=log_file_path, overwrite=True)

    end_time = time.time()
    MANUAL_BARCH_TIME = end_time - start_time
    logger.info("=" * 30 + " End Manual(batch) Preprocessing " + "=" * 30)
    logger.info("=" * 30 + f" {MANUAL_BARCH_TIME} " + "=" * 30)

    #====================================================== 
    # 인덱서 시작
    #======================================================
    data_type = 'indexer'
    logger = setup_logger(data_type)
    
    search_instance = AISearch()
    indexer_client = search_instance.indexer_client
    
    try :
        # 전체 인덱서 실행
        index_list=[index for index in search_instance.index_client.list_index_names()]
        run_indexer(logger=logger, indexer_list=index_list, indexer_client=indexer_client)

        # 문서 현황 업데이트
        result = update_index_count(logger=logger, search_instance=search_instance, index_list=[index for index in search_instance.index_client.list_index_names()]) 
        logger.info(result)

        # 문서 백업
        index_list=[index for index in search_instance.index_client.list_index_names()]
        for index_name in index_list :
            search_instance.backup_index(logger=logger, index_name=index_name, batch_size=1000)

        filter_logs_data_type = filter_logs_by_data_type(logs=thread_local.log_list, data_type=data_type)
        logs_str = "\n".join(filter_logs_data_type)
        log_file_path = f"prcapi/batch/log/{formatted_date}/{data_type}.log"
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
    
    # preprocess_result = {'procedure_result' : json.loads(procedure_result),
    #                     'spec_result' : json.loads(spec_result),
    #                     'contents_result' : json.loads(contents_result),
    #                      'microsites_result' : json.loads(microsites_result)}
    
    # output = json.dumps(preprocess_result, ensure_ascii=False, indent=4).encode('utf-8')
    # Return the response  
    return None