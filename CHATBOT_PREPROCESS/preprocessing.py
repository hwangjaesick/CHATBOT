import pandas as pd
import time
from database import AzureStorage, MySql
from model import OpenAI_Section # [2024.04.25] 추가
from datetime import datetime
from bs4 import BeautifulSoup
import yaml
import json
import logging
import tiktoken
import io, os, re
import fitz
import requests
from urllib.request import urlopen
from PyPDF2 import PdfReader, PdfWriter
from collections import Counter, defaultdict
from tqdm import tqdm
from tools import extract_data, remove_tag_between, chunked_texts, filter_logs_by_data_type, num_tokens_from_string, read_pdf_using_fitz, make_html_group, extract_page_patterns, to_structured_1, split_text_by_batch, get_system_mssg, parse_text_to_dict, merge_dicts, extract_main_text_microsite
import pytz
from zipfile import ZipFile # [2024.04.24] 추가
from ai_search import AISearch
from tenacity import retry, wait_fixed, stop_after_attempt

class Preprocessing():

    @retry(wait=wait_fixed(600), stop=stop_after_attempt(5))
    def run_executor(self, logger, procedure, table_list) :
        
        start_time = time.time()
        try :
            sql_instance = MySql()
            sql_instance.query_executor(query=f"call {procedure}('{table_list}');")
            end_time = time.time()
            TIME = end_time - start_time
            RESULT = f"success ( {procedure} )"
            logger.info(f"##### COMPLETE : {procedure} ( {TIME} )")

        except Exception as e :
            end_time = time.time()
            TIME = end_time - start_time
            RESULT = f"fail ( {TIME} )"
            logger.error(f"##### ERROR : {procedure}, {str(e)}")
        
        return RESULT
        
    def run_procedure(self, logger):

        procedure_result = {}

        result = self.run_executor(logger=logger, procedure='SP_IF_MANUAL_MIGRATION', table_list='IF_MANUAL_LIST')
        procedure_result['SP_IF_MANUAL_MIGRATION'] = result

        result = self.run_executor(logger=logger, procedure='SP_IF_CONTENT_MST_MIGRATION', table_list='IF_CON_MST_LIST')
        procedure_result['SP_IF_CONTENT_MST_MIGRATION'] = result
        
        result = self.run_executor(logger=logger, procedure='SP_IF_CONTENT_HTML_MIGRATION', table_list='IF_CON_HTML_LIST')
        procedure_result['SP_IF_CONTENT_HTML_MIGRATION'] = result
        
        result = self.run_executor(logger=logger, procedure='SP_IF_CONTENT_META_MIGRATION', table_list='IF_CON_META_LIST')
        procedure_result['SP_IF_CONTENT_META_MIGRATION'] = result
        
        # result = self.run_executor(logger=logger, procedure='SP_IF_MODEL_MAPPING_MIGRATION', table_list='IF_MODEL_MAPPING_LIST')
        # procedure_result['SP_IF_MODEL_MAPPING_MIGRATION'] = result   
        
        result = self.run_executor(logger=logger, procedure='SP_IF_SPEC_MST_MIGRATION', table_list='IF_SPEC_MST_LIST')
        procedure_result['SP_IF_SPEC_MST_MIGRATION'] = result
        
        result = self.run_executor(logger=logger, procedure='SP_IF_CONTENT_HIT_MIGRATION', table_list='IF_CON_HIT_LIST')
        procedure_result['SP_IF_CONTENT_HIT_MIGRATION'] = result
        
        result = self.run_executor(logger=logger, procedure='SP_IF_CONTENT_SHOTURL_MIGRATION', table_list='IF_CON_SHOT_LIST')
        procedure_result['SP_IF_CONTENT_SHOTURL_MIGRATION'] = result

        result = self.run_executor(logger=logger, procedure='SP_MST_CHAT_AUTO_LIST', table_list='MST_CHAT_AUTO_LIST')
        procedure_result['SP_MST_CHAT_AUTO_LIST'] = result

        result = self.run_executor(logger=logger, procedure='SP_MST_CONTNET_FAQ_SUMMARY', table_list='MST_FAQ_LIST')
        procedure_result['SP_MST_CONTNET_FAQ_SUMMARY'] = result        

        result = self.run_executor(logger=logger, procedure='SP_MST_CONTNET_HIT_SUMMARY', table_list='HIT')
        procedure_result['SP_MST_CONTNET_HIT_SUMMARY'] = result            

        result = self.run_executor(logger=logger, procedure='SP_MST_MODEL_SUMMARY', table_list='MST_MODEL_SUMMARY_LIST')
        procedure_result['SP_MST_MODEL_SUMMARY'] = result            
        
        procedure_result = json.dumps(procedure_result, ensure_ascii=False, indent=4).encode('utf-8')

        return procedure_result


    def contents_to_storage(self, logger, type='batch', user_type = "C", status_name="Approved", time=None, corp_cd=None):
        
        logger.info("###### Function Name : contents_to_storage")

        korea_timezone = pytz.timezone('Asia/Seoul')
        current_time_korea = datetime.now(korea_timezone)
        update_date = current_time_korea.strftime('%Y-%m-%d %H:%M:%S')
        
        if time is None :
            korea_timezone = pytz.timezone('Asia/Seoul')
            now = datetime.now(korea_timezone)
            formatted_date = now.strftime('%Y-%m-%d')
            
        else :
            formatted_date = time

        if type == 'batch' :

            parameters = (formatted_date, formatted_date)

            sql_instance = MySql()
            NEW_CONTENTS_MST = sql_instance.get_table(query="""
            SELECT distinct
                mst.contents_id, mst.contents_seq, html.html_contents, html.language_cd AS language, UPPER(meta.locale_cd) as locale_cd, mst.corp_cd,
                mst.prod_group_cd, mst.prod_group_name, mst.prod_cd, mst.prod_name, mst.approval_status, mst.expiredyn,  
                mst.user_type, mst.symp_level1_code, mst.symp_level1_name, mst.symp_level2_code, mst.symp_level2_name, 
                mst.classification_type, mst.contents_name, mst.summary, mst.key_list, mst.use_yn, 
                mst.regist_date, mst.start_date, mst.end_date, mst.content_update_date, mst.if_create_date, mst.create_date, 
                mst.create_user, mst.update_date, mst.update_user, url.short_url, null AS prc_type, "N" AS prc_flag, "N" AS prc_status
            FROM 
                tb_if_content_mst AS mst
            INNER JOIN (
                SELECT CONTENTS_ID, LOCALE_CD
                FROM tb_if_content_meta
            ) AS meta ON mst.contents_id = meta.contents_id
            INNER JOIN (
                SELECT CONTENTS_ID, LOCALE_CD, short_url, CREATE_DATE
                FROM (
                    SELECT CONTENTS_ID, LOCALE_CD, short_url, CREATE_DATE,
                        ROW_NUMBER() OVER (PARTITION BY CONTENTS_ID, LOCALE_CD ORDER BY CREATE_DATE DESC) AS row_num
                    FROM tb_if_content_url
                ) AS ranked_url
                WHERE row_num = 1
            ) AS url ON mst.contents_id = url.contents_id AND UPPER(meta.LOCALE_CD) = UPPER(url.LOCALE_CD)
            INNER JOIN (
                SELECT contents_id, html_contents, language_cd
                FROM tb_if_content_html
            ) AS html ON mst.contents_id = html.contents_id
            WHERE mst.create_date >= %s or mst.update_date >= %s;
            """, parameters=parameters) # content_update_date

            if NEW_CONTENTS_MST is None :
                NEW_CONTENTS_MST = []

            sql_instance = MySql()
            MISSING_CONTENTS_MST = sql_instance.get_table(query="""
                SELECT distinct
                    mst.contents_id, mst.contents_seq, html.html_contents, html.language_cd AS language, UPPER(meta.locale_cd) as locale_cd, mst.corp_cd,
                    mst.prod_group_cd, mst.prod_group_name, mst.prod_cd, mst.prod_name, mst.approval_status, mst.expiredyn,  
                    mst.user_type, mst.symp_level1_code, mst.symp_level1_name, mst.symp_level2_code, mst.symp_level2_name, 
                    mst.classification_type, mst.contents_name, mst.summary, mst.key_list, mst.use_yn, 
                    mst.regist_date, mst.start_date, mst.end_date, mst.content_update_date, mst.if_create_date, mst.create_date, 
                    mst.create_user, mst.update_date, mst.update_user, url.short_url, prc.prc_type, prc.prc_flag, prc.prc_status
                FROM 
                    tb_if_content_mst AS mst
                INNER JOIN (
                    SELECT CONTENTS_ID, LOCALE_CD
                    FROM tb_if_content_meta
                ) AS meta ON mst.contents_id = meta.contents_id
                INNER JOIN (
                    SELECT CONTENTS_ID, LOCALE_CD, short_url, CREATE_DATE
                    FROM (
                        SELECT CONTENTS_ID, LOCALE_CD, short_url, CREATE_DATE,
                            ROW_NUMBER() OVER (PARTITION BY CONTENTS_ID, LOCALE_CD ORDER BY CREATE_DATE DESC) AS row_num
                        FROM tb_if_content_url
                    ) AS ranked_url
                    WHERE row_num = 1
                ) AS url ON mst.contents_id = url.contents_id AND UPPER(meta.LOCALE_CD) = UPPER(url.LOCALE_CD)
                INNER JOIN (
                    SELECT contents_id, html_contents, language_cd
                    FROM tb_if_content_html
                ) AS html ON mst.contents_id = html.contents_id
                INNER JOIN (
                    SELECT contents_id, locale_cd, prc_type, prc_flag, prc_status
                    FROM tb_chat_content_prc
                ) AS prc ON mst.contents_id = prc.contents_id AND UPPER(meta.LOCALE_CD) = UPPER(prc.locale_cd)
                WHERE prc.prc_flag = 'Y' AND prc.prc_status='N'
                """)
            
            if MISSING_CONTENTS_MST is not None :
                NEW_CONTENTS_MST = NEW_CONTENTS_MST + MISSING_CONTENTS_MST

        else :

            parameters = (corp_cd.upper(),)
            sql_instance = MySql()
            NEW_CONTENTS_MST = sql_instance.get_table(query="""
            SELECT distinct
                mst.contents_id, mst.contents_seq, html.html_contents, html.language_cd AS language, UPPER(meta.locale_cd) as locale_cd, mst.corp_cd,
                mst.prod_group_cd, mst.prod_group_name, mst.prod_cd, mst.prod_name, mst.approval_status, mst.expiredyn,  
                mst.user_type, mst.symp_level1_code, mst.symp_level1_name, mst.symp_level2_code, mst.symp_level2_name, 
                mst.classification_type, mst.contents_name, mst.summary, mst.key_list, mst.use_yn, 
                mst.regist_date, mst.start_date, mst.end_date, mst.content_update_date, mst.if_create_date, mst.create_date, 
                mst.create_user, mst.update_date, mst.update_user, url.short_url, null AS prc_type, "N" AS prc_flag, "N" AS prc_status
            FROM 
                tb_if_content_mst AS mst
            INNER JOIN (
                SELECT CONTENTS_ID, LOCALE_CD
                FROM tb_if_content_meta
            ) AS meta ON mst.contents_id = meta.contents_id
            INNER JOIN (
                SELECT CONTENTS_ID, LOCALE_CD, short_url, CREATE_DATE
                FROM (
                    SELECT CONTENTS_ID, LOCALE_CD, short_url, CREATE_DATE,
                        ROW_NUMBER() OVER (PARTITION BY CONTENTS_ID, LOCALE_CD ORDER BY CREATE_DATE DESC) AS row_num
                    FROM tb_if_content_url
                ) AS ranked_url
                WHERE row_num = 1
            ) AS url ON mst.contents_id = url.contents_id AND UPPER(meta.LOCALE_CD) = UPPER(url.LOCALE_CD)
            INNER JOIN (
                SELECT contents_id, html_contents, language_cd
                FROM tb_if_content_html
            ) AS html ON mst.contents_id = html.contents_id
            WHERE mst.CORP_CD = %s;
            """, parameters=parameters)

        sql_instance = MySql()
        LANGUAGE_MST = sql_instance.get_table("""
                                                SELECT CORP_CD, LOCALE_CD, LANGUAGE_CD, CODE_NAME
                                                FROM tb_corp_lan_map
                                                JOIN tb_code_mst ON tb_code_mst.CODE_CD = tb_corp_lan_map.LANGUAGE_CD
                                                WHERE GROUP_CD = 'B00003'
                                                """)
        LANGUAGE_MST = pd.DataFrame(LANGUAGE_MST)
        LANGUAGE_MST = LANGUAGE_MST.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        sql_instance = MySql()
        ISO_CD = sql_instance.get_table("""
                                        SELECT *
                                        FROM tb_code_mst
                                        WHERE GROUP_CD = 'B00002';
                                        """)
        ISO_CD = pd.DataFrame(ISO_CD)
        ISO_CD = ISO_CD.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        as_instance_raw = AzureStorage(container_name='raw-data', storage_type= 'datast')
        as_instance_preprocessed = AzureStorage(container_name='preprocessed-data', storage_type= 'datast')
        as_instance_preprocessed_docst = AzureStorage(container_name='documents', storage_type='docst')

        utc_now = datetime.utcnow()
        utc_now = utc_now.replace(tzinfo=pytz.utc)
        korea_timezone = pytz.timezone('Asia/Seoul')
        korea_now = utc_now.astimezone(korea_timezone)

        index_list = []
        sql_instance = MySql()
        LOCALE_MST = sql_instance.get_table("""
                                        SELECT *
                                        FROM tb_code_mst
                                        WHERE GROUP_CD = 'B00004'
                                        """) 
        LOCALE_MST = pd.DataFrame(LOCALE_MST)
        LOCALE_MST = LOCALE_MST.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        temp_df = pd.DataFrame(NEW_CONTENTS_MST)

        locale_list = temp_df['locale_cd'].unique().tolist()

        for locale_cd in locale_list :
            LOCALE_MST[LOCALE_MST['ATTRIBUTE3'] == locale_cd.lower()]['CODE_CD']
            nation = LOCALE_MST[LOCALE_MST['ATTRIBUTE3'] == locale_cd.lower()]['CODE_CD'].iloc[0].lower()
            language = LOCALE_MST[LOCALE_MST['ATTRIBUTE3'] == locale_cd.lower()]['ATTRIBUTE4'].iloc[0].lower()
            index_name = nation + '-' + language
            index_list.append(index_name)
        
        prc_list = []
        del_list = []

        if NEW_CONTENTS_MST :
            logger.info(f"####### NEW_CONTENTS_MST : {len(NEW_CONTENTS_MST)}")

            for item in tqdm(NEW_CONTENTS_MST):
                
                item['update_date'] = update_date
                item['update_user'] = 'ADM'

                if "C" in item['user_type'] and \
                item['end_date'].astimezone(korea_now.tzinfo) >= korea_now and \
                item['approval_status'] == "Approval" and \
                item['short_url'] is not None :
                    print("전처리대상")
                    # 처리대상
                    contents_id = str(item['contents_id'])
                    corporation_name = str(item['corp_cd'])
                    language_code = LANGUAGE_MST[LANGUAGE_MST['LOCALE_CD'] == item['locale_cd'].lower()]['LANGUAGE_CD'].iloc[0]
                    language = LANGUAGE_MST[LANGUAGE_MST['LOCALE_CD'] == item['locale_cd'].lower()]['CODE_NAME'].iloc[0]
                    locale_cd = item['locale_cd']
                    product_group_code = item['prod_group_cd']
                    product_code = item['prod_cd']
                    product_code_list = product_code.split(',')
                    nation = ISO_CD[ISO_CD['CODE_CD'] == item['locale_cd'].split('_')[0].upper()]['CODE_CD'].iloc[0]
                    indexer_name = item['locale_cd'].split('_')[0].lower() + '-' + language_code.lower()
                    print(contents_id)

                    if item['expiredyn'] == 'Y' :
                        item['prc_flag'] = 'N'

                        df_item = pd.DataFrame([item])[['contents_id','language','locale_cd','corp_cd','prod_group_cd',
                                'prod_cd','approval_status','expiredyn','user_type','classification_type',
                                'use_yn','regist_date','start_date','end_date','content_update_date','if_create_date',
                                'create_date','create_user','update_date','update_user','short_url','prc_type','prc_flag','prc_status']]
                        items = df_item.to_dict(orient="records")

                        sql_instance = MySql()
                        sql_instance.update_or_insert_data_contents(data_list=items)
                    else :
                        item['prc_flag'] = 'Y'

                        df_item = pd.DataFrame([item])[['contents_id','language','locale_cd','corp_cd','prod_group_cd',
                                'prod_cd','approval_status','expiredyn','user_type','classification_type',
                                'use_yn','regist_date','start_date','end_date','content_update_date','if_create_date',
                                'create_date','create_user','update_date','update_user','short_url','prc_type','prc_flag','prc_status']]
                        
                        items = df_item.to_dict(orient="records")
                        sql_instance = MySql()
                        sql_instance.update_or_insert_data_contents(data_list=items)

                    # 전처리 시작 
                    try :
                        
                        html_data = item['html_contents']
                        for product_code in product_code_list :
                            if product_code =='W/M' :
                                product_code ='WM'
                            file_name = contents_id
                            text_data = BeautifulSoup(html_data, 'html.parser').body.get_text(separator='\n', strip=True)
                                
                            html_blob_path = f"data/{nation}/{language}/contents/{product_group_code}/{product_code}/HTML/{file_name}.html"
                            text_blob_path = f"data/{nation}/{language}/contents/{product_group_code}/{product_code}/TEXT/{file_name}.txt"
                            json_blob_path = f"data/{nation}/{language}/contents/{product_group_code}/{product_code}/JSON/{file_name}.json"
                                
                            as_instance_raw.upload_file(text_data, file_path=text_blob_path, overwrite=True)
                            as_instance_raw.upload_file(html_data, file_path=html_blob_path, overwrite=True)

                            content = text_data
                            content_ = content.split("main_text : ")[-1]  
                            content_ = content_.replace('""','').replace(u"\xa0",u"").replace("\t","").replace("\n ","\n").replace(" \n","\n").replace("\n\n\n","\n").replace("\n\n","\n")

                            video_links = []
                            image_links = []
                            
                            content_ = remove_tag_between(content_, start="<img src=", end=">")

                            tokenizer = tiktoken.get_encoding("cl100k_base")
                            tokenizer = tiktoken.encoding_for_model("text-embedding-ada-002")
                            encoding_result = tokenizer.encode(content_)

                            if len(encoding_result) > 8000 :
                                print("청킹대상")
                                data_type = "chunked_text"
                                chunked_list = chunked_texts(content_, tokenizer=tokenizer, max_tokens=7000, overlap_percentage=0.1)
                                for i, chunk in enumerate(chunked_list) :
                                    
                                    document_structured_1 = \
f"""
(title) {item["contents_name"]}

(keyword) {item["key_list"]}

(symptom) {item["symp_level1_name"]} - {item["symp_level2_name"]}

(content)
{chunk}
""".strip()
                                    text_blob_path = f"data/{nation}/{language}/contents/{product_group_code}/{product_code}/TEXT/{file_name}_{i}.txt"
                                    save_path = text_blob_path.replace('TEXT','structured_1') 
                                    if item['expiredyn'] == 'Y' :
                                        # 처리 대상이 아니므로 삭제
                                        as_instance_preprocessed_docst.delete_file(container='documents', blob=save_path)
                                        logger.info(f"##### delete data {save_path}")
                                        del_list.append(save_path)
                                    else :
                                        print("청킹 전처리")
                                        # 처리대상
                                        as_instance_preprocessed.upload_file(document_structured_1, file_path = save_path, overwrite=True)
                                        as_instance_preprocessed_docst.upload_file(document_structured_1, file_path = save_path, overwrite=True)
                                        logger.info(f"##### preprocess data {save_path}")
                                        item['prc_status'] = 'Y'
                                        item['prc_type'] = 'text'
                                        prc_list.append(save_path)
                                        
                                        df_item = pd.DataFrame([item])[['contents_id','language','locale_cd','corp_cd','prod_group_cd',
                                                'prod_cd','approval_status','expiredyn','user_type','classification_type',
                                                'use_yn','regist_date','start_date','end_date','content_update_date','if_create_date',
                                                'create_date','create_user','update_date','update_user','short_url','prc_type','prc_flag','prc_status']]
                                        items = df_item.to_dict(orient="records")
                                        sql_instance = MySql()
                                        sql_instance.update_or_insert_data_contents(data_list=items)

                                image_video_list = extract_data(html_data, start="src=\"", end="\"" )

                                for link in image_video_list:
                                    if 'image2play' in link:
                                        video_links.append(link)
                                    elif 'gscs.lge.com' in link:
                                        image_links.append(link)

                                contains_image2play = any('image2play' in link for link in image_video_list)
                                contains_gscs_lge = any('gscs.lge.com' in link for link in image_video_list)

                                
                            elif len(encoding_result) <= 8000 and len(content_) < 20 :
                                print("글자수 미달")
                                # main_text가 20글자 미만 ( 처리대상 제외 )
                                item['prc_flag'] = 'N'
                                item['prc_status'] = 'N'
                                item['prc_type'] = 'low_str'
                                image_video_list = extract_data(html_data, start="src=\"", end="\"" )

                                for link in image_video_list:
                                    if 'image2play' in link:
                                        video_links.append(link)
                                    elif 'gscs.lge.com' in link:
                                        image_links.append(link)

                                contains_image2play = any('image2play' in link for link in image_video_list)
                                contains_gscs_lge = any('gscs.lge.com' in link for link in image_video_list)

                                if contains_image2play and not contains_gscs_lge:
                                    item['prc_type'] = 'video'
                                elif contains_gscs_lge and not contains_image2play:
                                    item['prc_type'] = 'image'
                                elif contains_image2play and contains_gscs_lge:
                                    item['prc_type'] = 'image_video'
                                

                                df_item = pd.DataFrame([item])[['contents_id','language','locale_cd','corp_cd','prod_group_cd',
                                        'prod_cd','approval_status','expiredyn','user_type','classification_type',
                                        'use_yn','regist_date','start_date','end_date','content_update_date','if_create_date',
                                        'create_date','create_user','update_date','update_user','short_url','prc_type','prc_flag','prc_status']]
                                
                                items = df_item.to_dict(orient="records")
                                sql_instance = MySql()
                                sql_instance.update_or_insert_data_contents(data=items)
                                logger.info(f"##### No preprocess data {json_blob_path}")
                                save_path = text_blob_path.replace('TEXT','structured_1')
                                as_instance_preprocessed_docst.delete_file(container='documents', blob=save_path)

                                del_list.append(save_path)
                            
                            else :
                                document_structured_1 = \
f"""
(title) {item["contents_name"]}

(keyword) {item["key_list"]}

(symptom) {item["symp_level1_name"]} - {item["symp_level2_name"]}

(content)
{content_}
""".strip()
                                save_path = text_blob_path.replace('TEXT','structured_1') 

                                if item['expiredyn'] == 'Y' :
                                    as_instance_preprocessed_docst.delete_file(container='documents', blob=save_path)
                                    del_list.append(save_path)
                                    logger.info(f"##### delete data {save_path}")
                                    
                                else :
                                    print("전처리대상")
                                    # 처리대상
                                    as_instance_preprocessed.upload_file(document_structured_1, file_path = save_path, overwrite=True)
                                    as_instance_preprocessed_docst.upload_file(document_structured_1, file_path = save_path, overwrite=True)
                                    item['prc_status'] = 'Y'
                                    item['prc_type'] = 'text'
                                    prc_list.append(save_path)
                                    logger.info(f"##### preprocess data {save_path}")

                                    df_item = pd.DataFrame([item])[['contents_id','language','locale_cd','corp_cd','prod_group_cd',
                                            'prod_cd','approval_status','expiredyn','user_type','classification_type',
                                            'use_yn','regist_date','start_date','end_date','content_update_date','if_create_date',
                                            'create_date','create_user','update_date','update_user','short_url','prc_type','prc_flag','prc_status']]                                
                                    items = df_item.to_dict(orient="records")
                                    sql_instance = MySql()
                                    sql_instance.update_or_insert_data_contents(data_list=items)
                                
                                data_type = 'text'
                                image_video_list = extract_data(html_data, start="src=\"", end="\"" )

                                for link in image_video_list:
                                    if 'image2play' in link:
                                        video_links.append(link)
                                    elif 'gscs.lge.com' in link:
                                        image_links.append(link)

                                contains_image2play = any('image2play' in link for link in image_video_list)
                                contains_gscs_lge = any('gscs.lge.com' in link for link in image_video_list)

                            
                            links_data = {"contents_id" : contents_id,
                                            "data_type" : data_type,
                                            "video": video_links,
                                            "image" : image_links,
                                            "text" : content_,
                                            "content_length" : len(content_)}
                            
                            json_data = json.dumps(links_data, ensure_ascii=False, indent=4).encode('utf-8')  
                            as_instance_raw.upload_file(json_data, file_path=json_blob_path, overwrite=True)
                            print(f"{links_data['contents_id']} 전처리 완료, 처리 유형 : {links_data['data_type']}, 경로 :{json_blob_path}")
                            logger.info(f"{links_data['contents_id']} 전처리 완료, 처리 유형 : {links_data['data_type']}, 경로 :{json_blob_path}")

                    except Exception as e :
                        logging.error(str(e))
                        item['prc_status'] = 'N'
                        #item['prc_type'] = 'error'
                        df_item = pd.DataFrame([item])[['contents_id','language','locale_cd','corp_cd','prod_group_cd',
                                'prod_cd','approval_status','expiredyn','user_type','classification_type',
                                'use_yn','regist_date','start_date','end_date','content_update_date','if_create_date',
                                'create_date','create_user','update_date','update_user','short_url','prc_type','prc_flag','prc_status']]
                        items = df_item.to_dict(orient="records")
                        sql_instance = MySql()
                        sql_instance.update_or_insert_data_contents(data_list=items)
                        logger.info(items) 

                else :
                    print("전처리대상 제외")
                    contents_id = str(item['contents_id'])
                    corporation_name = str(item['corp_cd'])
                    language_code = LANGUAGE_MST[LANGUAGE_MST['LOCALE_CD'] == item['locale_cd'].lower()]['LANGUAGE_CD'].iloc[0]
                    language = LANGUAGE_MST[LANGUAGE_MST['LOCALE_CD'] == item['locale_cd'].lower()]['CODE_NAME'].iloc[0]
                    locale_cd = item['locale_cd']
                    product_group_code = item['prod_group_cd']
                    product_code = item['prod_cd']
                    product_code_list = product_code.split(',')
                    nation = item['locale_cd'].split('_')[0]

                    item['prc_flag'] = 'N'
                    item['prc_status'] = 'N'
                    
                    df_item = pd.DataFrame([item])[['contents_id','language','locale_cd','corp_cd','prod_group_cd',
                            'prod_cd','approval_status','expiredyn','user_type','classification_type',
                            'use_yn','regist_date','start_date','end_date','content_update_date','if_create_date',
                            'create_date','create_user','update_date','update_user','short_url','prc_type','prc_flag','prc_status']]
                    items = df_item.to_dict(orient="records")
                    sql_instance = MySql()
                    sql_instance.update_or_insert_data_contents(data_list=items)
                    text_blob_path = f"data/{nation}/{language}/contents/{product_group_code}/{product_code}/TEXT/{contents_id}.txt"
                    save_path = text_blob_path.replace('TEXT','structured_1') 
                    del_list.append(save_path)
                    as_instance_preprocessed_docst.delete_file(container='documents', blob=save_path)
                            
                    logger.info(f"##### No preprocess data : data/{nation}/{language}/contents/{product_group_code}/{product_code}/TEXT/{contents_id}.txt")

        else :
            logger.info(f"####### NO NEW_CONTENTS_MST")
            pass

        result = {'prc_list' : prc_list,
                'del_list' : del_list }

        result_data = json.dumps(result, ensure_ascii=False, indent=4).encode('utf-8')

        return index_list, result_data
    
    def spec_to_storage(self, logger, type='batch', time=None, locale_cd=None) :
        
        logger.info("###### Function Name : spec_to_storage")

        korea_timezone = pytz.timezone('Asia/Seoul')
        current_time_korea = datetime.now(korea_timezone)
        update_date = current_time_korea.strftime('%Y-%m-%d %H:%M:%S')
        
        if time is None :
            korea_timezone = pytz.timezone('Asia/Seoul')
            now = datetime.now(korea_timezone)
            formatted_date = now.strftime('%Y-%m-%d')
        else :
            formatted_date = time
        
        as_instance_preprocessed = AzureStorage(container_name='preprocessed-data', storage_type= 'datast')
        as_instance_preprocessed_docst = AzureStorage(container_name='documents', storage_type='docst')

        if type == 'batch' :
            logger.info("Spec Preprocessing batch")
            parameters = (formatted_date, formatted_date)
            sql_instance = MySql()
            
            SPEC_MST = sql_instance.get_table("""
                                            SELECT DISTINCT tb_if_spec_mst.*, PROD_GROUP_CD, PROD_CD, "N" AS PRC_FLAG, "N" AS PRC_STATUS
                                            FROM tb_if_spec_mst
                                            INNER JOIN tb_sales_prod_map ON tb_if_spec_mst.SALE_CD = tb_sales_prod_map.MATCHED_MODEL_CD
                                            AND tb_if_spec_mst.LOCALE_CD = tb_sales_prod_map.LOCALE_CD
                                            WHERE tb_if_spec_mst.CREATE_DATE >= %s or tb_if_spec_mst.UPDATE_DATE >= %s;
                                            """, parameters=parameters) 
            
            if SPEC_MST is None :
                SPEC_MST = []

            sql_instance = MySql()
            MISSING_SPEC_MST = sql_instance.get_table("""
                                            SELECT DISTINCT tb_if_spec_mst.*, tb_sales_prod_map.PROD_GROUP_CD, tb_sales_prod_map.PROD_CD, tb_chat_spec_prc.PRC_FLAG, tb_chat_spec_prc.PRC_STATUS
                                            FROM tb_if_spec_mst
                                            INNER JOIN tb_sales_prod_map ON tb_if_spec_mst.SALE_CD = tb_sales_prod_map.MATCHED_MODEL_CD
                                            AND tb_if_spec_mst.LOCALE_CD = tb_sales_prod_map.LOCALE_CD
                                            INNER JOIN tb_chat_spec_prc ON tb_if_spec_mst.SALE_CD = tb_chat_spec_prc.SALE_CD
                                            AND tb_if_spec_mst.LOCALE_CD = tb_chat_spec_prc.LOCALE_CD
                                            WHERE tb_chat_spec_prc.PRC_FLAG = 'Y' AND tb_chat_spec_prc.PRC_STATUS = 'N'
                                            """)
            
            if MISSING_SPEC_MST is not None :
                SPEC_MST = SPEC_MST + MISSING_SPEC_MST

        else :
            logger.info("Spec Preprocessing locale")
            parameters = (locale_cd.upper(),)
            sql_instance = MySql()
            SPEC_MST = sql_instance.get_table("""
                                            SELECT DISTINCT tb_if_spec_mst.*, PROD_GROUP_CD, PROD_CD, "N" AS PRC_FLAG, "N" AS PRC_STATUS
                                            FROM tb_if_spec_mst
                                            INNER JOIN tb_sales_prod_map ON tb_if_spec_mst.SALE_CD = tb_sales_prod_map.MATCHED_MODEL_CD
                                            AND tb_if_spec_mst.LOCALE_CD = tb_sales_prod_map.LOCALE_CD
                                            WHERE tb_if_spec_mst.LOCALE_CD = %s
                                            """, parameters=parameters)

        SPEC_MST = pd.DataFrame(SPEC_MST)
        SPEC_MST = SPEC_MST.sort_values(['LOCALE_CD','SALE_CD', 'GROUP_TITLE_NAME', 'SUB_TITLE_NAME']).reset_index(drop=True)

        sql_instance = MySql()
        LOCALE_MST = sql_instance.get_table("""
                                        SELECT *
                                        FROM tb_code_mst
                                        WHERE GROUP_CD = 'B00004'
                                        """) 
        LOCALE_MST = pd.DataFrame(LOCALE_MST)
        LOCALE_MST = LOCALE_MST.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        locale_list = SPEC_MST['LOCALE_CD'].unique().tolist()
        index_list = []
        
        for locale_cd in locale_list :
            LOCALE_MST[LOCALE_MST['ATTRIBUTE3'] == locale_cd.lower()]['CODE_CD']
            nation = LOCALE_MST[LOCALE_MST['ATTRIBUTE3'] == locale_cd.lower()]['CODE_CD'].iloc[0].lower()
            language = LOCALE_MST[LOCALE_MST['ATTRIBUTE3'] == locale_cd.lower()]['ATTRIBUTE4'].iloc[0].lower()
            index_name = nation + '-' + language
            index_list.append(index_name)

        sql_instance = MySql()
        LANGUAGE_MST = sql_instance.get_table("""
                                            SELECT CORP_CD, LOCALE_CD, LANGUAGE_CD, CODE_NAME
                                            FROM tb_corp_lan_map
                                            JOIN tb_code_mst ON tb_code_mst.CODE_CD = tb_corp_lan_map.LANGUAGE_CD
                                            WHERE GROUP_CD = 'B00003'
                                            """)
        LANGUAGE_MST = pd.DataFrame(LANGUAGE_MST)
        LANGUAGE_MST = LANGUAGE_MST.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        sql_instance = MySql()
        MODEL_INFO = sql_instance.get_table("""
                                        SELECT LOCALE_CD, SALES_CD, MATCHED_MODEL_CD, PROD_GROUP_CD, PROD_CD
                                        FROM tb_sales_prod_map
                                        """)
        MODEL_INFO = pd.DataFrame(MODEL_INFO)

        sql_instance = MySql()
        ISO_CD = sql_instance.get_table("""
                                        SELECT *
                                        FROM tb_code_mst
                                        WHERE GROUP_CD = 'B00002';
                                        """)
        ISO_CD = pd.DataFrame(ISO_CD)
        ISO_CD = ISO_CD.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        spec_sales_cd_list = SPEC_MST['SALE_CD'].unique().tolist()

        prc_list = []
        error_list = []
        del_list = []

        if spec_sales_cd_list :

            print(len(spec_sales_cd_list))

            for sales_cd in tqdm(spec_sales_cd_list) :
                
                logger.info(sales_cd)
                print(sales_cd)
                data_tmp = SPEC_MST[SPEC_MST['SALE_CD'] == sales_cd]
                info = data_tmp[['PROD_GROUP_CD','PROD_CD']].drop_duplicates()

                #print(data_tmp)
                
                for i in range(len(info)) :
                    if len(data_tmp) == 0 :
                        continue
                    
                    logger.info(len(info))
                    print(len(info))
                    
                    data = data_tmp[(data_tmp['PROD_GROUP_CD'] == info.iloc[i]['PROD_GROUP_CD'])&(data_tmp['PROD_CD'] == info.iloc[i]['PROD_CD'])]
                    data['UPDATE_DATE'] = update_date
                    data['UPDATE_USER'] = 'ADM'
                    data = data.dropna(subset=['GROUP_TITLE_NAME','SUB_TITLE_NAME','SPEC_VALUE_NAME'])

                    if len(data) == 0 :
                        continue
                        
                    use_yn = data['USE_FLAG'].iloc[0]
    
                    if use_yn == 'N':
                        data['PRC_FLAG'] = 'Y'
                    else :
                        data['PRC_FLAG'] = 'N'
    
                    df_items = data[['LOCALE_CD','SALE_CD','USE_FLAG','UPDATE_DATE','UPDATE_USER','PROD_GROUP_CD','PROD_CD','PRC_FLAG','PRC_STATUS']].drop_duplicates()
                    items = df_items.to_dict(orient="records")
                    sql_instance = MySql()
                    sql_instance.update_or_insert_data_spec(data_list=items)
                    
                    product_group_code = info.iloc[i]['PROD_GROUP_CD']
                    product_code = info.iloc[i]['PROD_CD']
    
                    if product_code == 'W/M':
                        product_code = 'WM'
    
                    locale_list = data['LOCALE_CD'].unique().tolist()
    
                    for locale_cd in locale_list :
                        
                        sale_cd_list = MODEL_INFO[(MODEL_INFO['MATCHED_MODEL_CD'] == sales_cd)&\
                            (MODEL_INFO['LOCALE_CD'] == locale_cd)&\
                            (MODEL_INFO['PROD_GROUP_CD'] == product_group_code)&\
                            (MODEL_INFO['PROD_CD'] == product_code)]['SALES_CD'].tolist()

                        sale_cd_list = list(set(sale_cd_list))
                        sale_cd_str = ', '.join(sale_cd_list)
                        
                        nation = ISO_CD[ISO_CD['CODE_CD'] == locale_cd.split('_')[0].upper()]['CODE_CD'].iloc[0]
                        
                        language = LANGUAGE_MST[LANGUAGE_MST['LOCALE_CD'] == locale_cd.lower()]['CODE_NAME'].iloc[0].strip()
                        group_title_name_list = data['GROUP_TITLE_NAME'].unique().tolist()
    
                        main_text = ""
                        main_text += f'Model Code : {sale_cd_str} \n\nBelow is specification information for the above models. \n'
                        for group_title in group_title_name_list :
                            main_text += '\n[ ' +group_title+' ]\n\n'
                            sub_data = data[data['GROUP_TITLE_NAME'] == group_title][['SUB_TITLE_NAME','SPEC_VALUE_NAME']].drop_duplicates(subset=['SUB_TITLE_NAME'])
                            for i in range(len(sub_data)):
                                main_text += '- ' + sub_data.iloc[i]['SUB_TITLE_NAME'] +' : '+sub_data.iloc[i]['SPEC_VALUE_NAME'] + '\n'
    
                        tokenizer = tiktoken.get_encoding("cl100k_base")
                        tokenizer = tiktoken.encoding_for_model("text-embedding-ada-002")
                        encoding_result = tokenizer.encode(main_text)
    
                        if len(encoding_result) > 8000 :
                            chunked_list = chunked_texts(main_text, tokenizer=tokenizer, max_tokens=7000, overlap_percentage=0.1)
                            for i, chunk in enumerate(chunked_list) :
                                if i != 0 :
                                    chunk += f'\n\nModel Code : {sale_cd_str} \n\nBelow is specification information for the above models. \n'
                                
                                preprocessed_path = f"data/{nation}/{language}/spec/{product_group_code}/{product_code}/{sales_cd}_{i}.txt"
                                docst_path = f"data/{nation}/{language}/spec/{product_group_code}/{product_code}/structured_1/{sales_cd}_{i}.txt"
    
                                if use_yn == 'Y':
                                    as_instance_preprocessed_docst.delete_file(container='documents', blob=docst_path)
                                    del_list.append(docst_path)
                                    df_items['PRC_STATUS'] = 'N'
    
                                    items = df_items.to_dict(orient="records")
    
                                    sql_instance = MySql()
                                    sql_instance.update_or_insert_data_spec(data_list=items)
                                    print(docst_path)
                                    logger.info(f"##### delete data {docst_path}")
                                    logger.info(json.dumps(items,indent=4)) 
                                else :
                                
                                    as_instance_preprocessed.upload_file(chunk, file_path = preprocessed_path, overwrite=True)
                                    as_instance_preprocessed_docst.upload_file(chunk, file_path = docst_path, overwrite=True)
                                    prc_list.append(docst_path)
    
                                    df_items['PRC_STATUS'] = 'Y'
                                    items = df_items.to_dict(orient="records")
    
                                    sql_instance = MySql()
                                    sql_instance.update_or_insert_data_spec(data_list=items)
                                    print(docst_path)
                                    logger.info(f"##### preprocess data {docst_path}")
                                    logger.info(json.dumps(items,indent=4)) 
                        else :
                            preprocessed_path = f"data/{nation}/{language}/spec/{product_group_code}/{product_code}/{sales_cd}.txt"
                            docst_path = f"data/{nation}/{language}/spec/{product_group_code}/{product_code}/structured_1/{sales_cd}.txt"
                            
                            if use_yn == 'Y':
                                as_instance_preprocessed_docst.delete_file(container='documents', blob=docst_path)
                                del_list.append(docst_path)
    
                                df_items['PRC_STATUS'] = 'N'
                                items = df_items.to_dict(orient="records")
                                sql_instance = MySql()
                                sql_instance.update_or_insert_data_spec(data_list=items)
                                print(docst_path)
                                logger.info(f"##### delete data {docst_path}")
                                logger.info(json.dumps(items,indent=4)) 
                            
                            else :            
    
                                as_instance_preprocessed.upload_file(main_text, file_path = preprocessed_path, overwrite=True)
                                as_instance_preprocessed_docst.upload_file(main_text, file_path = docst_path, overwrite=True)
    
                                df_items['PRC_STATUS'] = 'Y'
                                items = df_items.to_dict(orient="records")
    
                                sql_instance = MySql()
                                sql_instance.update_or_insert_data_spec(data_list=items)
                                prc_list.append(docst_path)
                                print(docst_path)
                                logger.info(f"##### preprocess data {docst_path}")
                                logger.info(json.dumps(items,indent=4)) 

        
        else :
            logger.info(f"##### NO NEW SPEC DATA")
            pass
        
        result = {'prc_list' : prc_list,
                    'del_list' : del_list }

        result_data = json.dumps(result, ensure_ascii=False, indent=4).encode('utf-8')
        return  index_list, result_data


    def microsites_to_storage(self, logger, type='batch', time=None, corp_cd=None) :
    
        logger.info("###### Function Name : microsites_to_storage")
        
        korea_timezone = pytz.timezone('Asia/Seoul')
        current_time_korea = datetime.now(korea_timezone)
        update_date = current_time_korea.strftime('%Y-%m-%d %H:%M:%S')

        as_instance_raw = AzureStorage(container_name='raw-data', storage_type='datast')
        as_instance_preprocessed_docst = AzureStorage(container_name='documents', storage_type='docst')
        
        if time is None :
            korea_timezone = pytz.timezone('Asia/Seoul')
            now = datetime.now(korea_timezone)
            formatted_date = now.strftime('%Y-%m-%d')
            
        else :
            formatted_date = time
        
        if type == 'batch' :
        
            parameters = (formatted_date, formatted_date)
        
            sql_instance = MySql()
            MICROSITES_MST = sql_instance.get_table("""
                                                    SELECT *, null AS PRC_TYPE, "N" AS PRC_FLAG, "N" AS PRC_STATUS
                                                    FROM tb_chat_microsite
                                                    WHERE create_date >= %s or update_date >= %s;
                                                    """, parameters=parameters) 
        
            if MICROSITES_MST is None :
                MICROSITES_MST = []
        
            sql_instance = MySql()
            MISSING_MICROSITES_MST = sql_instance.get_table(query="""
                                                                SELECT DISTINCT tb_chat_microsite.*, tb_chat_microsite_prc.PRC_TYPE, tb_chat_microsite_prc.PRC_FLAG, tb_chat_microsite_prc.PRC_STATUS
                                                                FROM tb_chat_microsite
                                                                INNER JOIN tb_chat_microsite_prc ON tb_chat_microsite.MICRO_ID = tb_chat_microsite_prc.MICRO_ID
                                                                WHERE tb_chat_microsite_prc.PRC_FLAG = 'Y' AND tb_chat_microsite_prc.PRC_STATUS = 'N'
                                                                """)
            
            if MISSING_MICROSITES_MST is not None :
                NEW_MICROSITES_MST = MICROSITES_MST + MISSING_MICROSITES_MST
        
        else :
        
            parameters = (corp_cd.upper(),)
            sql_instance = MySql()
            NEW_MICROSITES_MST = sql_instance.get_table(query="""
                                                            SELECT *, null AS PRC_TYPE, "N" AS PRC_FLAG, "N" AS PRC_STATUS
                                                            FROM tb_chat_microsite
                                                            WHERE CORP_CD = %s;
                                                            """, parameters=parameters)
    
        NEW_MICROSITES_MST = pd.DataFrame(NEW_MICROSITES_MST)
        NEW_MICROSITES_MST = NEW_MICROSITES_MST[(~NEW_MICROSITES_MST['PROD_GROUP_CD'].isna())&~(NEW_MICROSITES_MST['PROD_CD'].isna())].reset_index(drop=True)
        
        sql_instance = MySql()
        LANGUAGE_MST = sql_instance.get_table("""
                                                    SELECT CORP_CD, LOCALE_CD, LANGUAGE_CD, CODE_NAME
                                                    FROM tb_corp_lan_map
                                                    JOIN tb_code_mst ON tb_code_mst.CODE_CD = tb_corp_lan_map.LANGUAGE_CD
                                                    WHERE GROUP_CD = 'B00003'
                                                    """)
        LANGUAGE_MST = pd.DataFrame(LANGUAGE_MST)
        LANGUAGE_MST = LANGUAGE_MST.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
        prc_list = []
        del_list = []
        index_list = []
        
        if len(NEW_MICROSITES_MST) > 0 :
            
            for index, item in NEW_MICROSITES_MST.iterrows():
                item['UPDATE_DATE'] = update_date
                item['UPDATE_USER'] = 'ADM'
            
                if item['USE_YN'] == 'Y':
                    item['PRC_FLAG'] = 'Y'
                else :
                    item['PRC_FLAG'] = 'N'
                
                df_items = pd.DataFrame(item[['MICRO_ID','CORP_CD','PROD_GROUP_CD','PROD_CD','LANGUAGE_CD','USE_YN','CREATE_DATE','CREATE_USER','UPDATE_DATE','UPDATE_USER','PRC_TYPE','PRC_FLAG','PRC_STATUS']]).T.drop_duplicates()
                items = df_items.to_dict(orient="records")
                sql_instance = MySql()
                sql_instance.update_or_insert_data_microsite(data_list=items)
                
                micro_id = item['MICRO_ID']
                language_cd = item['LANGUAGE_CD']
                nation = LANGUAGE_MST[(LANGUAGE_MST['CORP_CD']==item['CORP_CD'])&(LANGUAGE_MST['LANGUAGE_CD']==item['LANGUAGE_CD'].lower())]['LOCALE_CD'].iloc[0].split('-')[0].upper()
                language = LANGUAGE_MST[(LANGUAGE_MST['CORP_CD']==item['CORP_CD'])&(LANGUAGE_MST['LANGUAGE_CD']==item['LANGUAGE_CD'].lower())]['CODE_NAME'].iloc[0]
                index_name = nation.lower()+'-'+language_cd.lower()
                index_list.append(index_name)
                
                product_group_code = item['PROD_GROUP_CD']
                product_code_list = [cd.strip() for cd in item['PROD_CD'].split(',')]
                main_text, image_urls, video_urls = extract_main_text_microsite(item['MICRO_URL'])
                title = item['CONTENTS_TITLE']
                
                for product_code in product_code_list :
                    tokenizer = tiktoken.get_encoding("cl100k_base")
                    tokenizer = tiktoken.encoding_for_model("text-embedding-ada-002")
                    encoding_result = tokenizer.encode(main_text)
                
                    if len(encoding_result) > 8000 :
                        data_type = 'chunk_text'
                        chunked_list = chunked_texts(main_text, tokenizer=tokenizer, max_tokens=7000, overlap_percentage=0.1)
                        for i, chunk in enumerate(chunked_list) :
                            document_structured_1 = \
f"""
(title) {title}

(content){main_text}
""".strip()
                
                            save_path = f"data/{nation}/{language}/contents/{product_group_code}/{product_code}/structured_1/{micro_id}_{i}.txt"
                            if item['USE_YN'] == 'Y' :
                                as_instance_preprocessed_docst.upload_file(document_structured_1, file_path = save_path, overwrite=True)
                                df_items['PRC_STATUS'] = 'Y'
                                df_items['PRC_TYPE'] = data_type
                                items = df_items.to_dict(orient="records")
                                sql_instance = MySql()
                                sql_instance.update_or_insert_data_microsite(data_list=items)
                                prc_list.append(save_path)
                                print(f"전처리 완료 : {save_path}")
                            else :
                                as_instance_preprocessed_docst.delete_file(container='documents', blob=save_path)
                                df_items['PRC_STATUS'] = 'N'
                                df_items['PRC_TYPE'] = data_type
                                items = df_items.to_dict(orient="records")
                                sql_instance = MySql()
                                sql_instance.update_or_insert_data_microsite(data_list=items)
                                del_list.append(save_path)
                                print(f"삭제 완료 : {save_path}")
                
                    
                    elif len(encoding_result) <= 8000 and len(main_text) < 20 :
                        
                        if image_urls and video_urls:
                            data_type = "image&video"
                        elif image_urls:
                            data_type = "image"
                        elif video_urls:
                            data_type = "video"
                        else:
                            data_type = "low_str"
                        
                        as_instance_preprocessed_docst.delete_file(container='documents', blob=save_path)
                        df_items['PRC_STATUS'] = 'N'
                        df_items['PRC_TYPE'] = data_type
                        items = df_items.to_dict(orient="records")
                        sql_instance = MySql()
                        sql_instance.update_or_insert_data_microsite(data_list=items)
                        del_list.append(save_path)
                        print(f"삭제 완료 : {save_path}")
                
                
                    else :
                        data_type = 'text'
                        document_structured_1 = \
f"""
(title) {title}

(content){main_text}
""".strip()
                
                        save_path = f"data/{nation}/{language}/microsites/{product_group_code}/{product_code}/structured_1/{micro_id}.txt"
                        if item['USE_YN'] == 'Y' :
                            as_instance_preprocessed_docst.upload_file(document_structured_1, file_path = save_path, overwrite=True)
                            df_items['PRC_STATUS'] = 'Y'
                            df_items['PRC_TYPE'] = data_type
                            items = df_items.to_dict(orient="records")
                            sql_instance = MySql()
                            sql_instance.update_or_insert_data_microsite(data_list=items)
                            prc_list.append(save_path)
                            print(f"전처리 완료 : {save_path}")
                        else :
                            as_instance_preprocessed_docst.delete_file(container='documents', blob=save_path)
                            df_items['PRC_STATUS'] = 'N'
                            df_items['PRC_TYPE'] = data_type
                            items = df_items.to_dict(orient="records")
                            sql_instance = MySql()
                            sql_instance.update_or_insert_data_microsite(data_list=items)
                            del_list.append(save_path)
                            print(f"삭제 완료 : {save_path}")
                
                    links_data = {"micro_id" : micro_id,
                                    "url" : item['MICRO_URL'],
                                "data_type" : data_type,
                                "video": video_urls,
                                "image" : image_urls,
                                "text" : document_structured_1,
                                "content_length" : len(document_structured_1)}
                
                    json_blob_path = f"data/{nation}/{language}/microsites/{product_group_code}/{product_code}/JSON/{micro_id}.json"
                    json_data = json.dumps(links_data, ensure_ascii=False, indent=4).encode('utf-8')
                    as_instance_raw.upload_file(json_data, file_path=json_blob_path, overwrite=True)
                    print(json_blob_path)
        
        else :
            logger.info(f"##### NO NEW SPEC DATA")
            pass
        
        result = {'prc_list' : prc_list,
                    'del_list' : del_list }

        result_data = json.dumps(result, ensure_ascii=False, indent=4).encode('utf-8')
        index_list = list(set(index_list))
        return  index_list, result_data
        
    def general_inquiry_to_storage(self, logger, locale_cd, intent_code=None):
        logger.info("###### Function Name : general_inquiry_to_storage")
        as_instance_raw = AzureStorage(container_name='raw-data', storage_type='datast')
        as_instance_preprocessed = AzureStorage(container_name='preprocessed-data', storage_type='datast')
        as_instance_preprocessed_docst = AzureStorage(container_name='documents', storage_type='docst')
        PRODUCT_MST = as_instance_raw.read_file(file_path="Contents_Manual_List_Mst_Data/PRODUCT_MST.csv")

        sql_instance = MySql()
        locale_mst_table = sql_instance.get_table("""
                                                SELECT CORP_CD, LOCALE_CD, LANGUAGE_CD, CODE_NAME
                                                FROM tb_corp_lan_map
                                                JOIN tb_code_mst ON tb_code_mst.CODE_CD = tb_corp_lan_map.LANGUAGE_CD
                                                WHERE GROUP_CD = 'B00003'
                                                """)
        LOCALE_MST = pd.DataFrame(locale_mst_table)
        LOCALE_MST = LOCALE_MST.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        sql_instance = MySql()
        ISO_CD = sql_instance.get_table("""
                                        SELECT *
                                        FROM tb_code_mst
                                        WHERE GROUP_CD = 'B00002';
                                        """)
        ISO_CD = pd.DataFrame(ISO_CD)
        ISO_CD = ISO_CD.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        language = LOCALE_MST[LOCALE_MST['LOCALE_CD']==locale_cd]['CODE_NAME'].values[0]
        nation = ISO_CD[ISO_CD['CODE_CD'] == locale_cd.split('_')[0].upper()]['CODE_CD'].iloc[0]
        language_code = LOCALE_MST[LOCALE_MST['LOCALE_CD']==locale_cd]['LANGUAGE_CD'].values[0].lower()
        
        product_list = (PRODUCT_MST['PROD_GROUP_CD'] +'/'+ PRODUCT_MST['PROD_CD']).drop_duplicates().tolist()
        sql_instance = MySql()
        
        if intent_code == None :
            parameters = (locale_cd,)
            INTENT_MST = sql_instance.get_table("""
                                                SELECT *
                                                FROM tb_chat_intent_mst
                                                WHERE USE_YN = 'Y' AND LOCALE_CD = %s
                                                """, parameters=parameters)
        else :
            
            parameters = (locale_cd, intent_code)
            INTENT_MST = sql_instance.get_table("""
                                                SELECT *
                                                FROM tb_chat_intent_mst
                                                WHERE USE_YN = 'Y' AND LOCALE_CD = %s AND INTENT_CODE = %s
                                                """, parameters=parameters)     
            
        for data in INTENT_MST :
            json_data = {"INTENT_NAME" : data['INTENT_NAME'],
                        "INTENT_CODE" : data['INTENT_CODE'],
                        "chatbot_response" : data['CHATBOT_RESPONSE'],
                        "related_link_name" : data['RELATED_LINK_NAME'],
                        "related_link_url" : data['RELATED_LINK_URL'],
                        "EVENT_CD" : data['EVENT_CD'],
                        "symptom" : data['SYMPTOM']}

            json_bytes = json.dumps(json_data).encode('utf-8')
            # 파일저장 임시 수정필요
            for product in product_list :
                as_instance_preprocessed.upload_file(json_bytes, file_path=f"data/{nation}/{language}/general-inquiry/{product}/{json_data['INTENT_CODE']}.json", overwrite=True)
                as_instance_preprocessed_docst.upload_file(json_bytes, file_path=f"data/{nation}/{language}/general-inquiry/{product}/{json_data['INTENT_CODE']}.json", overwrite=True)
                logger.info(f"data/{nation}/{language}/general-inquiry/{product}/{json_data['INTENT_CODE']}.json , upload complete")
                print(f"data/{nation}/{language}/general-inquiry/{product}/{json_data['INTENT_CODE']}.json , upload complete")
            else :
                pass
        
        return logger.info("upload complete")
    
    def pdf_to_image_html(self, logger, corporation_name, language_name) :

        logger.info("###### Function Name : pdf_to_image_html")
        as_instance_web = AzureStorage(container_name='$web', storage_type='docst')
        web_container_client = as_instance_web.container_client
        as_instance_raw = AzureStorage(container_name='raw-data', storage_type='datast')
        pdf_list = as_instance_raw.extract_blob_list(blob_path=f"manual_output_{corporation_name}_240207/{corporation_name}/{language_name}", file_type='pdf')
        product_to_check = as_instance_raw.read_file('Contents_Manual_List_Mst_Data/PRODUCT_MST.csv')
        sql_instance = MySql()
        locale_mst_table = sql_instance.get_table("""
                                                SELECT CORP_CD, LOCALE_CD, LANGUAGE_CD, CODE_NAME
                                                FROM tb_corp_lan_map
                                                JOIN tb_code_mst ON tb_code_mst.CODE_CD = tb_corp_lan_map.LANGUAGE_CD
                                                WHERE GROUP_CD = 'B00003'
                                                """)
        LOCALE_MST = pd.DataFrame(locale_mst_table)
        LOCALE_MST = LOCALE_MST.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        for file_path in pdf_list :
            pdf_data = as_instance_raw.read_file(file_path=file_path)
            language = file_path.split('/')[2]
            product_code = file_path.split('/')[3]
            product_group_code = product_to_check[product_to_check['PROD_CD'] == product_code]['PROD_GROUP_CD'].iloc[0].strip()
            item_id = file_path.split('/')[-1].split('.')[0]
            iso_cd_list = [item.upper() for item in LOCALE_MST[(LOCALE_MST['CORP_CD']==corporation_name)&(LOCALE_MST['CODE_NAME']==language_name)]['LOCALE_CD'].drop_duplicates().str.split('_').str[0].tolist()]
            try :
                doc = fitz.open(stream=pdf_data)
                
            except:
                logger.info(f"Broken PDF : {file_path}")
                continue
            logger.info(f"Converting pdf to image, PDF : {file_path}")
            
            for i, page in enumerate(doc):
                svg = page.get_svg_image(matrix=fitz.Identity)
                svg_bytes = svg.encode('utf-8')
                image_data = io.BytesIO(svg_bytes)
                for iso_cd in iso_cd_list :
                    save_path = f'data/{iso_cd}/{language}/{product_group_code}/{product_code}/{item_id}/images/{i+1}.svg'
                    image_data.seek(0)
                    as_instance_web.upload_file(image_data, file_path = save_path, content_type="image/svg+xml", overwrite=True)
            
            for iso_cd in iso_cd_list :
                img_url_list = []
                save_path = f'data/{iso_cd}/{language}/{product_group_code}/{product_code}/{item_id}/images/'
                image_list = as_instance_web.extract_blob_list(blob_path=save_path, file_type='svg')
                if len(image_list) == 0 :
                    logger.info(f"No Images : {save_path}")
                    continue
                else :
                    for image_path in image_list :
                        image_url = web_container_client.get_blob_client(image_path).url
                        img_url_list.append({"page" : image_path.split('/')[-1].split('.')[0],
                                            "img_url" : as_instance_web.get_sas_url(image_path,expiry_time=False)})
                    img_url_list = sorted(img_url_list, key=lambda x: int(x['page']))
            
                    html_content = "<html><body style='background-color: white;'>"
            
                    for url in img_url_list:
                        html_content += f'<div id="page{url["page"]}" style="text-align: center;"><img src="{url["img_url"]}" style="display: inline-block; margin: 10px auto;"></div>\n'
            
                    html_content += "</body></html>"
                    file_name = f"{item_id}.html"
                    blob_name = f'data/{iso_cd}/{language}/{product_group_code}/{product_code}/{item_id}/html/{file_name}'
                    logger.info(blob_name)
                    as_instance_web.upload_file(html_content, file_path=blob_name, overwrite=True, content_type=("text/html"))


        return logger.info("Successfully converted pdf to image, html")
    

    def save_pdfwriter(self, logger, sas_url, page_list, as_instnace, blob_name):
        logger.info("###### Function Name : save_pdfwriter")
        cont_client = as_instnace.container_client
        blob_client = cont_client.get_blob_client(blob_name)
        
        if len(page_list) == 0:
            blob_client.upload_blob_from_url(source_url = sas_url, overwrite=True)
        
        else :
            with urlopen(sas_url) as f:
                pdf_data = f.read()
            reader = PdfReader(io.BytesIO(pdf_data))
            
            writer = PdfWriter()
            for pnum in page_list:
                writer.add_page(reader.pages[pnum])
            
            with io.BytesIO() as bytes_stream:
                writer.write(bytes_stream)
                bytes_stream.seek(0)
                blob_client.upload_blob(bytes_stream, overwrite = True)    

    # [2024.04.25] 수정 - as_instance_raw 함수 내부에서 선언
    def get_metadata(self, logger, df):
        logger.info("###### Function Name : get_metadata")
        as_instance_raw = AzureStorage(container_name='raw-data', storage_type= 'datast')
        cont_client = as_instance_raw.container_client # raw
        df = df.reset_index(drop=True)
        for idx in df.index :
            idx_x = df.loc[idx]
            bname = f"{idx_x['BLOB_PATH']}/{idx_x['BLOB_FILE_NAME']}"
            blob_client = cont_client.get_blob_client(bname)
            
            byte_size = blob_client.get_blob_properties().size
            df.loc[idx, "BYTE_SIZE"] = byte_size
            df.loc[idx, "LANG_LIST_MULTI_YN"]  = "Y" if "," in idx_x['LANGUAGE_LIST'] else "N"
            
            if idx_x['EXT_NAME'] == 'pdf':
                sas_url = as_instance_raw.get_sas_url(blob_path=bname)
                try :
                    request_blob = requests.get(sas_url)
                    filestream   = io.BytesIO(request_blob.content)
                    with fitz.open(stream=filestream, filetype="pdf") as pdf_data:
                        num_pages   = len(pdf_data)
                        layout_info = ["width" if page.rect.width > page.rect.height else "height" for page in pdf_data]

                    if Counter(layout_info).most_common()[0] == ("width", num_pages) :
                        # 전체 페이지가 가로형인 경우
                        layout_str = "width_layout"
                    else :
                        layout_str = "normal"
                
                except Exception as e :
                    # -- LOG ?
                    num_pages  = -999
                    layout_str = "error"
                
                df.loc[idx, "NUM_PAGES"]   = num_pages
                df.loc[idx, "LAYOUT_TYPE"] = layout_str                
        
        return df
    
    # [2024.04.25] 추가
    def merge_pdfs(self, logger, df) :
        logger.info("###### Function Name : merge_pdfs")
        as_instance_raw = AzureStorage(container_name='raw-data', storage_type= 'datast')
        writer = PdfWriter()
        try :
            for m_idx in df.index:    
                sas_url = as_instance_raw.get_sas_url(blob_path = f"{df.loc[m_idx]['INNER_FILE_BLOB_PATH']}/{df.loc[m_idx]['INNER_FILE_EXT']}")
                
                if (((df.loc[m_idx]['USE_YN'] =='Y') & (df.loc[m_idx]['SPLIT_YN'] =='N')) |
                        ((df.loc[m_idx]['USE_YN'] =='Y') & (df.loc[m_idx]['SPLIT_YN'] ==''))):
                    # pdf - 그대로 사용
                    with urlopen(sas_url) as f:
                        pdf_data = f.read()
                    reader = PdfReader(io.BytesIO(pdf_data))
                    for page in reader.pages:
                        writer.add_page(page)
                
                elif ((df.loc[m_idx]['USE_YN'] =='Y') & (df.loc[m_idx]['SPLIT_YN'] =='Y')) :
                    # pdf - 분할
                    with urlopen(sas_url) as f:
                        pdf_data = f.read()
                    reader = PdfReader(io.BytesIO(pdf_data))
                    
                    str_prange = df.loc[m_idx]['PAGE_RANGE']
                    for pr in str_prange.split(","):
                        tmp_st_pnum, tmp_ed_pnum = pr.split("-")
                        for pnum in range(int(tmp_st_pnum)-1, int(tmp_ed_pnum)) :
                            writer.add_page(reader.pages[pnum])
            # end for-loop
        except Exception as e:
            return 
        
        return writer
    
    # [2024.04.25] 추가
    def merge_html_and_text(self, logger, blob_path:str, html_bname_list:list, filter:dict):
        logger.info("###### Function Name : merge_html_and_text")
        # filter: dict --> {'fld_name' : "de-DE", 'div_id' : 1}
        as_instance_raw = AzureStorage(container_name='raw-data', storage_type= 'datast')
        related_bname_list = as_instance_raw.container_client.list_blobs(name_starts_with=blob_path)
        related_bname_list =  [i.name for i in related_bname_list]
        
        # 챕터별 html 및 텍스트 묶기
        ch_doc = BeautifulSoup()
        ch_doc.append(ch_doc.new_tag("html"))
        ch_doc.html.append(ch_doc.new_tag("body"))
        ch_gtext = ""
        for html_bname in html_bname_list:
            file_client = as_instance_raw.container_client.get_blob_client(f"{blob_path}/{html_bname}")
            with io.BytesIO() as blob_io:
                file_client.download_blob().readinto(blob_io)
                blob_io.seek(0)
                soup = BeautifulSoup(blob_io, 'html.parser')
            
            # 1. 텍스트 추출
            paragraphs = soup.find_all('p')
            p_texts = " ".join([p.text for p in paragraphs])
            ch_gtext = ch_gtext + "\n" + p_texts
            
            # 2. html 내용 정리
            # - 링크가 존재하는 경우 삭제
            for a in soup.find_all('a', href=True):
                if 'href' in a.attrs:
                    del a.attrs['href']

            # - 이미지 src를 sas_url로 변경
            for img in soup.find_all('img'):
                src_name = img.attrs['src']
                src_bnames = [x for x in related_bname_list if src_name.replace("../../", "") in x]
                if len(src_bnames) > 1 :
                    src_bnames = [x for x in src_bnames if filter['fld_name'] in x]
                elif len(src_bnames) == 0:
                    continue
                
                # -- 매핑된 이미지 sas url 생성
                img_sas_url = as_instance_raw.get_sas_url(blob_path=src_bnames[0], expiry_time=False)
                img.attrs['src'] = img_sas_url
            # end for-loop : soup.find_all('img')
            ch_doc.body.extend(soup.body)
        # end for-loop : html_bname_list
        ch_gtext = re.sub(" +", " ", ch_gtext)
        ch_gtext = f"<page:{filter['div_id']}>\n" + ch_gtext.strip() + f"</page:{filter['div_id']}>\n"
        return ch_doc, ch_gtext

    # [2024.04.25] 추가
    def save_merged_htmls(logger, htmls_dict, save_bname):
        logger.info("###### Function Name : save_merged_htmls")
        as_instance_raw = AzureStorage(container_name='raw-data', storage_type= 'datast')
        output_doc = BeautifulSoup()
        output_doc.append(output_doc.new_tag("html"))
        output_doc.html.append(output_doc.new_tag("body"))
        for div_id, doc in htmls_dict.items():
            # -- output_doc에 doc(챕터별 HTML) append
            div_tag = output_doc.new_tag('div', id=f'page{div_id}')
            div_tag.insert(0, doc.body)
            output_doc.body.append(div_tag)
        blob_client = as_instance_raw.container_client.get_blob_client(save_bname)
        blob_client.upload_blob(str(output_doc), content_type=("text/html"), overwrite=True)
        
    # [2024.04.24] 수정
    # [2024.04.25] 수정 + as_instance_raw 선언문 함수내부에 추가
    def download_manual(self, logger, df):
        logger.info("###### Function Name : download_manual")
        as_instance_raw = AzureStorage(container_name='raw-data', storage_type= 'datast')
        raw_cont_client = as_instance_raw.container_client # raw

        BASE_URL         = 'https://gscs-b2c.lge.com/downloadFile?fileId='
        ROOT_FOLDER_NAME = 'testhjs/manual'
        korea_timezone = pytz.timezone('Asia/Seoul') # [2024.04.24] 수정
        
        for idx in df.index :
            idx_x = df.loc[idx].to_dict()
            down_bname = f"{ROOT_FOLDER_NAME}/{idx_x['CORP_CD']}/{idx_x['PROD_GROUP_CD']}/{idx_x['PROD_CD']}/{idx_x['EXT_NAME'].upper()}/{idx_x['ITEM_ID']}.{idx_x['EXT_NAME'].lower()}"
            down_blob_client = raw_cont_client.get_blob_client(down_bname)
            
            try:
                down_url = BASE_URL + idx_x['FILE_ID']
                data     = requests.get(down_url)
                down_blob_client.upload_blob(data, overwrite=True)
                    
                df.loc[idx, 'BLOB_PATH']      = os.path.split(down_bname)[0]
                df.loc[idx, 'BLOB_FILE_NAME'] = os.path.split(down_bname)[1]
                df.loc[idx, 'DOWNLOAD_YN']    = "Y"
                df.loc[idx, 'DOWNLOAD_DATE']  = datetime.now(korea_timezone) #[2024.04.25] 수정 down_blob_client.get_blob_properties()['last_modified']
                logger.info(f"Manual download complete : {down_url}")
            except Exception as e :
                df.loc[idx, 'BLOB_PATH']      = ""
                df.loc[idx, 'BLOB_FILE_NAME'] = ""
                df.loc[idx, 'DOWNLOAD_YN']    = "N"
                df.loc[idx, 'DOWNLOAD_DATE']  = datetime.now(korea_timezone)
            # [2024.04.24] 수정
            now = datetime.now(korea_timezone)
            df.loc[idx, 'CREATE_DATE'] = now
            df.loc[idx, 'CREATE_USER'] = 'SYS'
            df.loc[idx, 'UPDATE_DATE'] = now
            df.loc[idx, 'UPDATE_USER'] = 'SYS'
            
        # TB_IF_MANUAL_BLOB - UPSERT(insert + update)
        # key : ITEM_ID, CORP_CD, PROD_GROUP_CD, PROD_CD, FILE_ID
        insert_df = df[['ITEM_ID','CORP_CD','PROD_GROUP_CD','PROD_CD','FILE_ID'
                        ,'FILE_REAL_NAME','LANGUAGE_LIST','BLOB_PATH','BLOB_FILE_NAME','EXT_NAME','REGIST_DATE'
                        ,'DOWNLOAD_YN','DOWNLOAD_DATE'
                        ,'CREATE_DATE', 'CREATE_USER', 'UPDATE_DATE', 'UPDATE_USER']]
        
        upsert_query = f"""
            INSERT INTO tb_if_manual_blob
            ({", ".join(insert_df.columns)})
            VALUES ({", ".join(["%s"]*len(insert_df.columns))})
            ON DUPLICATE KEY UPDATE
                FILE_REAL_NAME = VALUES(FILE_REAL_NAME)
                , LANGUAGE_LIST  = VALUES(LANGUAGE_LIST)
                , BLOB_PATH      = VALUES(BLOB_PATH)
                , BLOB_FILE_NAME = VALUES(BLOB_FILE_NAME)
                , EXT_NAME       = VALUES(EXT_NAME)
                , REGIST_DATE    = VALUES(REGIST_DATE)
                , DOWNLOAD_YN    = VALUES(DOWNLOAD_YN)
                , DOWNLOAD_DATE  = VALUES(DOWNLOAD_DATE)
                , UPDATE_DATE    = VALUES(UPDATE_DATE)
                , UPDATE_USER    = VALUES(UPDATE_USER)
            ;
        """
        vals = []
        for idx in insert_df.index:
            vv = list(insert_df.loc[idx].to_dict().values())
            vals.append(vv) # [2024.04.24] 오타 수정
        sql_instance = MySql()
        sql_instance.commit_query_w_vals(query=upsert_query, vals=vals, many_tf=True)

    # [2024.04.24] 수정
    def set_process(self, logger, df):
        logger.info("###### Function Name : set_process")
        for idx in df.index :
            idx_x = df.loc[idx]
            if (idx_x['EXT_NAME'] == 'pdf') & (idx_x['LANG_LIST_MULTI_YN'] == 'N') :
                # PDF 파일 & 구성언어 : 단일어
                if (idx_x['LAYOUT_TYPE'] == "width_layout") & (idx_x['NUM_PAGES'] <= 10) :
                    # 10 페이지 이하 매뉴얼의 레이아웃이 전부 가로로 긴 경우
                    df.loc[idx, "METADATA_MULTI_YN"]   = "Y" # 검수 대상 = 'Y'
                    df.loc[idx, "METADATA_MULTI_DES"] = "layout"
                    df.loc[idx, 'SECTION_YN'] = ""
                elif (idx_x['NUM_PAGES'] <= 8) :
                    # pdf 매뉴얼이 8 페이지 이하인 경우 
                    df.loc[idx, "METADATA_MULTI_YN"]   = "Y" # 검수 대상 = 'Y'
                    df.loc[idx, "METADATA_MULTI_DES"] = "fewer pages" 
                    df.loc[idx, 'SECTION_YN'] = ""
                elif (idx_x['NP_75%'] < idx_x['NUM_PAGES']) :
                    # 기준 통계량보다 페이지 수가 많은 경우
                    df.loc[idx, "METADATA_MULTI_YN"]   = "Y" # 검수 대상 = 'Y'
                    df.loc[idx, "METADATA_MULTI_DES"] = "more pages than standard" 
                    df.loc[idx, 'SECTION_YN'] = ""
                else :
                    df.loc[idx, "METADATA_MULTI_YN"]   = "N" # 검수 대상 = 'N'
                    df.loc[idx, "METADATA_MULTI_DES"] = ""
                    df.loc[idx, 'SECTION_YN'] = "Y"
                
            elif (idx_x['EXT_NAME'] == 'pdf') & (idx_x['LANG_LIST_MULTI_YN'] == 'Y') :
                # PDF 파일 & 구성언어 : 다국어
                df.loc[idx, "METADATA_MULTI_YN"] = "Y" # 검수 대상 = 'Y'
                df.loc[idx, "METADATA_MULTI_DES"] = "multi-language"
                df.loc[idx, 'SECTION_YN'] = ""
            
            else : 
                # 그외 확장자 파일
                df.loc[idx, "METADATA_MULTI_YN"] = "Y" # 검수 대상 = 'Y'
                df.loc[idx, "METADATA_MULTI_DES"] = "not a pdf file"
                df.loc[idx, 'SECTION_YN'] = "N"  # [2024.04.24] 수정
            
            # [2024.04.24] 수정
            korea_timezone = pytz.timezone('Asia/Seoul')
            now = datetime.now(korea_timezone)
            
            df.loc[idx, 'SECTION_STATUS']        = ""
            df.loc[idx, 'SECTION_STATUS_DES']    = ""
            df.loc[idx, 'SPLIT_LANG_STATUS']     = ""
            df.loc[idx, 'SPLIT_LANG_STATUS_DES'] = ""
            df.loc[idx, 'CREATE_DATE'] = now
            df.loc[idx, 'CREATE_USER'] = 'SYS'
            df.loc[idx, 'UPDATE_DATE'] = now
            df.loc[idx, 'UPDATE_USER'] = 'SYS'
        # end for-loop
        
        insert_df = df[['ITEM_ID','CORP_CD','PROD_GROUP_CD','PROD_CD','FILE_ID','BLOB_PATH','BLOB_FILE_NAME'
                        ,'EXT_NAME','LANGUAGE_LIST','DOWNLOAD_YN','DOWNLOAD_DATE','REGIST_DATE'
                        ,'BYTE_SIZE','NUM_PAGES','LAYOUT_TYPE','LANG_LIST_MULTI_YN','METADATA_MULTI_YN','METADATA_MULTI_DES'
                        ,'SECTION_YN','SECTION_STATUS','SECTION_STATUS_DES','SPLIT_LANG_STATUS','SPLIT_LANG_STATUS_DES'
                        ,'CREATE_DATE', 'CREATE_USER', 'UPDATE_DATE', 'UPDATE_USER']]
        
        # 테이블 tb_chat_manual_prc : update & insert 수행
        # key : ITEM_ID, CORP_CD, PROD_GROUP_CD, PROD_CD, FILE_ID
        upsert_query = f"""
            INSERT INTO tb_chat_manual_prc
            ({", ".join(insert_df.columns)})
            VALUES ({", ".join(["%s"]*len(insert_df.columns))})
            ON DUPLICATE KEY UPDATE
                BLOB_PATH      = VALUES(BLOB_PATH)
                , BLOB_FILE_NAME = VALUES(BLOB_FILE_NAME)
                , EXT_NAME       = VALUES(EXT_NAME)
                , LANGUAGE_LIST  = VALUES(LANGUAGE_LIST)
                , DOWNLOAD_YN    = VALUES(DOWNLOAD_YN)
                , DOWNLOAD_DATE  = VALUES(DOWNLOAD_DATE)
                , REGIST_DATE    = VALUES(REGIST_DATE)
                , BYTE_SIZE      = VALUES(BYTE_SIZE)
                , NUM_PAGES      = VALUES(NUM_PAGES)
                , LAYOUT_TYPE    = VALUES(LAYOUT_TYPE)
                , LANG_LIST_MULTI_YN = VALUES(LANG_LIST_MULTI_YN)
                , METADATA_MULTI_YN  = VALUES(METADATA_MULTI_YN)
                , METADATA_MULTI_DES = VALUES(METADATA_MULTI_DES)
                , SECTION_YN         = VALUES(SECTION_YN)
                , SECTION_STATUS_DES = VALUES(SECTION_STATUS_DES)
                , SPLIT_LANG_STATUS  = VALUES(SPLIT_LANG_STATUS)
                , SPLIT_LANG_STATUS_DES = VALUES(SPLIT_LANG_STATUS_DES)
                , UPDATE_DATE = VALUES(UPDATE_DATE)
                , UPDATE_USER = VALUES(UPDATE_USER)
            ;
        """
        vals = []
        for idx in insert_df.index:
            vv = list(insert_df.loc[idx].to_dict().values())
            vals.append(vv)  # [2024.04.24] 오타 수정
        sql_instance = MySql()
        sql_instance.commit_query_w_vals(query=upsert_query, vals=vals, many_tf=True)

    # [2024.04.24] 수정
    # [2024.04.25] 수정 - as_instance_raw 함수 내부에서 선언
    def split_lang(self, logger, df, mst_blob_paths, temp_modify):
        logger.info("###### Function Name : split_lang")
        ROOT_FOLDER_NAME = 'testhjs/manual_split'
        
        as_instance_raw = AzureStorage(container_name='raw-data',storage_type='datast')
        korea_timezone  = pytz.timezone('Asia/Seoul') # [2024.04.24]
        
        # blob_client = as_instance_raw.container_client.get_blob_client(mst_blob_paths['cover_title']) # raw
        # cover_title = pd.read_csv(blob_client.download_blob())
        cover_title = as_instance_raw.read_file(mst_blob_paths['cover_title'])
        title_list  = [x.upper() for x in cover_title['title'].tolist()]
        
        success_idx_x = []
        
        for idx in tqdm(df.index) :
            idx_x = df.loc[idx].to_dict()
            output_path = f"{ROOT_FOLDER_NAME}/{idx_x['CORP_CD']}/{idx_x['PROD_GROUP_CD']}/{idx_x['PROD_CD']}/{idx_x['ITEM_ID']}"
            
            # -- [DELETE] 기존 결과 있으면 삭제
            # 1. table : tb_chat_manual_split_lang - key 기준 삭제
            delete_query = f"""
                DELETE FROM tb_chat_manual_split_lang
                WHERE 1=1
                    AND ITEM_ID = '{idx_x['ITEM_ID']}'
                    AND CORP_CD = '{idx_x['CORP_CD']}'
                    AND PROD_GROUP_CD = '{idx_x['PROD_GROUP_CD']}'
                    AND PROD_CD = '{idx_x['PROD_CD']}'
                ;
            """
            sql_instance = MySql()
            sql_instance.commit_query(delete_query)

            # 2. blobs - 기존 분할 결과 blob 백업 O
            as_instance_raw.delete_blobs(name_starts_with=f"{output_path}", backup='Y') # raw
            
            # -- sas url 및 PyMuPDF를 사용하여 텍스트 추출
            sas_url  = as_instance_raw.get_sas_url(blob_path=f"{idx_x['BLOB_PATH']}/{idx_x['BLOB_FILE_NAME']}")
            
            all_text = read_pdf_using_fitz(sas_url)
            all_text = [re.sub(" +", " ", ptext.replace("\n"," ")) for ptext in all_text]
            
            new_ptext = ""
            for pnum, ptext in enumerate(all_text):
                new_ptext = new_ptext + f"<page:{pnum+1}>\n{ptext.strip()}\n</page:{pnum+1}>\n"
            
            # -- 텍스트 추출 검사 : 토큰 수 확인 
            # 토큰 수가 0인 페이지가 50% 이상 차지하면 에러 처리.
            all_text_token = [num_tokens_from_string(txt) for txt in all_text]
            counter_token = Counter(all_text_token)

            if counter_token[0]/len(all_text_token) >= 0.5:
                self.save_pdfwriter(logger, sas_url, page_list = [], as_instance=as_instance_raw , blob_name = f"{output_path}/00_original.pdf")

                # [2024.04.24] UPDATE_DATE 추가
                update_query = f"""
                    UPDATE tb_chat_manual_prc
                    SET SPLIT_LANG_STATUS = 'Failed'
                        , SPLIT_LANG_STATUS_DES = 'Text could not be extracted from more than half of the pages of the PDF.'
                        , UPDATE_DATE = '{datetime.now(korea_timezone)}'
                    WHERE 1=1
                        AND ITEM_ID = '{idx_x['ITEM_ID']}'
                        AND CORP_CD = '{idx_x['CORP_CD']}'
                        AND PROD_GROUP_CD = '{idx_x['PROD_GROUP_CD']}'
                        AND PROD_CD = '{idx_x['PROD_CD']}'
                        AND FILE_ID = '{idx_x['FILE_ID']}'
                    ;
                """
                sql_instance = MySql()
                sql_instance.commit_query(update_query)
                continue
            
            # -- 각각 페이지에 언어별 커버 문구가 존재하는지 확인
            lang_pnum_dict = defaultdict(list) # {pnum:[language1, language2, ...]}
            for pnum, ptext in enumerate(all_text):
                ptext = ptext.upper()
                if any([x in ptext for x in title_list]):
                    for i in cover_title.index:
                        lang_info  = cover_title.loc[i].language
                        title_info = cover_title.loc[i].title
                        
                        if title_info.upper() in ptext:
                            # print(f"{title_info.upper()} is in page {pnum+1}")
                            lang_pnum_dict[pnum+1].append(lang_info)
            
            # -- 각 페이지에 매핑된 언어 취합
            # 한 페이지에 여러 언어 커버 문구가 존재한다면, 해당 페이지의 표지 정보 사용X
            fin_lang_pnum = defaultdict(list)
            for k, v in lang_pnum_dict.items():
                set_v = set(v)
                if len(set_v) == 1:
                    fin_lang_pnum[set_v.pop()].append(k)
                    
            # -- 커버 문구가 존재하지 않는 경우
            # 분할이 불가능하므로 에러 처리.
            if len(fin_lang_pnum) == 0:
                self.save_pdfwriter(logger, sas_url, page_list = [], as_instance=as_instance_raw , blob_name = f"{output_path}/00_original.pdf")

                # [2024.04.24] UPDATE_DATE 추가
                update_query = f"""
                    UPDATE tb_chat_manual_prc
                    SET SPLIT_LANG_STATUS = 'Failed'
                        , SPLIT_LANG_STATUS_DES = 'Mapped cover title does not exist in this PDF.'
                        , UPDATE_DATE = '{datetime.now(korea_timezone)}'
                    WHERE 1=1
                        AND ITEM_ID = '{idx_x['ITEM_ID']}'
                        AND CORP_CD = '{idx_x['CORP_CD']}'
                        AND PROD_GROUP_CD = '{idx_x['PROD_GROUP_CD']}'
                        AND PROD_CD = '{idx_x['PROD_CD']}'
                        AND FILE_ID = '{idx_x['FILE_ID']}'
                    ;
                """
                sql_instance = MySql()
                sql_instance.commit_query(update_query)
                continue
            
            # -- 각 언어 커버 문구가 가장 처음 나온 페이지 번호 사용하여 최종 결과 정리
            lp_list = [{"language": k, "st_pnum":min(v)} for k, v in fin_lang_pnum.items()]
            lp_df = pd.DataFrame(lp_list)
            lp_df = lp_df.sort_values(by=['st_pnum']).reset_index(drop=True)
            lp_df['ed_pnum'] = lp_df['st_pnum'].shift(-1, fill_value=len(all_text)+1)
            
            try :
                # 1. 원본 pdf 저장
                self.save_pdfwriter(logger, sas_url, page_list = [],  as_instance=as_instance_raw , blob_name = f"{output_path}/00_original.pdf")

                # 2. 언어별 pdf 저장
                for i in lp_df.index:
                    tmp_lang    = lp_df.loc[i].language
                    tmp_st_pnum = lp_df.loc[i].st_pnum-1 # include
                    tmp_ed_pnum = lp_df.loc[i].ed_pnum-1 # not include
                    
                    self.save_pdfwriter(logger, sas_url, page_list = list(range(tmp_st_pnum, tmp_ed_pnum)), as_instance=as_instance_raw ,save_blob_name = f"{output_path}/{tmp_lang}.pdf")
                
                # [2024.04.24] UPDATE_DATE 추가
                update_query = f"""
                    UPDATE tb_chat_manual_prc
                    SET SPLIT_LANG_STATUS = 'Success'
                        , SPLIT_LANG_STATUS_DES = 'Successfully perform split language process.'
                        , UPDATE_DATE = '{datetime.now(korea_timezone)}'
                    WHERE 1=1
                        AND ITEM_ID = '{idx_x['ITEM_ID']}'
                        AND CORP_CD = '{idx_x['CORP_CD']}'
                        AND PROD_GROUP_CD = '{idx_x['PROD_GROUP_CD']}'
                        AND PROD_CD = '{idx_x['PROD_CD']}'
                        AND FILE_ID = '{idx_x['FILE_ID']}'
                    ;
                """
                sql_instance = MySql()
                sql_instance.commit_query(update_query)
                
                idx_x['SPLIT_LANG_STATUS']      = "S"
                idx_x['SPLIT_LANG_STATUS_DES'] = "success"
                idx_x['languages']   = [(lang, f"{output_path}", f"{lang}.pdf") for lang in lp_df.language.tolist()]
                
                success_idx_x.append(idx_x)
                
            except Exception as e :
                self.save_pdfwriter(logger, sas_url, page_list = [], as_instance=as_instance_raw, blob_name = f"{output_path}/00_original.pdf")
                # [2024.04.24] UPDATE_DATE 추가
                update_query = f"""
                    UPDATE tb_chat_manual_prc
                    SET SPLIT_LANG_STATUS = 'Failed'
                        , SPLIT_LANG_STATUS_DES = 'Error occurred while splitting PDF using PyPDF2.'
                        , UPDATE_DATE = '{datetime.now(korea_timezone)}'
                    WHERE 1=1
                        AND ITEM_ID = '{idx_x['ITEM_ID']}'
                        AND CORP_CD = '{idx_x['CORP_CD']}'
                        AND PROD_GROUP_CD = '{idx_x['PROD_GROUP_CD']}'
                        AND PROD_CD = '{idx_x['PROD_CD']}'
                        AND FILE_ID = '{idx_x['FILE_ID']}'
                    ;
                """
                sql_instance = MySql()
                sql_instance.commit_query(update_query)
                continue
        # end for-loop
        
        success_res = pd.DataFrame(success_idx_x).explode('languages', ignore_index=True)
        success_res[['LANGUAGE_NAME', 'LANG_BLOB_PATH', 'LANG_BLOB_FILE_NAME']] = pd.DataFrame(success_res['languages'].tolist(), index=success_res.index)
        
        insert_df = success_res[['ITEM_ID', 'CORP_CD', 'PROD_GROUP_CD', 'PROD_CD'
                                ,'LANGUAGE_NAME', 'LANG_BLOB_PATH', 'LANG_BLOB_FILE_NAME']]\
                            .rename(columns={"LANG_BLOB_PATH" : "BLOB_PATH", "LANG_BLOB_FILE_NAME" : "BLOB_FILE_NAME"})\
                            .reset_index(drop=True)
        
        # -- 대상언어 여부 컬럼 추가
        sql_instance = MySql()
        corp_lang_mapp = sql_instance.get_tar_language(temp_modify) # [2024.04.24] 수정
        insert_df = insert_df.merge(corp_lang_mapp, how='left', on=['CORP_CD'])
        insert_df['TARGET_LANGUAGE_YN'] = ["Y" if any(lang in l for lang in tns) else "N" for l, tns in zip(insert_df['LANGUAGE_NAME'], insert_df['TARGET_LANGUAGE_NAMES'])]
        
        # [2024.04.24] 수정
        now = datetime.now(korea_timezone)
        insert_df['CREATE_DATE'] = now
        insert_df['CREATE_USER'] = 'SYS'
        insert_df['UPDATE_DATE'] = now
        insert_df['UPDATE_USER'] = 'SYS'
        
        # tb_chat_manual_split_lang - INSERT
        # key : ITEM_ID, CORP_CD, PROD_GROUP_CD, PROD_CD, FILE_ID
        inser_query = f"""
            INSERT INTO tb_chat_manual_split_lang
            ({", ".join(insert_df.columns)})
            VALUES ({", ".join(["%s"]*len(insert_df.columns))})
            ;
        """
        vals = []
        for idx in insert_df.index:
            vv = list(insert_df.loc[idx].to_dict().values())
            vals.append(vv)  # [2024.04.24] 오타 수정
        sql_instance = MySql()
        sql_instance.commit_query_w_vals(query=inser_query, vals=vals, many_tf=True)
    
    # [2024.04.24] unzip 함수 추가
    # [2024.04.25] as_instance_raw 함수 내부에서 선언
    def unzip(self, logger, df):
        logger.info("###### Function Name : unzip")
        as_instance_raw = AzureStorage(container_name='raw-data',storage_type='datast')
        raw_cont_client = as_instance_raw.container_client # raw
        ROOT_FOLDER_NAME = 'testhjs/manual_unzip'
        use_cols = ['ITEM_ID','CORP_CD','PROD_GROUP_CD','PROD_CD','FILE_ID', 'BLOB_PATH', 'BLOB_FILE_NAME']

        inner_file_info = []
        for idx in tqdm(df.index):
            idx_x = df.loc[idx].to_dict()
            bname = f"{idx_x['BLOB_PATH']}/{idx_x['BLOB_FILE_NAME']}"
            
            output_path = f"{ROOT_FOLDER_NAME}/"+ "/".join(os.path.splitext(bname)[0].split("/")[1:])
            
            # -- 기존 압축해제 결과 존재하는 경우 삭제, 백업 O
            as_instance_raw.delete_blobs(name_starts_with=f"{output_path}", backup='Y') # raw
            
            # -- 압축풀기 및 저장
            sas_url = as_instance_raw.get_sas_url(blob_path=bname)
            myzip = ZipFile(io.BytesIO(urlopen(sas_url).read()))
            for inner_file in myzip.filelist:
                if inner_file.is_dir():
                    continue
                
                inn_dict = {k : idx_x[k] for k in use_cols}
                inn_dict['INNER_FILE_NAME'] = inner_file.filename
                inn_dict['INNER_FILE_EXT']  = os.path.splitext(inner_file.filename)[-1].replace(".", "").lower()
                try :
                    uncompressed = myzip.read(inner_file.filename)
                    blob_client = raw_cont_client.get_blob_client(f'{output_path}/{inner_file.filename}')
                    blob_client.upload_blob(uncompressed, overwrite=True)
                    
                    inn_dict['UNCOMP_YN'] = "Y"
                    inn_dict['INNER_FILE_BLOB_PATH'] = output_path
                except Exception as e :
                    inn_dict['UNCOMP_YN'] = 'N'
                    inn_dict['INNER_FILE_BLOB_PATH'] = ""
                
                inner_file_info.append(inn_dict)
            # end for-loop : myzip.filelist
        # end for-loop : df.index
        
        # -- csv 파일 저장(개발 리소스)
        inner_file_df = pd.DataFrame(inner_file_info)
        inner_file_df['ORG_BLOB_NAME'] = inner_file_df['BLOB_PATH'] + "/" + inner_file_df['BLOB_FILE_NAME']
        inner_file_df = inner_file_df[['ITEM_ID', 'CORP_CD', 'PROD_GROUP_CD', 'PROD_CD', 'FILE_ID', 'ORG_BLOB_NAME'
                                    , 'INNER_FILE_NAME', 'INNER_FILE_BLOB_PATH', 'INNER_FILE_EXT', 'UNCOMP_YN']]
        
        korea_timezone = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_timezone)
        formatted_date = now.strftime('%Y%m%d')
        
        # 1. 전체 파일 목록 - blob 저장
        csv1_path = f"{ROOT_FOLDER_NAME}/TMP_CSV/{formatted_date}/compressed_file_list.csv"
        csv1_data = inner_file_df.to_csv(index=False)
        raw_cont_client.get_blob_client(csv1_path).upload_blob(csv1_data, overwrite=True)
        
        # 2. html 파일 목록 - blob 저장
        csv2_path = f"{ROOT_FOLDER_NAME}/TMP_CSV/{formatted_date}/html_file_list.csv"
        csv2_data = inner_file_df[(inner_file_df['EXT_NAME']=='html') & (inner_file_df['UNCOMP_YN'] == 'N')].to_csv(index=False)
        raw_cont_client.get_blob_client(csv2_path).upload_blob(csv2_data, overwrite=True)
        
        # 3. pdf 파일 목록 - blob 저장
        pdf_in_zip = inner_file_df[(inner_file_df['EXT_NAME']=='pdf') & (inner_file_df['UNCOMP_YN'] == 'N')].reset_index(drop=True)
        pdf_in_zip[['USE_YN', 'SPLIT_YN', 'FIN_LANGUAGE', 'PAGE_RANGE']] = ["", "", "", ""]
        csv3_path = f"{ROOT_FOLDER_NAME}/TMP_CSV/{formatted_date}/pdf_file_list.csv"
        csv3_data = pdf_in_zip.to_csv(index=False)
        raw_cont_client.get_blob_client(csv3_path).upload_blob(csv3_data, overwrite=True)
    
    # [2024.04.25] 추가
    def inspect_pdf(self, logger, df):
        logger.info("###### Function Name : inspect_pdf")
        ROOT_FOLDER_NAME = "testhjs/manual_valid"
        
        as_instance_raw = AzureStorage(container_name='raw-data', storage_type= 'datast')
        raw_cont_client = as_instance_raw.container_client
        
        key_df = df[['ITEM_ID', 'CORP_CD', 'PROD_GROUP_CD', 'PROD_CD', 'FIN_LANGUAGE']]\
                    .drop_duplicates().reset_index(drop=True)
        
        for idx in tqdm(key_df.index) :
            idx_dict = key_df.loc[idx].to_dict()
            idx_df = pd.DataFrame([idx_dict])
            
            out_bname = f"{ROOT_FOLDER_NAME}/{idx_dict['CORP_CD']}/{idx_dict['PROD_GROUP_CD']}/{idx_dict['PROD_CD']}/{idx_dict['ITEM_ID']}/{idx_dict['FIN_LANGUAGE']}.pdf"
            # -- 기존 적재 결과 삭제
            as_instance_raw.delete_blobs(name_starts_with=f"{os.path.split(out_bname)[0]}", backup='Y') # raw
            
            # -- 동일한 FIN_LANGUAGE가 2개 이상인 경우, pdf 병합 수행
            mer_df = idx_df.merge(df, how='left', on = ['ITEM_ID', 'CORP_CD', 'PROD_GROUP_CD', 'PROD_CD', 'FIN_LANGUAGE'])
            writer = self.merge_pdfs(logger, mer_df)
            
            if writer == None:
                # 에러발생 
                # -- logging?
                key_df.loc[idx, "MERGE_STATUS"] = "Failed"
                key_df.loc[idx, "BLOB_PATH"]    = ""
                key_df.loc[idx, "BLOB_FILE_NAME"] = ""
                continue
            
            with io.BytesIO() as bytes_stream :
                writer.write(bytes_stream)
                bytes_stream.seek(0)
                as_instance_raw.container_client
                blob_client = raw_cont_client.get_blob_client(out_bname)
                blob_client.upload_blob(bytes_stream, overwrite = True)
            
            key_df.loc[idx, "MERGE_STATUS"] = "Success"
            key_df.loc[idx, "BLOB_PATH"]    = os.path.split(out_bname)[0]
            key_df.loc[idx, "BLOB_FILE_NAME"] = os.path.split(out_bname)[1]
        # end for-loop : key_df.index
        return key_df
    

    # [2024.04.25] 추가
    def run_split_section(self, logger, x_info:dict, aoai_res:str, section_groups:pd.DataFrame):
        logger.info("###### Function Name : run_split_section")
        ROOT_FOLDER_NAME = "testhjs/manual_gpt"
        PAGE_BATCH_SIZE  = 40
        as_instance_raw = AzureStorage(container_name='raw-data', storage_type='datast')
        as_instance_pre = AzureStorage(container_name='preprocessed-data', storage_type='datast')
        
        # -- TEXT 추출
        load_bname = f"{x_info['BLOB_PATH']}/{x_info['BLOB_FILE_NAME']}"
        sas_url  = as_instance_raw.get_sas_url(load_bname)
        all_text = read_pdf_using_fitz(sas_url)
        num_pages = len(all_text)
        full_text = ""
        for pnum, ptext in enumerate(all_text):
            full_text = full_text + f"<page:{pnum+1}>\n{ptext.strip()}\n</page:{pnum+1}>\n"    
        
        # -- 텍스트 추출 검사 : 토큰 수 확인 
        all_text_token = [num_tokens_from_string(txt) for txt in all_text]
        counter_token = Counter(all_text_token)
        # -- 토큰 수가 0인 페이지가 50% 이상 차지하면 에러 처리.
        if counter_token[0]/len(all_text_token) >= 0.5:
            STATUS = 'Failed'
            STATUS_DES = f'Text could not be extracted from more than half of the pages of the PDF.\nblob name : {load_bname}'
            return {}, STATUS, STATUS_DES
        
        # -- 프롬프트 생성
        pgcd = x_info['PROD_GROUP_CD']
        pcd  = x_info['PROD_CD']
        system_message, groups = get_system_mssg(pgcd, pcd, section_groups)
        
        # -- 페이지 배치기준 텍스트 분할 및 GPT 수행
        data_list = split_text_by_batch(full_text, num_pages, PAGE_BATCH_SIZE)
        aoai_model = OpenAI_Section(resource_type=aoai_res, gpt_model='gpt-4-turbo-0125', max_tokens=4096, max_retries=2, sleep_time=2)
        result_total = {}
        ERROR_TF   = False
        STATUS     = ""
        STATUS_DES = ""
        for data in data_list :
            sliced_data = data['data']
            # -- gpt 호출
            try:
                response, completion_message = aoai_model.get_response(system_message=system_message
                                        , user_message=sliced_data
                                        )
            except Exception as e:
                ERROR_TF = True
                STATUS   = "Failed"
                STATUS_DES = f"OPENAI ERROR.\nblob name : {load_bname}\n{repr(e)}"
                break
            # -- 결과 정리
            try : 
                completion_dict = parse_text_to_dict(completion_message)
                result_total    = merge_dicts(result_total, completion_dict)
            except Exception as e:
                ERROR_TF = True
                STATUS   = "Failed"
                STATUS_DES = f"Error occurred while parsing results.\nblob name : {load_bname}\n{repr(e)}"
                break
        # end for-loop : data_list
        
        if ERROR_TF:
            # -- gpt 수행 중 에러 발생한 경우 종료
            return {}, STATUS, STATUS_DES
        
        # -- 후처리
        # 1. 후처리 - num_pages보다 큰 페이지넘버가 존재하는 경우
        #       해당 정보 삭제 + log 남기기(error 발생시키지는 않음)
        for k, v in result_total.items():
            if any(vv > num_pages for vv in v):
                re_list = list(filter(lambda i: i <= num_pages, v))
                result_total[k] = re_list
        
        # 2. 후처리  - 정의하지 않은 group명 존재하는 경우  Miscellaneous 에 추가 후 해당 그룹정보 제거
        no_gr_name  = []
        gr_num_list = []
        for k, v in result_total.copy().items():
            gr_num_list.extend(v)
            if k not in groups:
                if 'Miscellaneous' in result_total:
                    result_total['Miscellaneous'].extend(result_total[k])
                else :
                    result_total['Miscellaneous'] = result_total[k]
                no_gr_name.append(k)
        for del_k in no_gr_name:
            del result_total[del_k]
        
        # 3. 후처리 - 페이지 정보 취합후 없는 페이지 Miscellaneous 에 추가    
        gr_num_list = set(gr_num_list)
        for tp in range(num_pages):
            if tp+1 not in gr_num_list:
                if 'Miscellaneous' in result_total:
                    result_total['Miscellaneous'].append(tp+1)
                else :
                    result_total['Miscellaneous'] = [tp+1]
        
        # -- 섹션 분할 결과 저장
        # 1. blob 저장
        base_path = f"{ROOT_FOLDER_NAME}/{x_info['CORP_CD']}/{x_info['LANGUAGE_NAME']}/{x_info['PROD_GROUP_CD']}/{x_info['PROD_CD']}/{x_info['ITEM_ID']}/section"
        as_instance_pre.save_section(result_total, all_text, base_path)
        
        # 2. tb_chat_manual_sect 테이블 insert
        group_list = []
        for gname, page_list in result_total.items():
            tmp = {k : x_info[k] for k in ['ITEM_ID', 'CORP_CD', 'PROD_GROUP_CD', 'PROD_CD', 'LANGUAGE_NAME']}
            tmp['SOURCE_BLOB_PATH'] = x_info['BLOB_PATH']
            tmp['SOURCE_BLOB_FILE_NAME'] = x_info['BLOB_FILE_NAME']
            tmp['GROUP_NAME']  = gname
            tmp['GROUP_PAGES'] = ",".join([str(x) for x in page_list])
            tmp['BLOB_PATH']   = base_path
            tmp['BLOB_FILE_NAME'] = f"{gname}.txt"
            group_list.append(tmp)
        
        korea_timezone = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_timezone)
        
        insert_df = pd.DataFrame(group_list)
        insert_df['CREATE_DATE'] = now
        insert_df['CREATE_USER'] = 'SYS'
        insert_df['UPDATE_DATE'] = now
        insert_df['UPDATE_USER'] = 'SYS'
        
        insert_query = f"""
            INSERT INTO tb_chat_manual_sect
            ({", ".join(insert_df.columns)})
            VALUES ({", ".join(["%s"]*len(insert_df.columns))})
        """
        vals = []
        for idx in insert_df.index:
            vv = list(insert_df.loc[idx].to_dict().values())
            vals.append(vv)
        sql_instance = MySql()
        sql_instance.commit_query_w_vals(query=insert_query, vals=vals, many_tf=True)
        
        # -- 성공 결과 return
        STATUS = "Success"
        STATUS_DES = "Successfully perform split section process."
        return result_total, STATUS, STATUS_DES

    
    # [2024.04.25] 추가
    def prc_html_and_section(self, logger, df, tar_folder_mapp:dict, temp_modify:bool) :
        logger.info("###### Function Name : prc_html_and_section")
        HTML_ROOT_FOLDER_NAME = "testhjs/manual_valid"
        SEC_ROOT_FOLDER_NAME  = "testhjs/manual_gpt"
        
        as_instance_raw = AzureStorage(container_name='raw-data', storage_type='datast')
        as_instance_pre = AzureStorage(container_name='preprocessed-data', storage_type='datast')
        
        sql_instance = MySql()
        subs_info = sql_instance.get_subs_info(temp_modify)
        pattern = re.compile(r'ch\d+_s\d+_')
        
        key_df = df[['ITEM_ID', 'CORP_CD', 'PROD_GROUP_CD', 'PROD_CD']].drop_duplicates().reset_index(drop=True)
        for idx in key_df.index:
            key_idx_dict = key_df.loc[idx].to_dict()
            corp_cd = key_idx_dict['CORP_CD']
            pgcd    = key_idx_dict['PROD_GROUP_CD']
            pcd     = key_idx_dict['PROD_CD']
            item_id = key_idx_dict['ITEM_ID']
            
            # -- 기존 적재 결과 제거 (section, html 취합본)
            # 1. DB 내용 삭제
            delete_query = f"""
                DELETE FROM tb_chat_manual_sect
                WHERE 1=1
                    AND ITEM_ID = '{item_id}'
                    AND CORP_CD = '{corp_cd}'
                    AND PROD_GROUP_CD = '{pgcd}'
                    AND PROD_CD = '{pcd}'
                ;
            """
            sql_instance = MySql()
            sql_instance.commit_query(delete_query)
            # 2. blob 삭제 : 섹션 분할 결과, html 병합 결과
            del_sec_path = f"{SEC_ROOT_FOLDER_NAME}/{corp_cd}/{l_name}/{pgcd}/{pcd}/{item_id}/section"
            as_instance_pre.delete_blobs(name_starts_with=del_sec_path, backup='Y')
            del_html_path = f"{HTML_ROOT_FOLDER_NAME}/{corp_cd}/{pgcd}/{pcd}/{item_id}/" # manual_valid 에 저장 후 Search 파일 생성시 이관
            as_instance_raw.delete_blobs(name_starts_with=del_html_path, backup='Y')
            
            # --
            key_idx_df = pd.DataFrame([key_idx_dict])
            key_html_df = key_idx_df.merge(df, how='left', on = ['ITEM_ID', 'CORP_CD', 'PROD_GROUP_CD', 'PROD_CD'])
            html_fld_mapp = tar_folder_mapp[corp_cd]
            
            for fld in html_fld_mapp:
                l_name = subs_info[subs_info['LANGUAGE_CD'] == fld.split('-')[0].lower()].iloc[0]['LANGUAGE_NAME']
                # -- 언어-국가 정보(예 "de-DE")를 포함하는 html 추출
                cur_lang_html_df = key_html_df[(key_html_df['CORP_CD'] == corp_cd) 
                                            & (key_html_df['PROD_GROUP_CD'] == pgcd)
                                            & (key_html_df['PROD_CD'] == pcd)
                                            & (key_html_df['ITEM_ID'] == item_id) 
                                            & (key_html_df['INNER_FILE_NAME'].str.contains(fld)
                                        )].reset_index(drop=True)

                if len(cur_lang_html_df) == 0:
                    continue
                    
                cur_html_bpath = cur_lang_html_df['INNER_FILE_BLOB_PATH'].unique()[0]
                
                # 취합할 html 파일명 - ch\d+_s\d+_ 포함하는 html. (패턴과 매핑되는 html이 없는 경우 pass)
                tar_html_names = [x for x in cur_lang_html_df['INNER_FILE_NAME'] if pattern.findall(x)]
                if len(tar_html_names) == 0:
                    continue
                
                # -- HTML 취합 및 text 추출
                html_group_info = make_html_group(tar_html_names)
                txt_save_path   = f"{SEC_ROOT_FOLDER_NAME}/{corp_cd}/{l_name}/{pgcd}/{pcd}/{item_id}/section"
                html_save_bname = f"{HTML_ROOT_FOLDER_NAME}/{corp_cd}/{pgcd}/{pcd}/{item_id}/{l_name}.html"
                
                output_doc = {}
                section_group_tb = []
                for ch_txt, v in html_group_info.items():
                    div_id = int(ch_txt.replace("ch",""))
                    ch_gname, ch_html_list = v['group_name'], v['html_list']
                    
                    # -- 각 챕터(= 그룹)별 텍스트 저장
                    dch_doc, ch_gtext = self.merge_html_and_text(logger, cur_html_bpath, ch_html_list, filter={'fld_name':fld, 'div_id':div_id})
                    output_doc[div_id] = dch_doc
                    blob_client = as_instance_pre.container_client.get_blob_client(f"{txt_save_path}/{ch_gname}.txt")
                    blob_client.upload_blob(ch_gtext, overwrite = True)
                    
                    section_group_tb.append({
                        'ITEM_ID': item_id
                        , 'CORP_CD': corp_cd
                        , 'PROD_GROUP_CD': pgcd
                        , 'PROD_CD': pcd
                        , 'LANGUAGE_NAME': l_name
                        , 'SOURCE_BLOB_PATH': os.path.split(html_save_bname)[0]
                        , 'SOURCE_BLOB_FILE_NAME': os.path.split(html_save_bname)[1]
                        , 'GROUP_NAME': ch_gname
                        , 'GROUP_PAGES': f"{div_id}"
                        , 'BLOB_PATH': txt_save_path
                        , 'BLOB_FILE_NAME': f"{ch_gname}.txt"
                    })
                # end for-loop : html_group_info(그룹별 텍스트 저장
                 
                # -- 취합한 html 저장 
                self.save_merged_htmls(logger, output_doc, save_bname=html_save_bname)
                
                # -- tb_chat_manual_sect 정보 추가
                korea_timezone = pytz.timezone('Asia/Seoul')
                now = datetime.now(korea_timezone)
                insert_df = pd.DataFrame(section_group_tb)
                insert_df['CREATE_DATE'] = now
                insert_df['CREATE_USER'] = 'SYS'
                insert_df['UPDATE_DATE'] = now
                insert_df['UPDATE_USER'] = 'SYS'
                
                insert_query = f"""
                    INSERT INTO tb_chat_manual_sect
                    ({", ".join(insert_df.columns)})
                    VALUES ({", ".join(["%s"]*len(insert_df.columns))})
                """
                vals = []
                for idx in insert_df.index:
                    vv = list(insert_df.loc[idx].to_dict().values())
                    vals.append(vv)
                sql_instance = MySql()
                sql_instance.commit_query_w_vals(query=insert_query, vals=vals, many_tf=True)
            # end for-loop : html_fld_mapp(언어별 html 병합 저장)
        # end for-loop : key_df
    
    # [2024.04.25] 추가
    def structured_and_html(self, logger, df, temp_modify):
        logger.info("###### Function Name : structured_and_html")
        as_instance_raw = AzureStorage(container_name='raw-data', storage_type='datast')
        as_instance_pre = AzureStorage(container_name='preprocessed-data', storage_type='datast')
        as_instance_doc = AzureStorage(container_name='documents', storage_type='docst')
        as_instance_web = AzureStorage(container_name='$web', storage_type='docst')
        
        tokenizer = tiktoken.get_encoding("cl100k_base")
        sql_instance = MySql()
        subs_info = sql_instance.get_subs_info(temp_modify) # 법인, 국가, 언어 매핑 정보
        
        # -- key 정보 추출
        key_df = df[['ITEM_ID', 'CORP_CD', 'PROD_GROUP_CD', 'PROD_CD', 'LANGUAGE_NAME', 'SOURCE_BLOB_PATH', 'SOURCE_BLOB_FILE_NAME']]\
                    .drop_duplicates().reset_index(drop=True)
                    
        # -- Search용 파일 생성 - structured & html
        for idx in key_df.index:
            key_idx_dict = key_df.loc[idx].to_dict()
            corp_cd = key_idx_dict['CORP_CD']
            pgcd    = key_idx_dict['PROD_GROUP_CD']
            pcd     = key_idx_dict['PROD_CD']
            item_id = key_idx_dict['ITEM_ID']
            
            # -- ISO_CD, LANGUAGE_CD 매핑
            country_lang_map = subs_info[(subs_info['CORP_CD'] == corp_cd) 
                                     & (subs_info['LANGUAGE_NAME'] == key_idx_dict['LANGUAGE_NAME'])]
            
            # -- PRD_MODEL_CD 매핑
            query = f"""
                SELECT * FROM tb_if_manual_list
                WHERE 1=1
                    AND CORP_CD = '{corp_cd}'
                    AND ITEM_ID = '{item_id}'
                    AND PROD_CD = '{pcd}'
                    AND DELETE_FLAG = 'N'
                ;
            """
            sql_instance = MySql()
            pr_model_cd_map  = sql_instance.load_table_as_df(query)
            pr_model_cd_list = list(pr_model_cd_map['PROD_MODEL_CD'].unique())
            if len(pr_model_cd_list) == 0:
                # -- logging?
                continue
            pr_model_cd_str = ",".join(pr_model_cd_list)
            
            # -- 1. structured 파일 생성 - 적재 위치 : documents 컨테이너
            grp_df = pd.DataFrame([key_idx_dict]).merge(df, how='left', on = ['ITEM_ID', 'CORP_CD', 'PROD_GROUP_CD', 'PROD_CD', 'LANGUAGE_NAME', 'SOURCE_BLOB_PATH', 'SOURCE_BLOB_FILE_NAME'])
            for grp_idx in grp_df.index:
                grp_idx_x = grp_df.loc[grp_idx].to_dict()
                
                title_n   = grp_idx_x['GROUP_NAME'].replace(" ", "_")
                
                # section 분할 결과(txt 파일) - sas url 사용
                txt_sas_url = as_instance_pre.get_sas_url(blob_path=f"{grp_idx_x['BLOB_PATH']}/{grp_idx_x['BLOB_FILE_NAME']}")
                with urlopen(txt_sas_url) as url:
                    section_content = url.read().decode()
                page_list = extract_page_patterns(section_content)
                page_num_str = ",".join([x.replace("<page:",">").replace(">","") for x in page_list])
                
                new_content = re.sub('<page:\d+>', '', section_content).strip()
                new_content = re.sub('</page:\d+>', '', new_content).strip()
                if new_content.strip() == "":
                    # page 정보를 제외하고는 텍스트가 없다면, Search용 문서 생성하지 않음
                    # -- logging?
                    continue
                
                structured_info = {'group_name':title_n
                               , 'pr_model_cd_str':pr_model_cd_str
                               , 'pages':page_num_str}
                
                structured_list = to_structured_1(new_content, tokenizer, structured_info)
                for i, chunk in enumerate(structured_list):
                    data = json.dumps(obj = chunk, ensure_ascii=False, indent="\t")
                    for cl_idx in country_lang_map.index:
                        save_bname = f"data/{country_lang_map.loc[cl_idx,'ISO_CD']}/{country_lang_map.loc[cl_idx,'LANGUAGE_NAME']}/manual/{pgcd}/{pcd}/{item_id}/structured_1/{title_n}_{i}.json"
                        blob_client = as_instance_doc.container_client.get_blob_client(save_bname)
                        blob_client.upload_blob(data, overwrite=True)
                # end for-loop : structured_list
            # end for-loop : grp_df (섹션 그룹별 structured 파일 저장)
            
            # -- 2. html 파일 생성 - $web 컨테이너
            if ".html" in key_idx_dict['SOURCE_BLOB_FILE_NAME'] :
                # 병합된 html 파일인 경우, $web 컨테이너로 이관
                for cl_idx in country_lang_map.index:
                    src_sas_url = as_instance_raw.get_sas_url(f"{key_idx_dict['SOURCE_BLOB_PATH']}/{key_idx_dict['SOURCE_BLOB_FILE_NAME']}")
                    save_html_bname = f"data/{country_lang_map.loc[cl_idx,'ISO_CD']}/{country_lang_map.loc[cl_idx,'LANGUAGE_NAME']}/{pgcd}/{pcd}/{item_id}/html/{item_id}.html"
                    blob_client = as_instance_web.container_client.get_blob_client(save_html_bname)
                    blob_client.upload_blob_from_url(source_url=src_sas_url, overwrite=True)
            elif ".pdf" in key_idx_dict['SOURCE_BLOB_FILE_NAME']:
                # pdf 파일인 경우, 이미지 적재 후 html 생성 및 적재
                # -- pdf 각 페이지 svg로 저장
                src_sas_url = as_instance_raw.get_sas_url(f"{key_idx_dict['SOURCE_BLOB_PATH']}/{key_idx_dict['SOURCE_BLOB_FILE_NAME']}")
                request_blob = requests.get(src_sas_url)
                filestream = io.BytesIO(request_blob.content)
                doc = fitz.open(stream=filestream, filetype="pdf")
                for i, page in enumerate(doc):
                    svg = page.get_svg_image(matrix=fitz.Identity)
                    svg_bytes  = svg.encode('utf-8')
                    img_data = io.BytesIO(svg_bytes)
                    for cl_idx in country_lang_map.index:
                        img_data.seek(0)
                        svg_bname   = f"data/{country_lang_map.loc[cl_idx,'ISO_CD']}/{country_lang_map.loc[cl_idx,'LANGUAGE_NAME']}/{pgcd}/{pcd}/{item_id}/images/{i+1}.svg"
                        blob_client = as_instance_web.container_client.get_blob_client(svg_bname)
                        blob_client.upload_blob(img_data, content_type="image/svg+xml", overwrite=True)
                # end for-loop : doc(svg 저장)
                doc.close()
                
                # -- 적재된 svg 사용해서 html 생성
                for cl_idx in country_lang_map.index:
                    svg_blob_path  = f"data/{country_lang_map.loc[cl_idx,'ISO_CD']}/{country_lang_map.loc[cl_idx,'LANGUAGE_NAME']}/{pgcd}/{pcd}/{item_id}/images/"
                    svg_bname_list = as_instance_web.container_client.list_blobs(name_starts_with = svg_blob_path)
                    svg_bname_list = [i.name for i in svg_bname_list]
                    if len(svg_bname_list) == 0 :
                        continue
                    else :
                        # -- 저장된 페이지 이미지 sas url 
                        svg_url_list = []
                        for svg_bname in svg_bname_list :
                            svg_url_list.append({"page" : svg_bname.split('/')[-1].split('.')[0],
                                                    "img_url" : as_instance_web.get_sas_url(svg_bname, expiry_time=False)
                                })
                        svg_url_list = sorted(svg_url_list, key=lambda x: int(x['page']))
                        # -- html 생성
                        html_content = "<html><body style='background-color: white;'>"
                        for url in svg_url_list:
                            html_content += f'<div id="page{url["page"]}" style="text-align: center;"><img src="{url["img_url"]}" style="display: inline-block; margin: 10px auto;"></div>\n'
                        html_content += "</body></html>"        
                        # -- html 저장
                        html_bname  = f"data/{country_lang_map.loc[cl_idx,'ISO_CD']}/{country_lang_map.loc[cl_idx,'LANGUAGE_NAME']}/{pgcd}/{pcd}/{item_id}/html/{item_id}.html"
                        blob_client = as_instance_web.container_client.get_blob_client(html_bname)
                        blob_client.upload_blob(html_content, content_type=("text/html"), overwrite=True)
                # end for-loop : country_lang_map (국가-언어별 html 저장)
            # end if-else 
        # end for-loop : key_df
        
        indexer_df   = key_df.merge(subs_info, how='inner', on=['CORP_CD', 'LANGUEAGE_NAME'])
        indexer_info = indexer_df[['LOCALE_CD', 'LANGUAGE_CD']].drop_duplicates()
        indexer_list = (indexer_info['LOCALE_CD'].str.split('_').str[0] + '-' + indexer_info['LANGUAGE_CD']).tolist()
        return indexer_list