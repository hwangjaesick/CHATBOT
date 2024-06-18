import uuid
import json
import pytz
from datetime import datetime, timezone
import logging
from database import AzureStorage, MySql, CosmosDB
import re
import tiktoken
import requests
import os
import fitz
import io
from log import setup_logger
import pandas as pd
import time
import math
import pytz
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Get prompt ( model/generate_answer ) -- PRD
def get_prompt(prompt_path):
    logger.info("###### Function Name : get_prompt")
    with open(prompt_path, "r", encoding="utf-8") as f:
        prompt = f.read()
    
    return  prompt

# Preprocessing of received parameters ( chat/__init__) -- PRD
def preprocess_param(data) :
    logger.info("###### Function Name : preprocess_query")
    # 웹 데모 테스트용
 
    global_response_iso_cd = data['param']['countryCode']
    global_response_product_code = data['param']['productCode']
    global_response_product_model_code = data['param']['productModelCode']
    global_response_product_group_code = data['param']['productGroupCode']
    global_response_language_code = data['param']['language']
    global_response_chat_session_id = data['param']['chatSessionId']
    global_response_locale_code = data['param']['localeCode']
    chatid = data['param']['chatId']
    ragorder = data['param']['ragOrder']
    message_content = data['message'][-1]['content']
    message_content = str(message_content)
    platform = data['param']['platform']
    
    print("chatid :",chatid)

    # 실제 배포용
    # global_response_iso_cd = data['param']['CountryCode']
    # global_response_product_code = data['param']['ProductCode']
    # global_response_product_model_code = data['param']['ProductModelCode']
    # global_response_product_group_code = data['param']['ProductGroupCode']
    # chatid = data['param']['ChatId']
    # message_content = data['Message']['Content']
    # message_content = str(message_content)
    # print("chatid :",chatid)

    query = {'chatid' : chatid,
             'platform' : platform,
             'iso_cd' : global_response_iso_cd,
             'language_code' : global_response_language_code,
             'product_code' : global_response_product_code,
             'product_model_code' : global_response_product_model_code,
             'product_group_code' : global_response_product_group_code,
             'locale_cd' : global_response_locale_code,
             'question' : message_content,
             'chat_session_id' : global_response_chat_session_id,
             'rag_order' : ragorder}
    
    return query

# Extract strings between specific patterns ( ai_search/AISearch/search, Preprocessing/contents_to_storage ) -- PRD/DEV
def extract_data(text, start, end=None):
    logger.info("###### Function Name : extract_data")
    data_list = []
    start_index = text.find(start)

    if start_index != -1:
    
        while start_index != -1:
            start_index += len(start)
            
            if end is None:
                data = text[start_index:].strip()
                data_list.append(data)
                break
            else:
                end_index = text.find(end, start_index)
                if end_index != -1:
                    data = text[start_index:end_index].strip()
                    data_list.append(data)
                    start_index = text.find(start, end_index)
                else:
                    break

    elif start_index == -1:
        start_index = text.find("(1)EVENT_CODE:")
        end_index = text.find("(2)INTENT_CODE:", start_index)\
        
        data = text[start_index+len("(1)EVENT_CODE:"):end_index].strip()
        data_list.append(data)
 
    return data_list

# Remove strings between specific patterns ( Preprocessing/contents_to_storage ) -- DEV
def remove_tag_between(input_string, start, end) :
    logger.info("###### Function Name : remove_tag_between")
    pattern = rf'{re.escape(start)}[^\]]*{re.escape(end)}'
    result = re.sub(pattern, '', input_string)

    return result

# Chunking the data ( Preprocessing/contents_to_storage ) -- DEV
def chunked_texts(content, tokenizer, max_tokens=7000, overlap_percentage=0.1):
    logger.info("###### Function Name : chunked_texts")
    tokens = tokenizer.encode(content)
    overlap_tokens = int(max_tokens * overlap_percentage)
    chunk_list = []
    start_index = 0

    while start_index < len(tokens):
        end_index = min(start_index + max_tokens, len(tokens))
        chunk_tokens = tokens[start_index:end_index]
        chunk = tokenizer.decode(chunk_tokens)
        chunk_list.append(chunk)
        start_index += max_tokens - overlap_tokens
    
    return chunk_list

def remove_duplicates(lst):
    logger.info("###### Function Name : remove_duplicates")
    seen = set()
    result = []
    for item in lst:
        if item not in seen:
            result.append(item)
            seen.add(item)
        else:
            result.append('')
    return result

def remove_emojis(text):
    logger.info("###### Function Name : remove_emojis")
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F700-\U0001F77F"  # alchemical symbols
                               u"\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
                               u"\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
                               u"\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
                               u"\U0001FA00-\U0001FA6F"  # Chess Symbols
                               u"\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
                               u"\U00002702-\U000027B0"  # Dingbats
                               "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

def clean_string(input_string:str) -> str:
    # logger.info("###### Function Name : clean_string")
    cleaned_string = re.sub(r'[^\w\s]','', input_string)
    cleaned_string = re.sub(r' ','', cleaned_string)
    return cleaned_string

# 토큰 수 계산 함수 정의
def num_tokens_from_string(string: str, encoding_name: str="cl100k_base") -> int:
    # logger.info("###### Function Name : num_tokens_from_string")
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens

#[2024.04.25] 수정 - blob에 적재된 csv 파일 read 방식 변경
def calc_stats(mst_blob_paths, as_instance) :
    # num_pages 통계량 계산
    logger.info("###### Function Name : calc_stats")
    cont_client = as_instance.container_client
    
    # -- 검수된 pdf의 페이지수 정보 -> 통계량 계산
    # valid_metadata_blob = cont_client.get_blob_client(mst_blob_paths['inspection_pdf_metadata'])
    # valid_metadata  = pd.read_csv(valid_metadata_blob.download_blob())
   
    valid_metadata = as_instance.read_file(mst_blob_paths['inspection_pdf_metadata']) # [2024.04.25]수정
    valid_metadata2 = valid_metadata[~valid_metadata['REGIST_YEAR'].isnull()].reset_index(drop=True) # zip 파일에서 추출된 검증 pdf의 경우 제외
    valid_metadata2['REGIST_YEAR_TYPE'] = ["since_2016" if x >= 2016 else "before_2016" for x in valid_metadata2['REGIST_YEAR']]
    
    valid_desc = valid_metadata2.groupby(['PROD_GROUP_CD', 'REGIST_YEAR_TYPE'])[['NUM_PAGES']]\
                    .describe().reset_index()
    valid_desc = valid_desc.copy()
    valid_desc.columns = ['_'.join(col).replace("NUM_PAGES", "NP") if col[1] != "" else "".join(col) for col in valid_desc.columns.values]
    valid_desc['type'] = "valid"

    # -- 매뉴얼 목록 중 pdf의 페이지수 정보 -> 통계량 계산
    # list_metadata_blob = cont_client.get_blob_client(mst_blob_paths['manual_list_pdf_metadata'])
    # list_metadata = pd.read_csv(list_metadata_blob.download_blob())
    list_metadata = as_instance.read_file(mst_blob_paths['manual_list_pdf_metadata'])
    list_desc = list_metadata.groupby(['PROD_GROUP_CD', 'REGIST_YEAR_TYPE'])[['NUM_PAGES']]\
                    .describe().reset_index()
    list_desc = list_desc.copy()
    list_desc.columns = ['_'.join(col).replace("NUM_PAGES", "NP") if col[1] != "" else "".join(col) for col in list_desc.columns.values]
    list_desc['type'] = "manual_list"
    
    # -- 두 개 통계량 중 검수 결과 통계량 우선 선택
    tmp_desc = pd.concat([list_desc, valid_desc]).reset_index(drop=True)

    std = tmp_desc[['PROD_GROUP_CD', 'REGIST_YEAR_TYPE']].drop_duplicates().reset_index(drop=True)

    std_desc = pd.DataFrame([])
    for idx in std.index:
        idx_x    = std.loc[idx]
        idx_desc =  tmp_desc[(tmp_desc['PROD_GROUP_CD'] == idx_x['PROD_GROUP_CD']) & (tmp_desc['REGIST_YEAR_TYPE'] == idx_x['REGIST_YEAR_TYPE'])]
        
        if len(idx_desc) > 1:
            # 통계량이 manual_list, valid에 모두 존재할 경우
            std_desc = pd.concat([std_desc, idx_desc[idx_desc['type']=="valid"]])
        else :
            std_desc = pd.concat([std_desc, idx_desc])
    # end for-loop

    return std_desc.reset_index(drop=True)

def read_pdf_using_fitz(sas_url):
    logger.info("###### Function Name : read_pdf_using_fitz")
    request_blob = requests.get(sas_url)
    filestream = io.BytesIO(request_blob.content)
    
    # PyMuPDF 사용해 TEXT 추출
    with fitz.open(stream=filestream, filetype="pdf") as pdf_data:
        all_text = [page.get_text() for page in pdf_data]
    return all_text

# [2024.04.25] 추가
def make_html_group(html_fname_list):
    # html 파일명 패턴을 사용한 정보 취합
    # - ch\d+_s\d+ 를 파일명에 포함하는 html을 ch\d+로 그룹화
    logger.info("###### Function Name : make_html_group")
    pattern_2 = re.compile(r'ch\d+_s00_')
    group_dict = {}
    # ch**_s00의 파일명 사용해서 group명 생성
    for x in html_fname_list :
        matches = pattern_2.search(x)
        if matches:
            chpt_txt  = x.split("/")[-1].split("_s00_")[0]
            group_str = x.split("/")[-1].split("_s00_")[-1].replace(".html","")
            group_dict[chpt_txt] = {"group_name":group_str, 'html_list':[]}
    
    # 동일 ch 모음 - s00 제외(일반적으로 s01~ 등의 목차 내용)
    for x in html_fname_list :
        chpt_txt = x.split("/")[-1].split("_")[0]
        if x.split("/")[-1].split("_")[1] != "s00":
            group_dict[chpt_txt]['html_list'].append(x)
    # sorting
    for ch_txt, v in group_dict.items() :
        ch_html_list = v['html_list']
        v['html_list'] = sorted(ch_html_list, key=lambda x : int(x.split("/")[-1].split("_")[1].replace("s",""))) # 정렬
    
    return group_dict

# [2024.04.25] 추가
def extract_page_patterns(input_string:str):
    ## 정규 표현식을 사용해 "<page:x>" 형태의 패턴을 찾음.
    logger.info("###### Function Name : extract_page_patterns")
    pattern = re.compile(r'<page:\d+>')
    matches = pattern.findall(input_string)
    return matches

# [2024.04.25] 추가
def to_structured_1(section_str:str, tokenizer, str_info:dict):
    # section 텍스트를 토큰수 기준 분할 및 구조화
    logger.info("###### Function Name : to_structured_1")
    encoding_result = tokenizer.encode(section_str)
    structured_list = []
    if len(encoding_result) > 8000 :
        chunked_list = chunked_texts(section_str, tokenizer = tokenizer, max_tokens=7000, overlap_percentage=0.1)
        for i, chunk in enumerate(chunked_list) :
            document_structured_1 = {
                        "title" : str_info['group_name']
                        , "product_model_code" : str_info['pr_model_cd_str']
                        , "pages" : str_info['pages']
                        , "main_text" : chunk
                    }
            structured_list.append(document_structured_1)
            
    else :
        document_structured_1 = {
                        "title" : str_info['group_name']
                        , "product_model_code" : str_info['pr_model_cd_str']
                        , "pages" : str_info['pages']
                        , "main_text" : section_str
                    }
        structured_list.append(document_structured_1)
    
    return structured_list

# [2024.04.25] 추가
def split_text_by_batch(full_text, num_pages, page_batch_size = 40):
    # GPT 실행용 텍스트 분할 리스트 생성 : page_batch_size 단위로
    logger.info("###### Function Name : split_text_by_batch")
    data_list = []
    if (num_pages < page_batch_size + 10) & (num_tokens_from_string(full_text) <= 120000):
        # batch 크기 + 10 보다  페이지 수가 작고, 전체 토큰수가 120K 이하이면
        #   전체 텍스트를 사용
        data_list.append({"start_page": 1
                          , "end_page": num_pages
                          , "data" : full_text
                        })
        
    elif (num_pages < page_batch_size + 10) & (num_tokens_from_string(full_text) > 120000):
        # batch 크기 + 10 보다  페이지 수가 작고, 전체 토큰수가 120K 이상이면
        #   절반으로 나누기
        for i in range(2):
            st_page_num = int(num_pages/2 * i + 1)
            ed_page_num = int(num_pages/2 * (i+1) + 1)
            if ed_page_num > num_pages:
                ed_page_num = None
                data_list.append({"start_page": st_page_num
                                 , "end_page": num_pages
                                 , "data" :full_text[
                                                full_text.find(f"<page:{st_page_num}>"):
                                            ]
                                })
                
            else:
                data_list.append({"start_page": st_page_num
                                 , "end_page": ed_page_num-1
                                 , "data" :full_text[
                                                full_text.find(f"<page:{st_page_num}>"):
                                                full_text.find(f"<page:{ed_page_num}>")
                                            ]
                                })

    else :
        # 그 외
        ## 읽어온 파일을 page_batch_size 페이지 단위로 분할
        for idx in range(math.ceil(num_pages/page_batch_size)):
            st_page_num = page_batch_size * idx + 1
            ed_page_num = page_batch_size * (idx+1) + 1
            
            if ed_page_num > num_pages:
                ed_page_num = None
                sliced_text = full_text[
                    full_text.find(f"<page:{st_page_num}>"):
                ]
            else:
                sliced_text = full_text[
                    full_text.find(f"<page:{st_page_num}>"):
                    full_text.find(f"<page:{ed_page_num}>")
                ]

            # 토큰수 확인
            if (num_tokens_from_string(sliced_text) <= 120000) & (ed_page_num != None):
                data_list.append({"start_page": st_page_num
                                , "end_page": ed_page_num-1
                                , "data" : sliced_text
                                })
                
            elif (num_tokens_from_string(sliced_text) <= 120000) & (ed_page_num == None):
                data_list.append({"start_page": st_page_num
                                , "end_page": num_pages
                                , "data" : sliced_text
                                })
                
            elif (ed_page_num == None):
                # 토큰수가 120K 초과이고, ed_page_num이 none인경우
                half_page_num = int(st_page_num + (num_pages-st_page_num)/2)
                data_list.append({"start_page": st_page_num
                                , "end_page": half_page_num-1
                                , "data" : sliced_text[
                                            sliced_text.find(f"<page:{st_page_num}>"):
                                            sliced_text.find(f"<page:{half_page_num}>")
                                        ]
                            })
                
                data_list.append({"start_page": half_page_num
                                , "end_page": num_pages
                                , "data" : sliced_text[
                                            sliced_text.find(f"<page:{half_page_num}>"):
                                        ]
                            })
            
            else : 
                # 토큰수가 120K 초과이고, ed_page_num이 none이 아닌 경우
                half_page_num = int(st_page_num + page_batch_size/2)
                data_list.append({"start_page": st_page_num
                                , "end_page": half_page_num-1
                                , "data" : sliced_text[
                                            sliced_text.find(f"<page:{st_page_num}>"):
                                            sliced_text.find(f"<page:{half_page_num}>")
                                        ]
                            })
                
                data_list.append({"start_page": half_page_num
                                , "end_page": ed_page_num-1
                                , "data" : sliced_text[
                                            sliced_text.find(f"<page:{half_page_num}>"):
                                            sliced_text.find(f"<page:{ed_page_num}>")
                                        ]
                            })
    return data_list


# [2024.04.25] 추가
def get_system_mssg(pgcd, pcd, group_mapping):
    ## 제품군 또는 제품에 따른 섹션 그룹 정보 매핑 & 섹션 분할 시스템 프롬프트 생성
    logger.info("###### Function Name : get_system_mssg")
    if len(group_mapping[(group_mapping.key == "product_code") 
                         & (group_mapping.value == pcd)]) != 0 :
        # product_code의 그룹 존재여부 확인
        groups = list(group_mapping[(group_mapping.key == "product_code") & (group_mapping.value == pcd)].group_name.unique())

    elif len(group_mapping[(group_mapping.key == "product_group_code") 
                           & (group_mapping.value == pgcd)]) != 0 :
        # product_group_code의 그룹 존재여부 확인
        groups = list(group_mapping[(group_mapping.key == "product_group_code") & (group_mapping.value == pgcd)].group_name.unique())

    else :
        # 아무것도 매핑되지 않는 경우, product_group_code = 'OTH' 사용
        groups = list(group_mapping[(group_mapping.key == "product_group_code") & (group_mapping.value == "OTH")].group_name.unique())
    
    rule_txt = """As an insightful customer support analyst for LG Electronics, your task is to classify pages of the given <Documents>.\nThe <Documents> are Owner's manual of LG Electronics products.\nYou Must adhere to <Rules>.\n\n<Rules>\n- You MUST classify <Documents> into only ONE category per page.\n- If the content of a page corresponds to several categories, MUST classify it into only ONE Category with the most relevant.\n- There are description of <Categories> is as follows."""
    
    category_txt = "<Categories>\n"
    for ind, g in enumerate(groups):
        category_txt = category_txt + f"{ind+1}. {g}\n"
    
    output_txt = """The output should be of the following form\n```text\n- Page {number}: {category}\n- Page {number}: {category}\n- Page {number}: {category}\n```"""
    
    return f"{rule_txt}\n{category_txt}\n{output_txt}", groups

# [2024.04.25] 추가
def parse_text_to_dict(input_text:str):
    # -- 생성된 텍스트를 딕셔너리로 변환
    logger.info("###### Function Name : parse_text_to_dict")
    # 결과를 저장할 딕셔너리
    result_dict = {}
    # 입력 텍스트를 줄 단위로 분할
    lines = input_text.split("\n")
    
    for line in lines:
        # 각 줄에서 페이지 번호와 텍스트 분리
        if ": " in line:
            # ":" 기호를 기준으로 분리하여 페이지 번호와 텍스트를 추출
            page_info, text = line.split(": ")
            # 페이지 번호에서 숫자만 추출
            page_number = int(page_info.split(" ")[-1])
            # 딕셔너리에 텍스트를 키로 하여 페이지 번호 추가
            if text in result_dict:
                result_dict[text].append(page_number)
            else:
                result_dict[text] = [page_number]        
    return result_dict

# [2024.04.25] 추가
def merge_dicts(dict1, dict2):
    # -- 두 개의 딕셔너리 병합
    logger.info("###### Function Name : merge_dicts")
    # 결과 딕셔너리 초기화
    result = {}

    # 두 딕셔너리의 키 합집합을 순회
    for key in set(dict1) | set(dict2):
        if key in dict1 and key in dict2:
            # 리스트의 경우 합치고 정렬
            if isinstance(dict1[key], list) and isinstance(dict2[key], list):
                result[key] = sorted(dict1[key] + dict2[key])
            # 딕셔너리의 경우 재귀적으로 통합
            elif isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                result[key] = merge_dicts(dict1[key], dict2[key])
            else:
                # 같은 키가 있지만 타입이 다른 경우 리스트로 담아 통합
                result[key] = sorted(list(dict1[key]) + list(dict2[key]))
        elif key in dict1:
            result[key] = dict1[key]
        else:
            result[key] = dict2[key]
    return result

def run_indexer(logger, indexer_list, indexer_client):
    logger.info("###### Function Name : run_indexer")
    success_indexers = set()
    date_object = datetime.strptime(get_utc_now().split(' ')[0], '%Y-%m-%d')
    utc_date_object = date_object.replace(tzinfo=pytz.UTC)

    while len(success_indexers) < len(indexer_list):
        for indexer in indexer_list:
            if indexer not in success_indexers:
                print(indexer)
                success = False
                while not success:
                    try:
                        indexer_client.run_indexer(indexer + '-indexer')
                        print(f"########## {indexer + '-indexer'} Start Success")
                        time.sleep(120)
                        status = indexer_client.get_indexer_status(name=f"{indexer}-indexer")
                        if status.execution_history[0].status == 'success' and \
                        status.execution_history[0].end_time > utc_date_object :
                            success_indexers.add(indexer)
                            success = True
                            logger.info(f"SUCESS {indexer}")
                        else:
                            print(f"########## {indexer + '-indexer'} not yet successful. Retrying...")
                    except Exception as e:
                        error_message = str(e)
                        if "was not found" in error_message:
                            print(f"########## Indexer '{indexer}' not found. Skipping...")
                            success_indexers.add(indexer) 
                            success = True  
                        else:
                            logger.error(f"########## Error : {error_message}")
                            time.sleep(120)
                            
    
    logger.info("All indexers processed.")            
    return None


def filter_logs_by_data_type(logs, data_type):
    filter_chat = str(data_type)
    return [log for log in logs if filter_chat in log]

def get_kst_now():

    korea_time_zone = pytz.timezone('Asia/Seoul')
    utc_now = datetime.now(timezone.utc)
    kst_now = utc_now.astimezone(korea_time_zone)
    return kst_now.strftime('%Y-%m-%d %H:%M:%S')

def get_utc_now():
    utc_now = datetime.utcnow()
    return utc_now.strftime('%Y-%m-%d %H:%M:%S')

def update_index_count(logger, search_instance, index_list):

    korea_timezone = pytz.timezone('Asia/Seoul')
    now = datetime.now(korea_timezone)
    formatted_date = now.strftime('%Y-%m-%d')
    index_client = search_instance.index_client
    
    results_list = []
    for index_name in index_list:
        count = 0
        results = {'index_name': index_name}
        for key in ['contents', 'youtube', 'spec', 'general-inquiry', 'microsites']:
            result = search_instance.get_documents(index_name=index_name, type='type', mapping_key=key)
            results[key] = len(result)
            count += len(result)
            
        index_stats = index_client.get_index_statistics(index_name)
        total = search_instance.get_num_documents(index_name=index_name)
        results['manual'] = total - count
        results['total'] = total
        results['storage_size'] = index_stats['storage_size']
        results['vector_index_size'] = index_stats['vector_index_size']
        results['create_time'] = formatted_date
        results['create_user'] = 'ADM'
        logger.info(f"[Data]\n{json.dumps(results, indent=4, ensure_ascii=False)}")

        sql_instance = MySql()
        check_query = """
                      SELECT 1 
                      FROM tb_chat_index_prc 
                      WHERE INDEX_NAME = %s AND CREATE_DATE = %s
                      """
        existing_record = sql_instance.get_table(check_query, (index_name, formatted_date))

        if existing_record:
            # Update existing record
            sql_instance = MySql()
            update_query = """
                           UPDATE tb_chat_index_prc
                           SET CONTENTS = %s, YOUTUBE = %s, SPEC = %s, MICROSITES = %s, GENERAL_INQUIRY = %s, MANUAL = %s, TOTAL = %s,
                               STORAGE_SIZE = %s, VECTOR_INDEX_SIZE = %s,
                               CREATE_USER = %s
                           WHERE INDEX_NAME = %s AND CREATE_DATE = %s
                           """
            parameters = (
                results['contents'], results['youtube'], results['spec'], results['microsites'], results['general-inquiry'],
                results['manual'], results['total'], results['storage_size'], results['vector_index_size'], results['create_user'], index_name, formatted_date
            )
            sql_instance.insert_data(update_query, parameters)
        else:
            # Insert new record
            sql_instance = MySql()
            insert_query = """
                           INSERT INTO tb_chat_index_prc (INDEX_NAME, CONTENTS, YOUTUBE, SPEC, MICROSITES, GENERAL_INQUIRY, MANUAL, TOTAL, STORAGE_SIZE, VECTOR_INDEX_SIZE, CREATE_DATE, CREATE_USER)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                           """
            parameters = (
                index_name, results['contents'], results['youtube'], results['spec'], results['microsites'], results['general-inquiry'],
                results['manual'], results['total'], results['storage_size'], results['vector_index_size'], formatted_date, results['create_user']
            )
            sql_instance.insert_data(insert_query, parameters)

        results_list.append(results)

    return results_list

def batch_delete_documents(search_instance, data, index_mst):
    del_info = [item.split('/') for item in json.loads(data)['del_list']]
    index_del_list = [f"{item[3]}_{item[1]}_{item[2]}_{item[4]}_{item[5]}_{item[-1].split('.')[0]}" for item in del_info]
    for mapping_key in index_del_list :
        nation = mapping_key.split('_')[1]
        lang_cd = index_mst[index_mst['CODE_NAME'] == mapping_key.split('_')[2]]['CODE_CD'].iloc[0]
        index_name = nation.lower()+'-'+lang_cd
        try : 
            result = search_instance.get_documents(index_name=index_name, type='mapping_key',mapping_key=mapping_key)
            id_list = [item['id'] for item in result]
            for value in id_list :
                try : 
                    search_instance.delete_document(index_name=index_name ,key='id',key_value=value)
                except Exception as e :
                    continue
        except Exception as e :
            continue

def extract_main_text_microsite(url) :

    response = requests.get(url)

    if response.status_code == 200:
        # HTML 파싱
        soup = BeautifulSoup(response.content, 'html.parser')
        main_content = soup.find('div', {'id': 'main-content'})
        
        if main_content is None:
            main_content = soup.find('main')  
        
        if main_content is None:
            main_content = soup.find('article') 
        
        if main_content is None:
            main_content = soup.find('div', {'class': 'content'}) 

        image_urls = []
        video_urls = []
        
        if main_content:
            
            for img in main_content.find_all('img'):
                img_url = img.get('src')
                if img_url:
                    image_urls.append(img_url)

            for video in main_content.find_all('video'):
                video_url = video.get('src')
                if video_url:
                    video_urls.append(video_url)

                for source in video.find_all('source'):
                    source_url = source.get('src')
                    if source_url:
                        video_urls.append(source_url)
                        
            for ul in main_content.find_all('ul'):
                list_items = ul.find_all('li')
                processed_items = []
                for li in list_items:
                    for a in li.find_all('a'):
                        a_text = f"({a.get_text()})"
                        a.replace_with(a_text)
                    processed_items.append(li.get_text())
                comma_separated_text = ', '.join(processed_items)
                ul.clear()
                ul.append(comma_separated_text)

            # #만족도 조사 제외
            # h2_tags = main_content.find_all('h2')
            # if h2_tags:
            #     h2_tags[-1].decompose()
                
            for tag in main_content.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                
                tag_text = tag.get_text().replace('\n', '').replace('\r', '').strip()
                
                if tag_text:
                    tag.string = tag_text
                    tag.insert_before('[ ')
                    tag.insert_after(' ]')
                else:
                    tag.decompose()
                
            text = main_content.get_text()
            text = text.replace('\r\n','').replace(u"\xa0",u"").replace('  ','').replace('\t','').replace('\n,','\n').replace('\n','\n\n')
            text = re.sub(r'\n{2,}', '\n\n', text)

        else:
            text = " "
            print("Main content not found.")
    else:
        text = " "
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
    
    return text, image_urls, video_urls
        




