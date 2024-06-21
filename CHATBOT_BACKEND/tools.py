import uuid
import json
import pytz
from datetime import datetime
import logging
from database import AzureStorage, MySql, CosmosDB
import re
import tiktoken
import requests
import os
from log import setup_logger
from retry import retry

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from base64 import b64encode, b64decode
import os

logger = logging.getLogger(__name__)

class OpenAIResourceError(Exception):
    pass

class AuthorizationError(Exception):
    pass

# Create a prompt for display ( chat/__init__ ) -- PRD
def preprocess_thoughts_process(thoughts_process, documents, query, chat_history, answer):
    logger = setup_logger(query['chatid'],query['chat_session_id'], query['rag_order'])
    logger.info("###### Function Name : preprocess_thoughts_process")
    if chat_history == None :
        chat_history = ''
    
    thoughts_process = thoughts_process.replace("{document}",documents)
    thoughts_process = thoughts_process.replace("{question}",query['question'])
    thoughts_process = thoughts_process.replace("{chat}",chat_history)
    thoughts_process = thoughts_process.replace("<","[")
    thoughts_process = thoughts_process.replace(">","]")
    thoughts_process = thoughts_process.replace("==============","<br>==============<br>")

    if query['trans_language_code_score'] != 1:
        detectLang = query['user_selected_language']
    else :
        detectLang = query['trans_language']
    
    thoughts_process = thoughts_process.replace("{detectLang}",detectLang)

    thoughts_process = thoughts_process + answer

    return thoughts_process

@retry(tries=3, delay=1, backoff=1)
def load_balancing(token_size, type):
    logger.info("###### Function Name : load_balancing")

    url = "{URL}"
    if type == "gpt":
        
        data = {
        "tokenSize": token_size,
        "env" : "conv"
    }
    else :
        data = {
        "tokenSize": token_size,
        "env" : "emb"
    }

    headers = {
        "Content-Type": "application/json"
    }

    request = requests.post(url, json=data, headers=headers)
    response = request.json()
    
    if response['code'] == "E0000" :
        try :
            apiBase = response['result']['apiBase']
            apiKey = response['result']['apiKey']
            apiVersion = response['result']['apiVersion']
            apiModel = response['result']['apiModel']

        except :
            apiBase = None
            apiKey = None
            apiVersion = None
            apiModel = None
            logger.info("There is no response request. Proceed with default settings.")
        
        return apiBase, apiKey, apiVersion, apiModel
    
    else :
        raise OpenAIResourceError(f"{response['code']} : There are no openai resources available.")

    

# Create fixed answer ( chat/__init__ ) -- PRD
def preprocess_answer(trans_instance, query, answer, preprocessed, refined_info):
    logger = setup_logger(query['chatid'],query['chat_session_id'], query['rag_order'])
    logger.info("###### Function Name : preprocess_answer")
    if (len(preprocessed[0]['title']) == 0) and (len(preprocessed[1]['title']) == 0) and (len(preprocessed[2]['title']) == 0): 
        answer = "I am constantly learning to assist you better. In the meantime, please click on the “Live Chat” button to connect with one of our agents who will be happy to help you."
        
        if query['trans_language_code_score'] != 1:
            detectLang = query['user_selected_language_code']
        else :
            detectLang = query['trans_language_code']
        
        if query["trans_language_code"] != "en":
                body = [{'text': answer}]
                try :
                    answer = trans_instance.translate_question(language_from="en", language_to=detectLang, body=body)
                except:
                    answer = trans_instance.translate_question(language_to='en', body=body)
                answer = answer[0]['translations'][0]['text']
        else:
                pass

    elif len(refined_info) != 0:
        if query['trans_language_code_score'] != 1:
            detectLang = query['user_selected_language_code']
        else :
            detectLang = query['trans_language_code']
        
        if query["trans_language_code"] != "en":
            body = [{'text': refined_info}]
            try :
                refined_info = trans_instance.translate_question(language_from="en", language_to=detectLang, body=body)
            except:
                refined_info = trans_instance.translate_question(language_to='en', body=body)
            refined_info = refined_info[0]['translations'][0]['text']
        else:
            pass

        answer = refined_info + "\n\n" + answer
        answer = "[Inform]\n\n" + answer
    else:
        pass
   
    return answer

# Get chat history ( chat/__init__ ) -- PRD
def load_chat_data(db_instance, db_name, container_name, chatid, chat_session_id):
    logger.info("###### Function Name : load_chat_data")
    chat_data = db_instance.read_data(db_name, container_name, chatid, chat_session_id)
        
    if len(chat_data) == 0 :
            chat_history = None

    else :
        chat_data = sorted(chat_data, key=lambda x: x['chat_order'])

        chat_history = []
        for item in chat_data:
            role = item['chat_role'].replace('assistant','chat bot').replace('user','customer')
            # ''(INIT), None(LINK_CLICK), [](INIT), [None](FINISH)
            if not item['message']:
                item['message'] = ['']
            elif isinstance(item['message'], list):
                if not item['message'] or item['message'][0] is None:
                    item['message'] = ['']
            
            #print(item)
            message_list = item['message']
            #message_list = ["" if message is None else message for message in message_list]
            context = '\n'.join(message_list)
            context = remove_tag_between(context, start='<a href=', end='</a>')
            chat_history.append({"role" : role, "context" : context})
        
        chat_history = chat_history[-11:-1]
        chat_history = '\n\n'.join([f"{item['role']}: {item['context']}" for item in chat_history])

    return chat_history

# Get prompt ( model/generate_answer ) -- PRD
def get_prompt(prompt_path):
    logger.info("###### Function Name : get_prompt")
    with open(prompt_path, "r", encoding="utf-8") as f:
        prompt = f.read()
    
    return  prompt

# Upload chat history ( chat/__init__ ) -- PRD
def upload_chat_data(db_instance, db_name, container_name, output) :
    logger.info("###### Function Name : upload_chat_data")
    db_instance.upload_data(db_name, container_name, output)

    return "Upload Chat data"

# Preprocessing of final results to be delivered ( chat/__init__) -- PRD
def preprocess_output_data(trans_instance, preprocessed, query, thoughts_process, answer, paa, time_info, prompt, gpt_model, all_data) :
    logger = setup_logger(query['chatid'],query['chat_session_id'], query['rag_order'])
    logger.info("###### Function Name : preprocess_output_data")
    results = {}

    for key in all_data :
        
        preprocessed = all_data[key]
    
        sas_urls = {}
        for i in range(len(preprocessed)):
            
            if preprocessed[i]['type'] == 'manual' :
                pages = preprocessed[i]['pages']
                page_list = pages.split(',')
                page_list = [int(x) for x in page_list]
                title_page = min(page_list)
                as_instance_web = AzureStorage(container_name='$web',storage_type='docst')
                try :
                    manual_url = as_instance_web.get_sas_url(preprocessed[i]['url']) + f'#page{title_page}'
                except :
                    manual_url = ""

                sas_urls[f"{preprocessed[i]['type']}_{i}"] = manual_url
            
            else:
                sas_urls[f"{preprocessed[i]['type']}_{i}"] = preprocessed[i]['url']
        
        sas_urls_values = list(sas_urls.values())
        tokenizer = tiktoken.get_encoding("cl100k_base")
        tokenizer = tiktoken.encoding_for_model(gpt_model)
        
        ref_prompt_tokens = len(tokenizer.encode(prompt))
        ref_completion_tokens = len(tokenizer.encode(query["refined_answer"]))

        if query['flag'] == "RAG" :

            substring = "[answer]"
            index = thoughts_process.find(substring)
            if index != -1:
                prompt = thoughts_process[:index + len(substring)]
            else:
                prompt = thoughts_process

            prompt_tokens = len(tokenizer.encode(prompt))
            completion_tokens = len(tokenizer.encode(answer))

        else :
            prompt_tokens = 0
            completion_tokens = 0

        if len(paa) == 0:
            paa_list=["","",""]
        else :
            paa_list = [extract_data(paa,start='1.',end='2.')[0].replace("None", ""),
                        extract_data(paa,start='2.',end='3.')[0].replace("None", ""),
                        extract_data(paa,start='3.')[0].replace("None", "")]
            
        logger.info(f"###### paa_list : {paa_list}")
        logger.info(f"###### answer : {answer}")
        if key =='result' :
            
            ## 변환 결과인 Answer_temp 지정
            answer_pretty = ""

            ## answer를 "\n" 단위로 분리
            lines = answer.split('\n')

           ## 머릿말 안내문구 Bold체 변환 방지
            for i in range(len(lines)):
                ## ":"이 라인이 검출되면, 다음 라인에도 ":"이 있는지 확인하여, 그 여부에 따라 처리
                if lines[i].endswith(":"):
                    ## list indexing 에러 방지
                    try: 
                        ## ":" 이 있을 경우, 다음 라인에도 ":"이 있는지 확인
                        if lines[i+1].endswith(":"):
                            ## 머릿말 안내문구에 대하여 줄바꿈 개행문자 대신 "<br>" 추가 : 0번 Index가 맞는지 여부에 따라 <br> 추가
                            if i == 0:
                                lines[i] =lines[i][:-1]
                            else:
                                lines[i] = '<br>' + lines[i][:-1]
                            ## 머릿말 안내문구가 확인되었으므로 반복문 정지
                            break
                    except: ## Indexerror
                        continue
                ## ":"이 라인이 검출되지 않는 경우 다음 line 확인
                else:
                    continue
                    
            ## 꼬릿말 안내문구 줄바꿈
            for i in range(len(lines) - 1, 0, -1):
                ## 가장 마지막 줄에서부터 시작하여 "-" 또는 "(숫자)."으로 시작하는 라인이 검출되면, 다음 라인이 꼬릿말 안내문구라고 가정하여 처리
                if '-' in lines[i] or lines[i].strip().split('.')[0].isdigit():
                    ## 다음 라인이 없거나 다음 라인이 공백인 경우에만 pass
                    if i + 1 >= len(lines) or not lines[i + 1].strip():
                        continue
                    ## 다음 라인이 '-'나 숫자로 시작하지 않는 경우에만 <br> 추가
                    if not lines[i + 1].strip().startswith('-') and not lines[i + 1].strip().split('.')[0].isdigit():
                        lines[i + 1] = '<br>' + lines[i + 1]
                    ## 꼬릿말 안내문구가 확인되었으므로 반복문 정지
                    break


            ## 분리된 각 Line들에 대하여 처리
            for line in lines:
                line = line.strip()

                ## Line의 마지막이 ":"으로 종료되면, Bold체로 변환 및 줄바꿈 변경
                if line.endswith(":"):
                    answer_pretty += "\n" + "<strong>" + line + "</strong>" + "\n"
                ## 그렇지 않으면 "\n" 2개
                else:
                    answer_pretty += line + "\n"

            ## 개행문자 "\n"을 "<br>"으로 변환
            answer = answer_pretty[:].strip()
            answer = answer.replace("\n", "<br>")
            logger.info(f"###### answer : {answer}")

            contentskeywords = extract_data(query['refined_query'], start="keywords:", end="symptom")[0].split(',')
            contentskeywords = [item.replace("None","").strip() for item in contentskeywords]

            output_json= {
                "gpt" : answer,
                "eventCd" : query['eventcd'],
                "detectLanguage" : query['trans_language_code'],
                "userSelectLanguage" : query['user_selected_language_code'],
                "indexName" : query['index_name'],
                "contentKeywords" : contentskeywords,
                "contentIds":[f"{preprocessed[i]['data_id']}" if preprocessed[i]['type'] in ['contents','microsites'] else "" for i in range(len(preprocessed))],
                "youtubeIds":[f"{preprocessed[i]['data_id']}" if preprocessed[i]['type'] in ['youtube'] else "" for i in range(len(preprocessed))],
                "specIds":[f"{preprocessed[i]['data_id']}" if preprocessed[i]['type'] in ['spec'] else "" for i in range(len(preprocessed))],
                "itemIds":[f"{preprocessed[i]['data_id']}" if preprocessed[i]['type'] in ['manual'] else "" for i in range(len(preprocessed))],
                "additionalInfo" : {
                    "intent": query['intent'],
                    "refDocName" : remove_duplicates([f"{preprocessed[i]['title']}" if preprocessed[i]['type'] in ['contents','manual',"general-inquiry","microsites"] else "" for i in range(len(preprocessed))]),
                    "refDocUrl": remove_duplicates([f"{sas_urls[key]}" if 'contents' in key or 'manual' in key or 'general-inquiry' in key or 'microsites' in key else "" for key in sas_urls]),
                    "etcLinkName": [f"{preprocessed[i]['title']}" if preprocessed[i]['type'] in ['youtube'] else "" for i in range(len(preprocessed))],
                    "etcLinkUrl": [f"{sas_urls[key]}" if 'youtube' in key else "" for key in sas_urls],
                    "refDocScore": [f"{preprocessed[i]['score']}" for i in range(len(preprocessed))],
                    "call1PromptToken": ref_prompt_tokens,
                    "call1CompletionToken": ref_completion_tokens, 
                    "call2PromptToken": prompt_tokens,  
                    "call2CompletionToken": completion_tokens,
                    "time": {
                        "Refinement": time_info["Refinement"],
                        "ir": time_info["IR"],
                        "answer": time_info["answer"],
                        "total": time_info["total"]
                    },
                    "contexts": [f"{preprocessed[i]['main_text']}" for i in range(len(preprocessed))],
                    "paa" : [paa_list[i]  for i in range(len(paa_list))],
                    "originalQuery" : query['question'],
                    "refinedQuery": query['refined_query'],
                    "thoughts" : thoughts_process
                }
            }

        else :
                output_json= {
                "originalQuery" : query['question'],
                "eventCd" : query['eventcd'],
                "detectLanguage" : query['trans_language_code'],
                "userSelectLanguage" : query['user_selected_language_code'],
                "indexName" : query['index_name'],
                "contentIds":[f"{preprocessed[i]['data_id']}" if preprocessed[i]['type'] in ['contents','microsites'] else "" for i in range(len(preprocessed))],
                "youtubeIds":[f"{preprocessed[i]['data_id']}" if preprocessed[i]['type'] in ['youtube'] else "" for i in range(len(preprocessed))],
                "specIds":[f"{preprocessed[i]['data_id']}" if preprocessed[i]['type'] in ['spec'] else "" for i in range(len(preprocessed))],
                "itemIds":[f"{preprocessed[i]['data_id']}" if preprocessed[i]['type'] in ['manual'] else "" for i in range(len(preprocessed))],
                "additionalInfo" : {
                    "intent": query['intent'],
                    "refDocName": [f"{preprocessed[i]['title']}" for i in range(len(preprocessed))],
                    "refDocUrl": [f"{sas_urls_values[i]}" for i in range(len(sas_urls_values))],
                    "refDocScore": [f"{preprocessed[i]['score']}" for i in range(len(preprocessed))],
                    "contexts": [f"{preprocessed[i]['main_text']}" for i in range(len(preprocessed))],
                    "refinedQuery": query['refined_query'],
                }
            }

        results[key] = output_json
        
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    final_result = results['result']
    output = json.dumps(final_result,indent=4,ensure_ascii=False)

    results['id'] = str(uuid.uuid4())
    results['chatid'] = query['chatid']
    results['chat_session_id'] = query['chat_session_id']
    results['rag_order'] = query['rag_order']
    
    db_instance = CosmosDB()
    upload_chat_data(db_instance, db_name='lgdb', container_name='aicontainer', output=results)
    upload_chat_summary(query,results)
    
    return output


def preprocess_intent_search(filter_df, search_instance, query) :
    logger = setup_logger(query['chatid'],query['chat_session_id'], query['rag_order'])
    results = {}
    logger.info("###### Function Name : preprocess_intent_search")
    preprocessed = search_instance.intent_search(filter_df, query['index_name'],query,3)
    
    if any(item["type"] == "general-inquiry" for item in preprocessed):
        
        preprocessed = [item for item in preprocessed if item["type"] == "general-inquiry"]
        
        unique_types = set()
        filtered_data = []

        for item in preprocessed:
            if item['type'] not in unique_types:
                unique_types.add(item['type'])
                filtered_data.append(item)

        preprocessed = filtered_data

        korea_timezone = pytz.timezone('Asia/Seoul')
        korea_time = datetime.now(korea_timezone)
        current_time = korea_time.strftime('%Y-%m-%d %H:%M:%S')

        if len(preprocessed) < 3 :

            dummy = [{"timestamp" : current_time,
                    "id": str(uuid.uuid4()),
                    "type": "",
                    "iso_cd": "",
                    "language": "",
                    "product_group_code": "",
                    "product_code": "",
                    "product_model_code": "",
                    "data_id" : "",
                    "mapping_key": "",
                    "chunk_num" : "",
                    "file_name": "",
                    "pages" : "",
                    "url": "",
                    "title": "",
                    "main_text": "",
                    "main_text_path" : "",
                    "score": ""}]
            
            preprocessed += dummy*(3-len(preprocessed))

        results['result'] = preprocessed
        
        documents = ""
        for i in range(len(preprocessed)) :
            documents += f"[{i+1}]\n{preprocessed[i]['main_text']}\n"
    
    else :
        preprocessed = ""
        documents = ""
        results = ""
        logger.info("=" * 30 + "No General Inquiry " + "=" * 30)

    return preprocessed, documents, results

# Preprocess search results ( chat/__init__) -- PRD
def preprocess_documents(filter_df, search_instance, query) :
    logger = setup_logger(query['chatid'],query['chat_session_id'], query['rag_order'])
    logger.info("###### Function Name : preprocess_documents")
    results = {}
    
    preprocessed_contents = search_instance.contents_search(filter_df, query['index_name'],query,3)
    preprocessed_youtube = search_instance.youtube_search(filter_df, query['index_name'],query,3)
    preprocessed_manual = search_instance.manual_search(filter_df, query['index_name'],query,3)
    preprocessed_spec = search_instance.spec_search(filter_df, query['index_name'],query,3)
    
    all_data = []

    for data in [preprocessed_contents, preprocessed_youtube, preprocessed_manual, preprocessed_spec]:
        
        if data is None:
            data = []
        
        all_data += data

    #all_data = preprocessed_contents + preprocessed_youtube + preprocessed_manual + preprocessed_spec
    sorted_results = sorted(all_data, key=lambda x: x['score'], reverse=True)
    # 순위 결정 후 중복 제거
    preprocessed_all = sorted_results[:3]
    
    documents = ""
    for i in range(len(preprocessed_all)) :
        documents += f"[{i+1}]\n{preprocessed_all[i]['main_text']}\n"
    # 중복 제거 후 순위 결정
    # preprocessed_all = sorted_results

    # unique_types = set()
    # filtered_data = []

    # for item in preprocessed_all:
    #     if item['type'] not in unique_types:
    #         unique_types.add(item['type'])
    #         filtered_data.append(item)

    # preprocessed = filtered_data#[:3]
    # #preprocessed_all = preprocessed_all[:3]

    preprocessed = preprocessed_all
    
    logger.info(f"{preprocessed}")
    all_data = {"result" : preprocessed,
                "total_result" : preprocessed_all,
                "contents_result" : preprocessed_contents if preprocessed_contents is not None else [],
                "manual_result" : preprocessed_manual if preprocessed_manual is not None else [], 
                "youtube_result" : preprocessed_youtube if preprocessed_youtube is not None else [],
                "spec_result" : preprocessed_spec if preprocessed_spec is not None else []}

    for key in all_data :
        tmp_preprocessed = all_data[key]
        korea_timezone = pytz.timezone('Asia/Seoul')
        korea_time = datetime.now(korea_timezone)
        current_time = korea_time.strftime('%Y-%m-%d %H:%M:%S')

        if len(tmp_preprocessed) < 3 :
            dummy = [{"timestamp" : current_time,
                    "id": str(uuid.uuid4()),
                    "type": "",
                    "iso_cd": "",
                    "language": "",
                    "product_group_code": "",
                    "product_code": "",
                    "product_model_code": "",
                    "data_id" : "",
                    "mapping_key": "",
                    "chunk_num" : "",
                    "file_name": "",
                    "pages" : "",
                    "url": "",
                    "title": "",
                    "main_text": "",
                    "main_text_path" : "",
                    "score": ""}]
            
            tmp_preprocessed += dummy*(3-len(tmp_preprocessed))
        results[key] = tmp_preprocessed

    return preprocessed, documents, results

# Preprocessing of received parameters ( chat/__init__) -- PRD
def preprocess_query(data) :
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
    current_user = data['param']['currentUser']
    
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
             'rag_order' : ragorder,
             'current_user' : current_user}
    
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
                    data = text[start_index:].strip()
                    data_list.append(data)
                    break

    elif start_index == -1:
        data_list.append("None")
 
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

def preprocessing_answer(logger, answer) :
    logger.info("###### Function Name : preprocessing_answer")

    ## GPT의 JSON Format 출력 결과 후처리
    answer_str_1 = answer.replace("```json", "").replace("```", "").strip()
    logger.info(f"############## answer_str_1 :\n{answer_str_1}")
    
    ## 출력 결과 1차 확인 및 answer_dict로 재구성
    try:
        answer_dict_temp = eval(answer_str_1)
        assert isinstance(answer_dict_temp, dict)

        answer_dict = {"response_language" : answer_dict_temp["response_language"],
                    "response_body" : answer_dict_temp["response_body"],
                    "solution" : answer_dict_temp["solution"],
                    "additional_questions" : " ".join(answer_dict_temp["additional_questions"]),
                    "response" : "\n".join(answer_dict_temp["response_body"])}
        
    ## 출력 결과 2차 확인 및 answer_dict로 재구성
    except Exception as e :

        try :
            logger.info(f"############## answer_str_1 Error : {type(e).__name__} - {e}")

            answer_str_2 = answer.replace("```json", "").replace("```", "").replace(r'{','').replace(r'}','').strip()
            logger.info(f"############## answer_str_2 :\n{answer_str_2}")

            response_language = extract_data(answer_str_2, start="response_language", end= "response_body")[0].replace('\"','').replace(':','').replace('\n','').replace(',','').strip()

            temp_response_body = extract_data(answer_str_2, start="response_body", end= "solution")[0].replace('[','').replace(']','').replace('\"','').\
            replace(':','').replace('    ','').split(',\n')
            response_body = [item.replace('\n','').replace('\t','').strip() for item in temp_response_body if item.strip()]

            try :
                temp_solution = extract_data(answer_str_2, start="solution", end="additional_questions")[0].replace('\"','').replace(':','').replace('\n','').replace(',','').strip()
                solution = 'Yes' if 'Yes' in temp_solution else 'No'
            except :
                solution = 'No'

            temp_additional_questions = extract_data(answer_str_2, start="additional_questions")[0].replace('[','').replace(']','').replace('\"','').\
            replace(':','').replace('    ','').split(',\n')
            additional_questions = [item.replace('\n','').replace('\t','').strip() for item in temp_additional_questions if item.strip()]

            answer_dict = {"response_language" : response_language,
                        "response_body" : response_body,
                        "solution" : solution,
                        "additional_questions" : " ".join(additional_questions),
                        "response" : "\n".join(response_body)}
        
        except Exception as e:
            logger.info(f"############## answer_str_2 Error : {type(e).__name__} - {e}")
            
            answer = "I am constantly learning to assist you better. In the meantime, please click on the “Live Chat” button to connect with one of our agents who will be happy to help you."
            answer_dict = {"response_body" : answer,
                "solution" : "No",
                "additional_questions" : "",
                "response" : answer}

    return answer_dict


def refinement_flag(trans_instance, query):
    logger = setup_logger(query['chatid'],query['chat_session_id'], query['rag_order'])
    logger.info("###### Function Name : refinement_flag")

    flag = query["flag"]
    if flag == "FIX_1":
        answer = "I'm sorry that i can't understand your issue.\n\nCould you tell me in detail the issue or check Error Code on display window.\n\nThank you for your patience"
    elif flag == "FIX_2":
        answer = "It seems the product previously selected differs from your current query.\n\nPlease choose the relevant product again to ensure accurate assistance."
    elif flag == "FIX_3":
        answer = "The Error Code identified in your query could not be verified.\n\nPlease check the Error Code on the display window.\n\nIf you require further assistance, click on the 'Live Chat' button to connect with one of our agents who will be happy to assist you."
    else:
        pass
    
    if query['trans_language_code_score'] != 1:
        detectLang = query['user_selected_language_code']
    else :
        detectLang = query['trans_language_code']
    
    if query["trans_language_code"] != "en":
        body = [{'text': answer}]
        try :
            answer = trans_instance.translate_question(language_from="en", language_to=detectLang, body=body)
        except:
            answer = trans_instance.translate_question(language_to='en', body=body)
        answer = answer[0]['translations'][0]['text']
    else:
        pass
    
    documents = ""
    thoughts_process = ""
    paa = ""
    query['answer'] = answer
    query['eventcd'] = "FIX" ## 임의로 지정함
    query['intent'] = "FIX" ## 임의로 지정함
    
    korea_timezone = pytz.timezone('Asia/Seoul')
    korea_time = datetime.now(korea_timezone)
    current_time = korea_time.strftime('%Y-%m-%d %H:%M:%S')

    preprocessed = []
    if len(preprocessed) < 3 :
            dummy = [{"timestamp" : current_time,
                    "id": str(uuid.uuid4()),
                    "type": "",
                    "iso_cd": "",
                    "language": "",
                    "product_group_code": "",
                    "product_code": "",
                    "product_model_code": "",
                    "data_id" : "",
                    "mapping_key": "",
                    "chunk_num" : "",
                    "file_name": "",
                    "pages" : "",
                    "url": "",
                    "title": "",
                    "main_text_path" : "",
                    "main_text": "",
                    "question": "",
                    "score": ""}]
                    
            preprocessed += dummy*(3-len(preprocessed))          

    all_data = {"result" : preprocessed}

    return documents, thoughts_process, paa, all_data, query, preprocessed, answer

def answer_solution_flag(trans_instance, query, answer_dict):
    logger = setup_logger(query['chatid'],query['chat_session_id'], query['rag_order'])
    logger.info("###### Function Name : answer_solution_flag")
    ## solution이 "1"이 아닌 경우에, paa로 사용되는 "additional_questions" 모두 공란으로 지정
    if answer_dict["solution"] != "Yes":
        answer_dict["additional_questions"] = ""

        korea_timezone = pytz.timezone('Asia/Seoul')
        korea_time = datetime.now(korea_timezone)
        current_time = korea_time.strftime('%Y-%m-%d %H:%M:%S')

        preprocessed = []
        if len(preprocessed) < 3 :
            dummy = [{"timestamp" : current_time,
                    "id": str(uuid.uuid4()),
                    "type": "",
                    "iso_cd": "",
                    "language": "",
                    "product_group_code": "",
                    "product_code": "",
                    "product_model_code": "",
                    "data_id" : "",
                    "mapping_key": "",
                    "chunk_num" : "",
                    "file_name": "",
                    "pages" : "",
                    "url": "",
                    "title": "",
                    "main_text_path" : "",
                    "main_text": "",
                    "question": "",
                    "score": ""}]
                    
            preprocessed += dummy*(3-len(preprocessed))          
 
    else:
        pass

    answer = answer_dict['response']
    if query['trans_language_code_score'] != 1:
        detectLang = query['user_selected_language_code']
    else :
        detectLang = query['trans_language_code']
    
    if query["trans_language_code"] != "en":
            body = [{'text': answer}]
            try :
                answer = trans_instance.translate_question(language_from="en", language_to=detectLang, body=body)
            except:
                answer = trans_instance.translate_question(language_to='en', body=body)
            
            answer = answer[0]['translations'][0]['text']
    else:
            pass
    
    paa = answer_dict["additional_questions"]

    return answer, paa, preprocessed

def index_to_dict(answer_str:str, start_key:str, end_key:str):
    logger.info("###### Function Name : index_to_dict")
    idx_response_start = answer_str.find('"'+start_key+'"')
    idx_response_end = answer_str.find('"'+end_key+'"')

    if idx_response_start != -1 and idx_response_end != -1:
        response_prefix = answer_str[idx_response_start:idx_response_end].strip()
        try:
            json_response = eval("{"+response_prefix+"}")
        except:
            json_response = {start_key:""}

    else:
        json_response = {start_key:""}

    return json_response

def make_error_result(error) :
    logger.info("###### Function Name : make_error_result")
    output_json= {
                "gpt" : f"{error}",
                "eventCd" : "",
                "detectLanguage" : "",
                "userSelectLanguage" : "",
                "indexName" : "",
                "contentKeywords" : [""],
                "contentIds":["","",""],
                "youtubeIds":["","",""],
                "itemIds":["","",""],
                "additionalInfo" : {
                    "intent": "",
                    "refDocName" :["","",""],
                    "refDocUrl": ["","",""],
                    "etcLinkName": ["","",""],
                    "etcLinkUrl": ["","",""],
                    "refDocScore": ["","",""],
                    "call1PromptToken": "",
                    "call1CompletionToken": "", 
                    "call2PromptToken": "",  
                    "call2CompletionToken": "",
                    "time": {
                        "Refinement": "",
                        "ir": "",
                        "answer": "",
                        "total": ""
                    },
                    "contexts": ["","",""],
                    "paa" : ["","",""],
                    "refinedQuery": "",
                    "thoughts" : ""
                }
            }
    
    output = json.dumps(output_json,ensure_ascii=False)
    return output

def upload_chat_summary(query, results) :
    logger = setup_logger(query['chatid'],query['chat_session_id'], query['rag_order'])
    logger.info("###### Function Name : upload_chat_summary")
    now = datetime.now()
    formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")
    sql_instance = MySql()

    if 'total_result' in results:
        context = "".join(results['total_result']['additionalInfo']['contexts'])
        score = results['total_result']['additionalInfo']['refDocScore']
        lists = {
        'contents': results['total_result']['contentIds'],
        'manual': results['total_result']['itemIds'],
        'youtube': results['total_result']['youtubeIds']
        }

        keys = sorted(lists.keys())
        values = [lists[key] for key in keys]
        transposed_lists = list(zip(*values))
        ref_doc = []
        for sublist in transposed_lists:
            for index, item in enumerate(sublist):
                if item != "":
                    ref_doc.append((keys[index], item))
                    break
            else:
                ref_doc.append(("", ""))
    
    else :
        context = "".join(results['result']['additionalInfo']['contexts'])
        score = ["","",""]
        ref_doc = [("",""),("",""),("","")]


    REFINE_KEYWORDS = extract_data(results['result']['additionalInfo']['refinedQuery'], start='keywords:', end='symptom')[0]
    REFINE_SYMPTOM = extract_data(results['result']['additionalInfo']['refinedQuery'], start='symptom:')[0]

    if REFINE_KEYWORDS == 'None':
        REFINE_KEYWORDS = ""
    
    if REFINE_SYMPTOM == 'None':
        REFINE_SYMPTOM = ""
    
    try :
        REFINE_OUTPUT_QUESTION_1 = extract_data(results['result']['additionalInfo']['refinedQuery'], start='additional_questions:', end='keywords')[0].split('\n')[0]
        REFINE_OUTPUT_QUESTION_2 = extract_data(results['result']['additionalInfo']['refinedQuery'], start='additional_questions:', end='keywords')[0].split('\n')[1]
        REFINE_OUTPUT_QUESTION_3 = extract_data(results['result']['additionalInfo']['refinedQuery'], start='additional_questions:', end='keywords')[0].split('\n')[2]
    
    except :
        REFINE_OUTPUT_QUESTION_1 = ""
        REFINE_OUTPUT_QUESTION_2 = ""
        REFINE_OUTPUT_QUESTION_3 = ""

    parameters = (results['chatid'], 
                int(results['chat_session_id']), 
                query['iso_cd'].upper(), 
                query['language_code'].lower(), 
                int(results['rag_order']),
                query['product_code'],
                results['result']['additionalInfo']['originalQuery'], 
                remove_emojis(results['result']['gpt']), 
                results['result']['additionalInfo']['originalQuery'],
                remove_emojis(results['result']['gpt']), 
                query['trans_language_code'],
                query['user_selected_language_code'],
                query['index_name'],
                int(results['result']['additionalInfo']['call1PromptToken'] + results['result']['additionalInfo']['call1CompletionToken']),
                int(results['result']['additionalInfo']['call2PromptToken'] + results['result']['additionalInfo']['call2CompletionToken']),
                float(sum(results['result']['additionalInfo']['time'].values())),
                results['result']['additionalInfo']['refinedQuery'],
                results['result']['additionalInfo']['intent'],
                REFINE_KEYWORDS,
                REFINE_SYMPTOM,
                REFINE_OUTPUT_QUESTION_1,
                REFINE_OUTPUT_QUESTION_2,
                REFINE_OUTPUT_QUESTION_3,
                remove_emojis(context),
                ref_doc[0][0], ref_doc[0][1], float(score[0]) if score[0] != "" else None,
                ref_doc[1][0], ref_doc[1][1], float(score[1]) if score[1] != "" else None,
                ref_doc[2][0], ref_doc[2][1], float(score[2]) if score[2] != "" else None,
                formatted_now, "HJS", None, None, None, "N")

    logger.info(f"############## Parameters : {parameters}")
    sql_insert_query = """INSERT INTO tmp_chat_summary_dash (CHAT_ID, CHAT_SESSION_ID, COUNTRY_CD, LANGUAGE_CD, RAG_ORDER, PROD_CD, USER ,
                                                        ASSISTANT, QUERY_ORI, GPT_ANSWER, DETECTLANGUAGE, USERSELECTLANGUAGE,
                                                        INDEXNAME, REFINEMENT_TOKEN, QA_TOKEN, RAG_TOTAL_TIME,REFINE_INPUT_QUESTION,
                                                        REFINE_INTENT, REFINE_KEYWORDS, REFINE_SYMPTOM, REFINE_OUTPUT_QUESTION_1,
                                                        REFINE_OUTPUT_QUESTION_2, REFINE_OUTPUT_QUESTION_3, CONTEXTS,
                                                        REF_DOC_1_TYPE, REF_DOC_1_ID, REF_DOC_1_SCORE,
                                                        REF_DOC_2_TYPE, REF_DOC_2_ID, REF_DOC_2_SCORE,
                                                        REF_DOC_3_TYPE, REF_DOC_3_ID, REF_DOC_3_SCORE,
                                                        CREATE_DATE, CREATE_USER, UPDATE_DATE, UPDATE_USER, GPT_EVAL, EVAL_YN)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    sql_instance.insert_data(sql_insert_query, parameters)

    return logger.info("chat summary upload complete")

def get_ErrorCode(Code_or_List:str, GROUP_CD:str, PROD_CD:str, ErrorCode:str="None"):
    ## 제품군/제품 코드 별 에러코드 리스트 불러오기
    logger.info("###### Function Name : get_ErrorCode")

    ## http://service.academy.lge.com/#/Troubleshooting 참고
    if GROUP_CD == "REF":

        list_ErrorCode = [
            "C1", "CF", "CH", "CL", "CO", "dH", "dS", "FF", "FS", "FU", "gF", "Hi", "IF", "Lo", "OFF", "rF", "rS",
            "Ad", "dL", "dr", "E AS", "gS", "HS", "I Ld", "I LS", "Id", "IS", "It", "IU", "L AS", "nC", "nE", "nF", "Od", "r AS", "r2", "rt", "S1", "S2", "SS", "tt", "U Ld", "U LS", "UC",
            ]

    elif GROUP_CD == "WM":
        if PROD_CD == "W/M" or PROD_CD == "WM" or PROD_CD == "DRW":
            list_ErrorCode = [
                "AE", "Cd", "CE", "CL", "dcL", "dE", "dE1", "dE2", "dE3", "dHE", "dO", "E7", "Ed1", "Ed2", "Ed3", "Ed4", "Ed5", "FE", "FF", "IE", "LE", "LE1~9", "nC", "nE", "nF", "OE", "OPn", "PE", "PF", "PS", "SE", "SEE", "tE", "u5", "UE", "VS",
                ] ## "tCL" : 20240424, 피드백에 따라 "tCL" 에러코드 제외
        elif PROD_CD == "DRR":
            list_ErrorCode = [
                "AE", "Ant", "bE", "Cd", "CE1", "CL", "dE", "dE4", "EHE", "ELE", "F1", "IE", "LE1", "LE2", "nC", "nE", "nF", "OE", "tE", "tE1", "tE2", "tE3", "tE4", 
                ]
        else:
            list_ErrorCode = []

    elif GROUP_CD == "STL":
            list_ErrorCode = [
                "AE", "bE2", "bE3", "bE4", "bE5", "bE6", "bE7", "CE2", "CE3", "CE4", "CE5", "CE6", "CE7", "CL", "dE", "dHE", "E1", "E4", "EE", "LE", "LE2", "nC", "nE", "nF", "tE1", "tE2", "tE3", "tE4", "tE5", 
                ]

    elif PROD_CD == "ELC":
            list_ErrorCode = [
                "F1", "F10", "F11", "F16", "F17", "F18", "F19", "F2", "F20", "F21", "F22", "F24", "F25", "F3", "F33", "F34", "F36", "F38", "F4", "F42", "F45", "F46", "F5", "F51", "F52", "F56", "F59", "F6", "F62", "F69", "F7", "F8", "F9", 
                ]

    elif PROD_CD == "MWO":
            list_ErrorCode = [
                "COOL", "DOOR", "E-01", "E-02", "E-03", "E-04", "E-05", "E-10", "F-01", "F-02", "F-1", "F-10", "F-11", "F-13", "F-14", "F-16", "F-17", "F-2", "F-3", "F-4", 
                ]

    elif PROD_CD == "ACL":
            list_ErrorCode = [
                "E10", "E11", "E15", "E9", "Hi", "Lo", 
                ]

    elif GROUP_CD == "ACN":
        if PROD_CD == "WRA": ## 벽걸이 에어컨 "RAC"을 지칭하는 모든 에러코드
            list_ErrorCode = [
                "CH01", "CH02", "CH06", "CH12", "CH03", "CH04", "CH09", "CH10", "CH21", "CH29", "CH22", "CH23", "CH24", "CH26", "CH27", "CH32", "CH34", "CH35", "CH36", "CH38", "CH37", "CH41", "CH44", "CH45", "CH48", "CH46", "CH42", "CH43", "CH51", "CH60", "CH61", "CH62", "CH65", "CH67", "CH72"
                ]
        else : ## 이동형 에어컨을 지칭하는 모든 에러코드 -> Else문으로 적용
            list_ErrorCode = [
                "CH01", "CH02", "CH10", "CH22", "CH26", "CH32", "CH38", "CH41", "CH45", "CH61", "CH62", "CH67", "CH75", "CL", "Co", "CP", "FL", "Po", 
            ]

    elif GROUP_CD == "VC": ## 이미지 형태로 되어 있어 미기입
            list_ErrorCode = [
                 
                ]

    # elif GROUP_CD == "": ## LG ThinQ 미기입
    #         list_ErrorCode = [
                 
    #             ]

    ## 20240411 이후
        
    else:
        list_ErrorCode = []

    ## 추출 대상 확인 : 에러코드 리스트
    if Code_or_List == "List":
        return str(list_ErrorCode)

    ## 추출 대상 확인 : 에러코드
    if Code_or_List == "Code":

        ## 에러코드가 추출되었는지 확인
        if ErrorCode == "None":
            return "None"
        else:
            pass

        ## 추출된 모든 에러코드가 불러온 에러코드 리스트에 포함되는지 여부 확인
        if len(ErrorCode.split(",")) > 1:
            ErrorCodes = ErrorCode.split(",")
        else:
            ErrorCodes = [ErrorCode]

        list_ErrorCode = [x.upper() for x in list_ErrorCode]
        ErrorCodes = [x.upper().strip() for x in ErrorCodes]
        num_of_ErrorCodes = len(ErrorCodes)

        list_exist = list()
        for idx_ErrorCodes in range(num_of_ErrorCodes):
            for item in list_ErrorCode:

                if ErrorCodes[idx_ErrorCodes] in item:
                    list_exist.append(True)
                    break
                else:
                    continue

        try:
            for idx_ErrorCodes in range(num_of_ErrorCodes):
                assert list_exist[idx_ErrorCodes] == True
            return ErrorCode
        except:
            ## 추출된 에러코드가 불러온 에러코드 리스트에 포함되어 있지 않다면 "Unknown_ErrorCode" 반환
            return "Unknown_ErrorCode"
    
def get_GroupName(GROUP_CD:str, PROD_CD:str="None"):
    logger.info("###### Function Name : get_GroupName")

    ## 제품군/제품 코드 별 제품 명 불러오기
    if GROUP_CD == "ACN":
        GROUP_N = "AirConditioner"
        PROD_N = next((item for temp_PROD_CD, item in zip(["WRA", "SRA", "OTH"],["Window", "Single-Split Wall Mounted", ""]) if temp_PROD_CD == PROD_CD), "")

    elif GROUP_CD == "AUD":
        GROUP_N = "Audio"
        PROD_N = next((item for temp_PROD_CD, item in zip([""],[""]) if temp_PROD_CD == PROD_CD), "")

    elif GROUP_CD == "COK":
        GROUP_N = "CookingAppliance"
        PROD_N = next((item for temp_PROD_CD, item in zip(["MWO", "GOR", "ELC", "OTH"],["Countertop MWO", "Gas Cooktop", "Range/Oven/Electric Cooktop", ""]) if temp_PROD_CD == PROD_CD), "")

        ## MWO를 GPT가 인식하지 못하는 이유로 추가 // 기존 값은 Product MST Table 값을 따라 그대로 유지
        if PROD_N == "Countertop MWO":
            PROD_N = "Countertop Microwave Oven"

    elif GROUP_CD == "MNT":
        GROUP_N = "Monitor"
        PROD_N = next((item for temp_PROD_CD, item in zip([""],[""]) if temp_PROD_CD == PROD_CD), "")

    elif GROUP_CD == "PC":
        GROUP_N = "Computer"
        PROD_N = next((item for temp_PROD_CD, item in zip([""],[""]) if temp_PROD_CD == PROD_CD), "")

    elif GROUP_CD == "REF":
        GROUP_N = "Refrigerator"
        PROD_N = next((item for temp_PROD_CD, item in zip(["SBS", "REF", "OTH"],["Side by Side", "Top/Bottom Mount", ""]) if temp_PROD_CD == PROD_CD), "")

    elif GROUP_CD == "STL":
        GROUP_N = "Styler"
        PROD_N = next((item for temp_PROD_CD, item in zip([""],[""]) if temp_PROD_CD == PROD_CD), "")

    elif GROUP_CD == "TV":
        GROUP_N = "TV"
        PROD_N = next((item for temp_PROD_CD, item in zip([""],[""]) if temp_PROD_CD == PROD_CD), "")

    elif GROUP_CD == "VC":
        GROUP_N = "VacuumCleaner"
        PROD_N = next((item for temp_PROD_CD, item in zip(["RBC", "CVC", "OTH"],["Robot Type", "Bagless Type", ""]) if temp_PROD_CD == PROD_CD), "")

    elif GROUP_CD == "VPJ":
        GROUP_N = "Projector"
        PROD_N = next((item for temp_PROD_CD, item in zip([""],[""]) if temp_PROD_CD == PROD_CD), "")

    elif GROUP_CD == "WM":
        GROUP_N = "WashingMachine"
        PROD_N = next((item for temp_PROD_CD, item in zip(["WM", "DRW", "DRR"],["Top Loader", "Front Loader", "Laundry Dryer"]) if temp_PROD_CD == PROD_CD), "")

        ## Dryer와 WashingMachine을 GPT가 혼동하는 이유로 추가
        if PROD_N == "Laundry Dryer":
            GROUP_N = "Laundry Dryer"

    elif GROUP_CD == "DWM":
        GROUP_N = "DWM"
        PROD_N = next((item for temp_PROD_CD, item in zip([""],[""]) if temp_PROD_CD == PROD_CD), "")

    ## GROUP_CD : OTH(Other) 인 경우 또는 지정되지 않은 제품 그룹 코드인 경우
    else:
        GROUP_N = "Appliances"
        PROD_N = next((item for temp_PROD_CD, item in zip(["VCR", "CDM", "IPD", "ACL", "OTH"],["Video/DVD", "CD-Rom", "Innovative Personal Device", "Air Cleaner", "Appliances"]) if temp_PROD_CD == PROD_CD), "")
        GROUP_NAME = f"{GROUP_N} - {PROD_N}"
        return GROUP_NAME

    ## GROUP_NAME을 다른 곳에서 활용하기위해 "제품군 명"만 추출
    GROUP_NAME = f"{GROUP_N}"
    return GROUP_NAME

def remove_duplicates(lst):
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

def process_locale(locale):
    parts = locale.split('_')
    if len(parts) > 1 and parts[0] == parts[1]:
        return parts[0]
    else:
        return locale
    
def preprocessing_refinement(answer) :
    logger.info("###### Function Name : preprocessing_refinement")

    ## GPT의 JSON Format 출력 결과 후처리
    answer_str_1 = answer.replace("```json", "").replace("```", "").strip()
    logger.info(f"############## refinement_answer_str_1 :\n{answer_str_1}")
    
    ## 출력 결과 1차 확인 및 answer_dict로 재구성
    try:
        answer_dict = eval(answer_str_1)
        
    ## JSONDecode ERROR : 출력 결과 2차 확인 및 answer_dict로 재구성
    except json.decoder.JSONDecodeError as e:

        logger.info(f"############## refinement_answer_str_1 Error : {type(e).__name__} - {e}")

        answer_str_2 = answer.replace("```json", "").replace("```", "").replace(r'{','').replace(r'}','').strip()
        logger.info(f"############## refinement_answer_str_2 :\n{answer_str_2}")

        ## 점수
        device_score = float(extract_data(answer_str_2, start="device_score", end= "intention_score")[0].replace('None','0.0').replace('\"','').replace(':','').replace('\n','').replace(',','').strip())
        intention_score = float(extract_data(answer_str_2, start="intention_score", end= "refinement")[0].replace('None','0.0').replace('\"','').replace(':','').replace('\n','').replace(',','').strip())

        ## Refine Query
        refined_question = extract_data(answer_str_2, start="refined_question", end= "additional_sentences")[0].replace('\"','').replace(':','').replace('\n','').replace(',','').strip()
        temp_additional_sentences = extract_data(answer_str_2, start="additional_sentences", end= "keywords")[0].replace('[','').replace(']','').replace('\"','').replace(':','').replace('    ','').split(',\n')
        additional_sentences = [item.replace('\n','').replace('\t','').strip() for item in temp_additional_sentences if item.strip()]
        keywords = extract_data(answer_str_2, start="keywords", end= "symptom")[0].replace('\"','').replace(':','').replace('\n','').replace(',','').strip()
        symptom = extract_data(answer_str_2, start="symptom", end= "Model_Number")[0].replace('\"','').replace(':','').replace('\n','').replace(',','').strip()

        ## Refined Filter
        Model_Number = extract_data(answer_str_2, start="Model_Number", end= "Error_Code")[0].replace('\"','').replace(':','').replace('\n','').replace(',','').strip()
        Error_Code = extract_data(answer_str_2, start="Error_Code")[0].replace('\"','').replace(':','').replace('\n','').replace(',','').strip()

        ## answer_dict 구성
        answer_dict = {
            "response_language":"",
            "evaluation":{
                "device_score": device_score,
                "intention_score": intention_score
            },
            "refinement":{
                "refined_question": refined_question,
                "additional_sentences": additional_sentences,
                "keywords": keywords,
                "symptom": symptom,
                "Model_Number": Model_Number,
                "Error_Code": Error_Code
            }
        }

    except Exception as e :
        ## answer_dict 구성
        answer_dict = {
            "response_language":"",
            "evaluation":{
                "device_score": "0.0",
                "intention_score": "0.0"
            },
            "refinement":"STOP"
        }
    
    return answer_dict

def filter_logs_by_chat_id(logs, chat_id, chat_session_id, rag_order):
    filter_chat = str(chat_id)+'_'+str(chat_session_id)+'_'+str(rag_order)
    return [log for log in logs if filter_chat in log]

def encrypt(key, plaintext):
    # 패딩
    padder = padding.PKCS7(algorithms.AES.block_size).padder()
    padded_plaintext = padder.update(plaintext) + padder.finalize()
 
    # 암호화
    iv = os.urandom(16)  # Initialization Vector 생성
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(padded_plaintext) + encryptor.finalize()
 
    # IV와 암호문을 함께 반환
    return iv + ciphertext
 
def decrypt(key, ciphertext):
    # IV 추출
    iv = ciphertext[:16]
    ciphertext = ciphertext[16:]
 
    # 복호화
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    decrypted_data = decryptor.update(ciphertext) + decryptor.finalize()
 
    # 언패딩
    unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
    unpadded_data = unpadder.update(decrypted_data) + unpadder.finalize()
 
    return unpadded_data

def Authorization(value, key) :
    encrypted_data = b64decode(value)
    byte_key = bytes.fromhex(key)
    decrypted_data = decrypt(byte_key, encrypted_data)
    decrypted_data = decrypted_data.decode()

    return decrypted_data
