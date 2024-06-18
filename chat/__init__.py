import azure.functions as func
import time
from datetime import datetime, timezone
import pycountry
import json
import credential
import pandas as pd
import pytz
import os
import traceback

def main(req: func.HttpRequest) -> func.HttpResponse:
        
        if not credential.keyvault_values:
                credential.fetch_keyvault_values()

        from ai_search import AISearch
        from model import OpenAI, Translator
        from database import CosmosDB, MySql, AzureStorage
        from tools import ( preprocess_thoughts_process, 
                            load_chat_data, 
                            preprocess_answer, 
                            preprocess_output_data,
                            preprocess_documents,
                            preprocess_intent_search,
                            preprocess_query,
                            refinement_flag,
                            answer_solution_flag,
                            make_error_result,
                            filter_logs_by_chat_id,
                            OpenAIResourceError,
                            AuthorizationError)
        
        from tools import Authorization
        from credential import keyvault_values
        from log import setup_logger, thread_local

        auth_header = req.headers.get('Authorization')
        auth = Authorization(value=keyvault_values['Authorization'], key=auth_header)

        # Get query
        data = req.get_json()
        query = preprocess_query(data)
        logger = setup_logger(query['chatid'],query['chat_session_id'], query['rag_order'])

        logger.info("=" * 30 + " POD_NAME " + "=" * 30)
        # logger.info(dict(os.environ)['HOSTNAME'])
        # query['pod_name'] = dict(os.environ)['HOSTNAME']
        query['pod_name'] = 'DEV'

        if query['iso_cd'] == 'GB' :
                query['iso_cd'] = 'UK'
        
        logger.info(f"Get KeyVault Values : {json.dumps(credential.keyvault_values,indent=4,ensure_ascii=False)}")
        logger.info("=" * 30 + " Start chat API " + "=" * 30)
        # Create instance
        gpt_model='gpt-35-turbo'
        openai_instance = OpenAI(gpt_model=gpt_model)
        db_instance = CosmosDB()
        as_instance_log = AzureStorage(container_name='logs', storage_type='datast')
        as_instance_log_file_share =  AzureStorage(container_name="lge-python-back",storage_type='log')
        as_instance_raw = AzureStorage(container_name='raw-data', storage_type='datast')

        search_instance = AISearch()
        trans_instance = Translator()
        
        logger.info("=" * 30 + " Load query " + "=" * 30)

        logger.info(f"first query : {json.dumps(query,indent=4,ensure_ascii=False)}")
        start_time = time.time()
        
        sql_instance = MySql()
        locale_mst_table_1 = sql_instance.get_table("""
                                                SELECT CODE_CD, CODE_NAME
                                                FROM tb_code_mst
                                                WHERE GROUP_CD = 'B00003'
                                                """)
        locale_mst_table_1 = pd.DataFrame(locale_mst_table_1)
        locale_mst_table_1 = locale_mst_table_1.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        sql_instance = MySql()
        locale_mst_table_2 = sql_instance.get_table("""
                                                SELECT CORP_CD, LOCALE_CD, LANGUAGE_CD
                                                FROM tb_corp_lan_map
                                                """)
        locale_mst_table_2 = pd.DataFrame(locale_mst_table_2)
        locale_mst_table_1.columns = ['LANGUAGE_CD', 'CODE_NAME']
        LOCALE_MST = pd.merge(locale_mst_table_1,locale_mst_table_2, left_on='LANGUAGE_CD', right_on='LANGUAGE_CD')

        sql_instance = MySql()
        LANGUAGE_MST = sql_instance.get_table("""
                                                select
                                                CODE_CD, CODE_NAME
                                                from
                                                tb_code_mst
                                                where
                                                group_cd = "B00006"
                                                """)
        LANGUAGE_MST = pd.DataFrame(LANGUAGE_MST)
        LANGUAGE_MST = LANGUAGE_MST.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        logger.info("=" * 30 + " 1st Query Preprocessing " + "=" * 30)
        # Find Index Name
        query['user_selected_language_code'] = query['language_code']
        query['user_selected_language'] = LOCALE_MST[LOCALE_MST['LANGUAGE_CD']==query['user_selected_language_code']]['CODE_NAME'].values[0]
        
        query['corp_cd'] = LOCALE_MST[LOCALE_MST['LOCALE_CD']==query['locale_cd']]['CORP_CD'].values[0]
        query['language_code'] = LOCALE_MST[LOCALE_MST['LOCALE_CD']==query['locale_cd']]['LANGUAGE_CD'].values[0]
        query['language'] = LOCALE_MST[LOCALE_MST['LOCALE_CD']==query['locale_cd']]['CODE_NAME'].values[0]
 
        try :   

                if auth != "DEV" :
                        logger.error(f"####### Authorizaion Error : {auth} != DEV")
                        raise AuthorizationError(f"####### Authorizaion Error : {auth} != DEV")

                if query['current_user'] > 1180 :
                        logger.error(f"####### Current User : {query['current_user']}")
                        raise OpenAIResourceError(f"####### Current User : {query['current_user']}")
                # Detect language
                #query['index_name'] = query['iso_cd'].lower()+'-'+query['language_code'].lower()
                query['index_name'] = query['locale_cd'].split('_')[0].lower()+'-'+query['language_code'].lower()
                body = [{'text': query['question']}]

                try:
                        detect_output = trans_instance.detect_language(body=body)
                        query['trans_language_code'] = detect_output[0]['language'].split('-')[0]
                        query['trans_language_code_score'] = detect_output[0]['score']
                        language = LANGUAGE_MST[LANGUAGE_MST['CODE_CD'] == query['trans_language_code']]['CODE_NAME'].iloc[0]
                        query['trans_language'] = language
                except :
                        query['trans_language_code'] = query['user_selected_language_code']
                        query['trans_language_code_score'] = 1.0
                        query['trans_language'] = query['user_selected_language']
                
                # Translate Query to Locale cd
                try :   
                        trans_question = trans_instance.translate_question(body=body,language_from= query['trans_language_code'], language_to=query['language_code'])
                        query['trans_question'] = trans_question[0]['translations'][0]['text']
                except :
                        query['trans_question'] = query['question']
 
                logger.info(f"1st query : {json.dumps(query,indent=4,ensure_ascii=False)}")
                logger.info("=" * 30 + " Start General Inquiry Search" + "=" * 30)
                product_mst = as_instance_raw.read_file('Contents_Manual_List_Mst_Data/PRODUCT_MST.csv')

                if query['product_code'] == 'W/M' :
                        query['product_code'] = 'WM'

                if len(query['product_group_code']) == 0:
                        query['product_group_code'] = product_mst[product_mst['DISP_PROD_CD'] == query['product_code']][['PROD_GROUP_CD']].iloc[0].values[0]

                if len(query['product_code']) == 0:
                        filter_df = product_mst[product_mst['DISP_GROUP_CD'] == query['product_group_code']][['PROD_GROUP_CD','PROD_CD']]
                else :
                        filter_df = product_mst[(product_mst['DISP_GROUP_CD'] == query['product_group_code'])&(product_mst['DISP_PROD_CD'] == query['product_code'])][['PROD_GROUP_CD','PROD_CD']]
                        if len(filter_df) == 0 :
                                filter_df = product_mst[product_mst['DISP_GROUP_CD'] == query['product_group_code']][['PROD_GROUP_CD','PROD_CD']]
                                logger.error(f"########## FILTER ERROR ##########\nPROD_GROUP_CD : {query['product_group_code']}\nPROD_CD : {query['product_code']}")
                                korea_timezone = pytz.timezone('Asia/Seoul')
                                now = datetime.now(korea_timezone)
                                formatted_date = now.strftime('%Y%m%d')
                                filter_logs_chat_id = filter_logs_by_chat_id(thread_local.log_list, query['chatid'],query['chat_session_id'],rag_order=query['rag_order'])
                                logs_str = "\n".join(filter_logs_chat_id)
                                log_file_path = f"chatapi/error_log/{formatted_date}/{query['iso_cd']}/{query['language']}/FILTER_ERROR_{query['chatid']}_{query['chat_session_id']}.log"
                                as_instance_log.upload_log(logs_str, file_path=log_file_path)

                preprocessed, documents, all_data = preprocess_intent_search(filter_df,search_instance, query)

                use_memory = True
                chat_history = load_chat_data(db_instance, db_name='lgdb', container_name='lgcontainer', chatid=query['chatid'], chat_session_id = query['chat_session_id'])
                
                if any(item["type"] == "general-inquiry" for item in preprocessed):
                        
                        query['flag'] = "general-inquiry"
                        logger.info("=" * 30 + " General Inquiry " + "=" * 30)
                        logger.info(f" Document search results : {json.dumps(all_data,indent=4,ensure_ascii=False)}")
                        answer = preprocessed[0]['main_text']
                        
                        if query['trans_language_code_score'] != 1:
                                detectLang = query['user_selected_language_code']
                        else :
                                detectLang = query['trans_language_code']

                        body = [{'text': f"{answer}"}]

                        trans_response = trans_instance.translate_question(body=body, language_to=detectLang)
                        answer = trans_response[0]['translations'][0]['text']
                        
                        EVENT_CODE = preprocessed[0]['file_name']
                        INTENT_CODE = preprocessed[0]['pages']
                        related_link_url = preprocessed[0]['main_text']
                        thoughts_process = ""
                        query['eventcd'] = EVENT_CODE
                        query['intent'] = INTENT_CODE
                        query['refined_query'] = ""
                        query["refined_answer"] = ""
                        paa = ""
                        prompt = ""
                        end_time = time.time()
                        IR_time = end_time - start_time
                        refinement_time = 0
                        start_time = time.time()

                else :
                        
                        logger.info("=" * 30 + " Start Refinement " + "=" * 30)
                        # Refinement
                        refined_answer, refined_info, refined_query, refined_flag, model_code, prompt = openai_instance.generate_answer(logger=logger, query=query, use_memory=use_memory, chat_history=chat_history, refinement=True)

                        if len(query['product_model_code']) != 0:
                                logger.info("=" * 30 + " product model code detected " + "=" * 30)
                                logger.info(f"product_model_code : {query['product_model_code']}")
                                pass
                        else :
                                if model_code == 'None' :
                                        logger.info("=" * 30 + " No product model code detected " + "=" * 30)
                                        query['product_model_code'] = ""
                                else :  
                                        logger.info("=" * 30 + " product model code detected " + "=" * 30)
                                        logger.info(f"product_model_code : {model_code}")
                                        query['product_model_code'] = model_code

                        query["refined_query"] = refined_query
                        query["refined_answer"] = refined_answer
                        end_time = time.time()
                        refinement_time = end_time - start_time
                        logger.info("=" * 30 + " End Refinement " + "=" * 30)
                        logger.info(f"2nd query : {json.dumps(query,indent=4,ensure_ascii=False)}")
                        
                        if "FIX" in refined_flag:
                                start_time = time.time()
                                # query['flag'] = "FIX"
                                query['flag'] = refined_flag
                                logger.info("=" * 30 + " Create fixed answer " + "=" * 30)
                                documents, thoughts_process, paa, all_data, query, preprocessed, answer = refinement_flag(trans_instance, query)
                                query['intent'] = "FIX"
                                IR_time = 0

                        else :  
                                
                                logger.info("=" * 30 + " Start AI Search " + "=" * 30)
                                # Search related documents
                                start_time = time.time()
                                product_mst = as_instance_raw.read_file('Contents_Manual_List_Mst_Data/PRODUCT_MST.csv')

                                if query['product_code'] == 'W/M' :
                                        query['product_code'] = 'WM'

                                if query['product_group_code'] == 'REF' or query['product_group_code'] == 'TV' or query['product_group_code'] == 'WM':
                                        query['product_code'] = ""

                                if len(query['product_code']) == 0:
                                        filter_df = product_mst[product_mst['DISP_GROUP_CD'] == query['product_group_code']][['PROD_GROUP_CD','PROD_CD']]
                                else :
                                        filter_df = product_mst[(product_mst['DISP_GROUP_CD'] == query['product_group_code'])&(product_mst['DISP_PROD_CD'] == query['product_code'])][['PROD_GROUP_CD','PROD_CD']]
                                        if len(filter_df) == 0 :
                                                filter_df = product_mst[product_mst['DISP_GROUP_CD'] == query['product_group_code']][['PROD_GROUP_CD','PROD_CD']]

                                preprocessed, documents, all_data = preprocess_documents(filter_df,search_instance, query)
                                end_time = time.time()
                                IR_time = end_time - start_time

                                start_time = time.time()
                                        
                                logger.info(f"Document search results : {json.dumps(all_data,indent=4,ensure_ascii=False)}")
                                answer_dict, thoughts_process = openai_instance.generate_answer(logger=logger, query=query, use_memory=use_memory, chat_history=chat_history, refinement=False, documents=documents)
                                
                                logger.info("=" * 30 + " Start Generating Answers " + "=" * 30)
                                
                                if answer_dict["solution"] != "Yes":
                                        answer, paa, preprocessed = answer_solution_flag(trans_instance, query, answer_dict)
                                        all_data['result'] = preprocessed
                                        query['intent'] = "FIX"

                                else:
                                        answer, paa = answer_dict["response"], answer_dict["additional_questions"]
                                        answer = preprocess_answer(trans_instance, query, answer, preprocessed, refined_info)
                                        query['intent'] = "RAG"
                                
                                logger.info(f"Answer : {answer}")
                                #answer = answer_dict['response']
                                #paa = answer_dict["additional_questions"]
                                #answer = preprocess_answer(answer, preprocessed)
                                query['flag'] = "RAG"
                                query['eventcd'] = "RAG"

                # Prompt replace (for display)
                thoughts_process = preprocess_thoughts_process(thoughts_process, documents, query, chat_history, answer)
                end_time = time.time()
                answer_time = end_time - start_time
                time_info = {"IR" : IR_time,
                        "Refinement" : refinement_time,
                        "answer" : answer_time,
                        "total" : IR_time+refinement_time+answer_time}
                
                logger.info("=" * 30 + " Make Final Output " + "=" * 30)
                # Create the final output
                output = preprocess_output_data(trans_instance, preprocessed, query, thoughts_process, answer, paa, time_info, prompt, gpt_model, all_data)

                logger.info(f"Final Output : {output}")
                korea_timezone = pytz.timezone('Asia/Seoul')
                now = datetime.now(korea_timezone)
                formatted_date = now.strftime('%Y%m%d')

                filter_logs_chat_id = filter_logs_by_chat_id(logs=thread_local.log_list, chat_id=query['chatid'], 
                                                             chat_session_id=query['chat_session_id'], rag_order=query['rag_order'])
                
                logs_str = "\n".join(filter_logs_chat_id)
                #logs_str = "\n".join(thread_local.log_list)
                log_file_path = f"chatapi/log/{formatted_date}/{query['iso_cd']}/{query['language']}/{query['chatid']}_{query['chat_session_id']}.log"
                as_instance_log.upload_log(logs_str, file_path=log_file_path)

                # current_utc_time = datetime.now(timezone.utc)
                # formatted_date_utc = current_utc_time.strftime("%Y-%m/%Y-%m-%d")
                # formatted_filename = current_utc_time.strftime("%Y-%m-%d_%Hh")
                # directory_name = 'chatbot-python-back/logs/'+ formatted_date_utc
                # file_name = f"{formatted_filename}_{query['pod_name']}.log"
                # as_instance_log_file_share.upload_log_file_share(log_data=logs_str, directory_name=directory_name, file_name=file_name)
                
        except OpenAIResourceError as e:

                logger.error(str(e))
                logger.error(traceback.format_exc())
                error_contents = f"{type(e).__name__} : '{e}'"
                logger.error(error_contents)
                
                try :
                        # Detect language
                        body = [{'text': query['question']}]
                        detect_output = trans_instance.detect_language(body=body)
                        query['trans_language_code'] = detect_output[0]['language']
                        query['trans_language_code_score'] = detect_output[0]['score']
                        if query['trans_language_code_score'] != 1:
                                detectLang = query['user_selected_language_code']
                        else :
                                detectLang = query['trans_language_code']
                        
                        body = [{'text':
"""Due to high user requests, we are currently experiencing delays in our chatbot response times.
 
Please attempt your request again shortly."""}]
                
                        trans_error_answer_response = trans_instance.translate_question(body=body,language_from='en', language_to=detectLang)
                        trans_error_answer = trans_error_answer_response[0]['translations'][0]['text']
                        trans_error_answer = trans_error_answer.replace("\n","<br>")
                except :
                        trans_error_answer = "Apologies for the inconvenience. Could you please try again later?"

                output = make_error_result(trans_error_answer)
                logger.info(f"Final Output : {output}")
                korea_timezone = pytz.timezone('Asia/Seoul')
                now = datetime.now(korea_timezone)
                formatted_date = now.strftime('%Y%m%d')
                filter_logs_chat_id = filter_logs_by_chat_id(thread_local.log_list, query['chatid'], query['chat_session_id'],rag_order=query['rag_order'])
                logs_str = "\n".join(filter_logs_chat_id)
                log_file_path = f"chatapi/error_log/{formatted_date}/{query['iso_cd']}/{query['language']}/{query['chatid']}_{query['chat_session_id']}.log"
                as_instance_log.upload_log(logs_str, file_path=log_file_path)

                # current_utc_time = datetime.now(timezone.utc)
                # formatted_date_utc = current_utc_time.strftime("%Y-%m/%Y-%m-%d")
                # formatted_filename = current_utc_time.strftime("%Y-%m-%d_%Hh")
                # directory_name = 'chatbot-python-back/logs/'+ formatted_date_utc
                # file_name = f"{formatted_filename}_{query['pod_name']}.log"
                # as_instance_log_file_share.upload_log_file_share(log_data=logs_str, directory_name=directory_name, file_name=file_name)

                

        except Exception as e :
                logger.error(str(e))
                logger.error(traceback.format_exc())
                error_contents = f"{type(e).__name__} : '{e}'"
                logger.error(error_contents)
                
                try:
                        body = [{'text': query['question']}]
                        detect_output = trans_instance.detect_language(body=body)
                        query['trans_language_code'] = detect_output[0]['language'].split('-')[0]
                        query['trans_language_code_score'] = detect_output[0]['score']
                        language = LANGUAGE_MST[LANGUAGE_MST['CODE_CD'] == query['trans_language_code']]['CODE_NAME'].iloc[0]
                        query['trans_language'] = language
                except :
                        query['trans_language_code'] = query['user_selected_language_code']
                        query['trans_language_code_score'] = 1.0
                        query['trans_language'] = query['user_selected_language']
                try :

                        if query['trans_language_code_score'] != 1:
                                detectLang = query['user_selected_language_code']
                        else :
                                detectLang = query['trans_language_code']
                        
                        body = [{'text': "Apologies for the inconvenience. Could you please try again later?"}]
                
                        trans_error_answer_response = trans_instance.translate_question(body=body,language_from='en', language_to=detectLang)
                        trans_error_answer = trans_error_answer_response[0]['translations'][0]['text']
                        trans_error_answer = trans_error_answer.replace("\n","<br>")
                except :
                        trans_error_answer = "Apologies for the inconvenience. Could you please try again later?"

                output = make_error_result(trans_error_answer)
                logger.info(f"Final Output : {output}")
                korea_timezone = pytz.timezone('Asia/Seoul')
                now = datetime.now(korea_timezone)
                formatted_date = now.strftime('%Y%m%d')
                filter_logs_chat_id = filter_logs_by_chat_id(thread_local.log_list, query['chatid'],query['chat_session_id'],rag_order=query['rag_order'])
                logs_str = "\n".join(filter_logs_chat_id)
                log_file_path = f"chatapi/error_log/{formatted_date}/{query['iso_cd']}/{query['language']}/{query['chatid']}_{query['chat_session_id']}.log"
                as_instance_log.upload_log(logs_str, file_path=log_file_path)

                # current_utc_time = datetime.now(timezone.utc)
                # formatted_date_utc = current_utc_time.strftime("%Y-%m/%Y-%m-%d")
                # formatted_filename = current_utc_time.strftime("%Y-%m-%d_%Hh")
                # directory_name = 'chatbot-python-back/logs/'+ formatted_date_utc
                # file_name = f"{formatted_filename}_{query['pod_name']}.log"
                # as_instance_log_file_share.upload_log_file_share(log_data=logs_str, directory_name=directory_name, file_name=file_name)
        
        finally:
                logger.info("=" * 30 + " End chat API " + "=" * 30)
                return func.HttpResponse(body=output,
                                mimetype="application/json",
                                status_code=200)
        





