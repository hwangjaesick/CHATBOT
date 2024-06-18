import yaml
import uuid
import requests
import json
from database import MySql, AzureStorage
from openai import AzureOpenAI
from langchain_openai import AzureChatOpenAI
from langchain.prompts.chat import (
    ChatPromptTemplate,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate,
    )
from langchain.chains import LLMChain
from tools import get_prompt, preprocessing_answer, preprocessing_refinement, load_balancing, get_ErrorCode, get_GroupName
from retry import retry
import logging
import tiktoken
from credential import keyvault_values
from log import setup_logger
logger = logging.getLogger(__name__)

class OpenAI:
    def __init__(self, gpt_model = None):
        # config 파일 설정
        self.api_key = keyvault_values["openai-api-key"]
        self.api_base = keyvault_values["openai-api-base"]
        self.api_version = keyvault_values["openai-api-version"]

        # client 초기화
        self.openai_client = AzureOpenAI(api_key = self.api_key,  
                                        api_version = self.api_version,
                                        azure_endpoint = self.api_base)
        
        if gpt_model is None:
            gpt_model = "gpt-35-turbo"
        
        self.llm_client = AzureChatOpenAI( deployment_name=gpt_model,
                                            azure_endpoint=self.api_base,
                                            openai_api_key=self.api_key,
                                            openai_api_version=self.api_version,
                                            temperature=0)
        
    @retry(tries=6, delay=10, backoff=1)
    def generate_evaluation(self, row, messages):
        logger.info("###### Function Name : generate_evaluation")

        response = self.openai_client.chat.completions.create(
            model="gpt-35-turbo",
            messages=messages,
            temperature=0,
            seed=0,
            max_tokens=800)
        results = response.choices[0].message.content
        return results

    
    @retry(tries=6, delay=10, backoff=1)
    def generate_embeddings(self, text, model=None):
        logger.info("###### Function Name : generate_embeddings")
        if model is None:
            model = "text-embedding-ada-002"
        return self.openai_client.embeddings.create(input=[text], model=model).data[0].embedding

    def generate_embeddings_chat(self, text, model=None):
        logger.info("###### Function Name : generate_embeddings_chat")

        tokenizer = tiktoken.get_encoding("cl100k_base")
        tokenizer = tiktoken.encoding_for_model("text-embedding-ada-002")
        tokenSize = len(tokenizer.encode(text))

        logger.info("=" * 30 + " Load Balancing " + "=" * 30)
        apiBase, apiKey, apiVersion, apiModel = load_balancing(tokenSize, type ="embedding")
        logger.info(f"{apiBase}, {apiVersion}, {apiModel}")

        if apiBase is None or apiKey is None or apiVersion is None or apiModel is None:
            openai_client = self.openai_client
            if model is None:
                model = "text-embedding-ada-002"
            return openai_client.embeddings.create(input=[text], model=model).data[0].embedding
        
        else:
            openai_client = AzureOpenAI(api_key = apiKey,  
                                    api_version = apiVersion,
                                    azure_endpoint = f"https://{apiBase.split('/')[2]}",)
            if model is None:
                model = apiModel
            return openai_client.embeddings.create(input=[text], model=model).data[0].embedding
    
    @retry(tries=3, delay=1, backoff=1)
    def generate_answer(self, logger, query, use_memory, chat_history, refinement, documents=None):
        logger = setup_logger(query['chatid'],query['chat_session_id'], query['rag_order'])
        logger.info("###### Function Name : generate_answer")
        
        if refinement :
            system_msg_prompt = get_prompt("./config/prompt/refinement_system.txt")
            human_msg_prompt = get_prompt("./config/prompt/refinement_human.txt")
        
        else :
            system_msg_prompt = get_prompt("./config/prompt/system.txt")
            human_msg_prompt = get_prompt("./config/prompt/human.txt")
        
        system_message_prompt = SystemMessagePromptTemplate.from_template(system_msg_prompt)
        human_message_prompt = HumanMessagePromptTemplate.from_template(human_msg_prompt)

        tokenizer = tiktoken.get_encoding("cl100k_base")
        tokenizer = tiktoken.encoding_for_model('gpt-3.5-turbo')
        tokenSize = len(tokenizer.encode(system_msg_prompt + human_msg_prompt + query["trans_question"]))

        limit = 16385 - len(tokenizer.encode(system_msg_prompt)) - len(tokenizer.encode(human_msg_prompt)) - len(tokenizer.encode(query["trans_question"])) - 300
        
        logger.info(f"############# MAX_DOCUMENTS_TOKEN : {limit}") 

        if documents is not None :
            document_tokens = tokenizer.encode(documents)
            tokenSize = len(tokenizer.encode(system_msg_prompt + human_msg_prompt + query["trans_question"] + documents))
            if tokenSize > 16385 :
                document_tokens = document_tokens[:limit]
                documents = tokenizer.decode(document_tokens)
                tokenSize = len(tokenizer.encode(system_msg_prompt + human_msg_prompt + query["trans_question"] + documents))
        else :
            tokenSize = len(tokenizer.encode(system_msg_prompt + human_msg_prompt + query["trans_question"]))
        
        logger.info(f"############# TOKEN_SIZE : {tokenSize}")
        logger.info("=" * 30 + " Load Balancing " + "=" * 30)
        apiBase, apiKey, apiVersion, apiModel = load_balancing(tokenSize, type ="gpt")
        logger.info(f"{apiBase}, {apiVersion}, {apiModel}")
        
        if apiBase is None or apiKey is None or apiVersion is None or apiModel is None:
            llm_client = self.llm_client
        
        else:
            llm_client = AzureChatOpenAI( deployment_name=apiModel,
                                azure_endpoint=f"https://{apiBase.split('/')[2]}",
                                openai_api_key=apiKey,
                                openai_api_version=apiVersion,
                                temperature=0)
            
        # model_kwargs={
        #     "seed": 12345,
        #     "response_format": {"type": "json_object"}
        #     }
        
        chain_kwargs = {
            "llm": llm_client,
            "verbose": False,
        }
        
        chat_prompt = ChatPromptTemplate.from_messages([system_message_prompt, human_message_prompt])
        chain_kwargs["prompt"] = chat_prompt

        if query['trans_language_code_score'] != 1:
            detectLang = query['user_selected_language']
        else :
            detectLang = query['trans_language']

        product_group_code = query["product_group_code"]
        product_code = query["product_code"]

        llm_chain = LLMChain(**chain_kwargs)
        if use_memory :
            if refinement :

                ## 에러코드 리스트 가져오기
                error_code_list = get_ErrorCode(Code_or_List="List", GROUP_CD=product_group_code, PROD_CD=product_code)

                ## 제품군 이름 가져오기
                ## 제품군/제품 코드가 "PRODUCT_MST.csv"에 존재하지 않는 경우는 고려하지 않음
                
                product_group_name = get_GroupName(GROUP_CD=product_group_code, PROD_CD=product_code)

                logger.info("###### Refinement Answer ######")
                answer = llm_chain.predict(question=query["trans_question"], chat=chat_history, detectLang=query['language'], error_code_list=error_code_list, product_group_name=product_group_name)
                if not answer.strip():
                    raise ValueError("No answer returned")
                
                logger.info(answer)

            else:
                logger.info("###### QA Answer ######")
                answer = llm_chain.predict(question=query["trans_question"], chat=chat_history, document=documents.replace("[","(").replace("]",")"), detectLang=detectLang)
                if not answer.strip():
                    raise ValueError("No answer returned")
                
                logger.info(answer)
        else :
            answer = llm_chain.predict(question=query["trans_question"], document=documents.replace("[","(").replace("]",")"), detectLang=detectLang)
            if not answer.strip():
                raise ValueError("No answer returned")
                
            logger.info(answer)
        prompt = llm_chain.prompt.messages[0].prompt.template + llm_chain.prompt.messages[1].prompt.template

        if refinement :
            answer_dict = preprocessing_refinement(answer)

            try:
                device_score = float(answer_dict["evaluation"].get("device_score", "0.0"))
            except:
                device_score = float(0.0)

            try:
                intention_score = float(answer_dict["evaluation"].get("intention_score", "0.0"))
            except:
                intention_score = float(0.0)

            ## RefineQuery 데이터
            refined_question = answer_dict["refinement"].get("question", query["trans_question"])
            refined_additional = "\n".join(answer_dict["refinement"].get("additional_sentences", ["","",""]))
            refined_keywords = answer_dict["refinement"].get("keywords", "None")
            refined_symptom = answer_dict["refinement"].get("symptom", "None")

            ## Filtering 데이터
            model_code = answer_dict["refinement"].get("Model_Number", "None")
            error_code = answer_dict["refinement"].get("Error_Code", "None")

            ## Refinement로 검출된 mode_code와 error_code가 복수인 경우(ex : string:"code_1, code_2" or list:["code_1, code_2"])
            ## model_code : LIKE 검색을 위해 String으로 변환
            if type(model_code) == list:
                model_code = ",".join(model_code)
            else:
                pass

            ## error_code : get_ErrorCode 함수 입력을 위해 String으로 변환
            if type(error_code) == list:
                error_code = ",".join(error_code)
            else:
                pass

            ## 추출된 결과를 통합하여 "refined_query" 구성
            if error_code == "None":
                refined_query = "question: "+refined_question+"\nadditional_questions: "+refined_additional+"\nkeywords: "+refined_keywords+"\nsymptom: "+refined_symptom
            ## 검출된 에러 코드 있음
            else:
                refined_query = "question: "+refined_question+"\nadditional_questions: "+refined_additional+"\nkeywords: "+refined_keywords+"\nsymptom: "+refined_symptom+"\nretrieve_error_code: "+error_code

            logger.info(f"################ REFINED_QUERY : {refined_query}")
            ## 고정 답변 출력
            ## 질문 의도 평가 결과 후처리 
            if intention_score == 0.0 :
                refined_flag = "FIX_1"
                information = ""
            
            ## 선택한 제품과 사용자 질문에서 다루는 제품 비교 및 후처리 : 안내 문구 출력
            elif device_score == 0.0:
                refined_flag = "INFO_1"
                information = f"It seems the product {product_group_name} previously selected differs from your current query.\n\nI have done my best to answer your question, though it might not be suitable.\n========"

            ## 추출된 Error Code 후처리 : 안내 문구 출력
            elif get_ErrorCode(
                Code_or_List="Code", GROUP_CD=product_group_code, PROD_CD=product_code,
                ErrorCode=error_code
                ) == "Unknown_ErrorCode":
                refined_flag = "INFO_2"
                information = f"The Error Code [{error_code}] identified in your query could not be verified.\n\nI have done my best to answer your question, though it might not be suitable.\n========"

            else:
                refined_flag = "RAG"
                information = ""

            return answer, information, refined_query, refined_flag, model_code, prompt
        
        else :
            answer_dict = preprocessing_answer(logger, answer)
            return answer_dict, prompt
        

class Translator:
    def __init__(self):
        
        # config 파일 설정
        self.TRANSLATOR_TEXT_RESOURCE_KEY = keyvault_values["translator-account-key"]
        self.TRANSLATOR_TEXT_REGION = keyvault_values["translator-region"]
        self.TRANSLATOR_TEXT_ENDPOINT = keyvault_values["translator-endpoint"]

    def detect_language(self, body):
        logger.info("###### Function Name : detect_language")

        path = 'detect'
        constructed_url = self.TRANSLATOR_TEXT_ENDPOINT + path
        
        headers = {
            'Ocp-Apim-Subscription-Key': self.TRANSLATOR_TEXT_RESOURCE_KEY,
            'Ocp-Apim-Subscription-Region': self.TRANSLATOR_TEXT_REGION,
            'Content-type': 'application/json',
            'X-ClientTraceId': str(uuid.uuid4())
        }

        try:
            request = requests.post(constructed_url, headers=headers, json=body)
            request.raise_for_status() 
        except requests.exceptions.RequestException as e:
            return logger.error(f"An error occurred while sending the request: {e}")

        try:
            response = request.json()
        except json.JSONDecodeError as e:
            return logger.error(f"JSON decoding error: {e}")
        
        else:
            logger.info("Language detection successful")
            logger.info(json.dumps(response, sort_keys=True, indent=4, ensure_ascii=False, separators=(',', ': ')))
        
        return response
    
    def translate_question(self, language_to, body, language_from=None) :
        logger.info("###### Function Name : translate_question")
        path = 'translate'
        constructed_url = self.TRANSLATOR_TEXT_ENDPOINT + path
 
        if language_from is None :
            params = {
                'api-version': '3.0',
                'to': language_to
            }
        else :
            params = {
                'api-version': '3.0',
                'from': language_from,
                'to': language_to
            }
 
        headers = {
            'Ocp-Apim-Subscription-Key': self.TRANSLATOR_TEXT_RESOURCE_KEY,
            # location required if you're using a multi-service or regional (not global) resource.
            'Ocp-Apim-Subscription-Region': self.TRANSLATOR_TEXT_REGION,
            'Content-type': 'application/json',
            'X-ClientTraceId': str(uuid.uuid4())
        }
 
        request = requests.post(constructed_url, params=params, headers=headers, json=body)
        response = request.json()
 
        logger.info("Language translation successful")
        logger.info(json.dumps(response, sort_keys=True, indent=4, ensure_ascii=False, separators=(',', ': ')))
 
        return response
    
    def translate_multi_question(self, language_to, texts, language_from=None) :
        logger.info("###### Function Name : translate_question")
        path = 'translate'
        constructed_url = self.TRANSLATOR_TEXT_ENDPOINT + path
 
        if language_from is None :
            params = {
                'api-version': '3.0',
                'to': language_to
            }
        else :
            params = {
                'api-version': '3.0',
                'from': language_from,
                'to': language_to
            }
 
        headers = {
            'Ocp-Apim-Subscription-Key': self.TRANSLATOR_TEXT_RESOURCE_KEY,
            # location required if you're using a multi-service or regional (not global) resource.
            'Ocp-Apim-Subscription-Region': self.TRANSLATOR_TEXT_REGION,
            'Content-type': 'application/json',
            'X-ClientTraceId': str(uuid.uuid4())
        }

        body = [{'text': text} for text in texts]
        request = requests.post(constructed_url, params=params, headers=headers, json=body)
        response = request.json()
 
        logger.info("Language translation successful")
        logger.info(json.dumps(response, sort_keys=True, indent=4, ensure_ascii=False, separators=(',', ': ')))
 
        return response

    
