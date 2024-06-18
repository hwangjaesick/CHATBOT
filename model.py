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
from tools import get_prompt
from retry import retry
import logging
import tiktoken
from credential import keyvault_values
import time # [2024.04.25] 추가
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
                                            temperature=0,
                                            model_kwargs={
                                                        "seed": 12345,
                                                        "response_format": {"type": "json_object"}
                                                    })
        
    @retry(tries=6, delay=10, backoff=1)
    def generate_evaluation(self, row, messages):
        logger.info("###### Function Name : generate_evaluation")

        response = self.openai_client.chat.completions.create(
            model="gpt-35-turbo",
            messages=messages,
            temperature=0,
            seed=0,
            max_tokens=800,
            response_format = {'type':"json_object"})
        results = response.choices[0].message.content
        return results

    
    @retry(tries=6, delay=10, backoff=1)
    def generate_embeddings(self, text, model=None):
        logger.info("###### Function Name : generate_embeddings")
        if model is None:
            model = "text-embedding-ada-002"
        return self.openai_client.embeddings.create(input=[text], model=model).data[0].embedding


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

# [2024.04.25] 추가
class OpenAI_Section:
    # 매뉴얼 섹션 분할용 GPT 모델 정의
    def __init__(self, resource_type, gpt_model="gpt-4-turbo-0125", max_tokens=4096, max_retries=2, sleep_time=2):
        # config 파일 설정
        if resource_type == 'm1':
            self.api_key  = keyvault_values["openai-api-key-m1"]
            self.api_base = keyvault_values["openai-api-base-m1"]
            self.api_version = keyvault_values["openai-api-version-m1"]
        else :
            self.api_key  = keyvault_values["openai-api-key-m2"]
            self.api_base = keyvault_values["openai-api-base-m2"]
            self.api_version = keyvault_values["openai-api-version-m2"]
            
        # client 초기화
        self.openai_client = AzureOpenAI(api_key = self.api_key,  
                                        api_version = self.api_version,
                                        azure_endpoint = self.api_base)
        self.model = gpt_model
        self.max_tokens  = max_tokens
        self.max_retries = max_retries
        self.sleep_time  = sleep_time
        
    def get_response(self, system_message:str, user_message:str):
        logger.info("###### Function Name : get_response")
        messages = [{
            "role":"system",
            "content":system_message
        },{
            "role":"user",
            "content":user_message
        }]
        
        response = self.openai_client.chat.completions.create(
            model=self.model
            , messages=messages
            , temperature=0
            , seed=0
            , max_tokens=self.max_tokens
            , stop=None
        )
        completion_message = response.choices[0].message.content
        time.sleep(self.sleep_time)
        return response, completion_message
    
