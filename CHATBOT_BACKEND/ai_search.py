import yaml
import uuid
from model import OpenAI
from database import MySql, AzureStorage
import pytz
from datetime import datetime, timedelta
import logging
import re
import json
from tools import extract_data
from credential import keyvault_values
from log import setup_logger
from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient  
from azure.core.credentials import AzureKeyCredential  
from azure.search.documents import SearchClient, SearchIndexingBufferedSender
from azure.search.documents.models import VectorizedQuery,VectorFilterMode
from azure.core.exceptions import ResourceNotFoundError
from azure.search.documents.indexes.models import (  
    HnswAlgorithmConfiguration,
    ExhaustiveKnnAlgorithmConfiguration,
    ExhaustiveKnnParameters,
    SearchIndex,  
    SearchField,  
    SearchFieldDataType,  
    SimpleField,  
    SearchableField,  
    VectorSearch,  
    HnswParameters,  
    VectorSearchAlgorithmKind,
    VectorSearchProfile,
    VectorSearchAlgorithmMetric,
    SearchIndexerDataContainer,  
    SearchIndexerDataSourceConnection, 
    )

logger = logging.getLogger(__name__)

class AISearch(OpenAI):
    def __init__(self, type=None):
        super().__init__()
        
        if type =='dev' :
            self.service_endpoint = keyvault_values["aisearch-dev-endpoint"]
            self.account_key = keyvault_values["aisearch-dev-account-key"]
            self.account_name = keyvault_values["aisearch-dev-account-name"]
            self.as_account_str = keyvault_values["storage-doc-account-str"]

        else :
            self.service_endpoint = keyvault_values["aisearch-endpoint"]
            self.account_key = keyvault_values["aisearch-account-key"]
            self.account_name = keyvault_values["aisearch-account-name"]
            self.as_account_str = keyvault_values["storage-doc-account-str"]
        
        # iclient 초기화
        self.index_client = SearchIndexClient(endpoint=self.service_endpoint, credential=AzureKeyCredential(self.account_key))

    def create_index(self, index_name) :

        # 필드명 정의
        fields = [SimpleField(name="id", type=SearchFieldDataType.String, key=True, sortable=True, filterable=True, facetable=True),
                SearchableField(name="type", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="iso_cd", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="language", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="product_group_code", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="product_code", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="product_model_code", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="data_id", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="mapping_key", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="chunk_num", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="file_name", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="pages", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="url", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="title", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchableField(name="main_text_path", type=SearchFieldDataType.String, filterable=True, facetable=True),
                SearchField(name="main_text_vector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                                    searchable=True, vector_search_dimensions=1536, vector_search_profile_name="myHnswProfile")]
        
        # 백터 서치 configuration ( HNSW, KNN 사용 가능 )
        vector_search = VectorSearch(
            algorithms=[
                HnswAlgorithmConfiguration(
                    name="myHnsw",
                    kind=VectorSearchAlgorithmKind.HNSW,
                    parameters=HnswParameters(
                        m=4,
                        ef_construction=400,
                        ef_search=500,
                        metric=VectorSearchAlgorithmMetric.COSINE
                    )
                ),
                ExhaustiveKnnAlgorithmConfiguration(
                    name="myExhaustiveKnn",
                    kind=VectorSearchAlgorithmKind.EXHAUSTIVE_KNN,
                    parameters=ExhaustiveKnnParameters(
                        metric=VectorSearchAlgorithmMetric.COSINE
                    )
                )
            ],
            profiles=[
                VectorSearchProfile(
                    name="myHnswProfile",
                    algorithm_configuration_name="myHnsw",
                ),
                VectorSearchProfile(
                    name="myExhaustiveKnnProfile",
                    algorithm_configuration_name="myExhaustiveKnn",
                )
            ]
        )

        try:
            # 인덱스가 이미 존재하는지 확인
            existing_index = self.index_client.get_index(index_name)
            print(f"인덱스 '{index_name}'가 이미 존재합니다. 업데이트를 진행합니다.")

            # 인덱스 업데이트 (필요한 경우)
            index = SearchIndex(name=index_name, fields=fields, vector_search=vector_search)
            result = self.index_client.create_or_update_index(index)
            return print(f"인덱스 '{result.name}' 업데이트 완료")
            

        except Exception as e:
            # 인덱스가 존재하지 않는 경우 예외 발생
            print(f"인덱스 ( {index_name} ) 가 존재하지 않습니다. 새로운 인덱스를 생성합니다.")

            # 새 인덱스 생성
            index = SearchIndex(name=index_name, fields=fields, vector_search=vector_search)
            result = self.index_client.create_or_update_index(index)
            return print(f"인덱스 ( {result.name} ) 생성 완료.")
        
    def delete_index(self,index_name) :
        
        try:
            # 인덱스가 이미 존재하는지 확인
            self.index_client.get_index(index_name)

            # 인덱스 삭제
            self.index_client.delete_index(index_name)
            print(f"인덱스 ( {index_name} ) 삭제 완료.")

        except ResourceNotFoundError:
            # 인덱스가 존재하지 않는 경우 예외 발생
            print(f"인덱스 ( {index_name} ) 가 이미 존재하지 않습니다.")
    
    def create_datasource(self, container_name, index_name) :  

        indexer_client = SearchIndexerClient(endpoint=self.service_endpoint, credential=AzureKeyCredential(self.account_key))
        container = SearchIndexerDataContainer(name=f'{container_name}')
        data_source_connection = SearchIndexerDataSourceConnection(
            name=f"{index_name}-blob",
            type="azureblob",
            connection_string=self.as_account_str,
            container=container,
        )
        data_source = indexer_client.create_or_update_data_source_connection(data_source_connection)

        return print(f"Data source '{data_source.name}' created or updated \nPlease go to the Azure portal and specify the detailed path.")
        
    def upload_documents(self, index_name, documents):
        
        # documents는 fileds 형식과 같아야함 ( sample.json )
        try : 
            search_client = SearchClient(endpoint=self.service_endpoint, index_name=index_name, credential=AzureKeyCredential(self.account_key))
            #search_client.upload_documents(documents)

            #문서가 많을 경우 아래 코드로 배치 처리
            with SearchIndexingBufferedSender(endpoint=self.service_endpoint, 
                                              index_name=index_name,  
                                              credential=AzureKeyCredential(self.account_key)) as batch_client: 
                    batch_client.upload_documents(documents=documents)

            return print(f"documents{len(documents)} upload complete")
        
        except Exception as e:
            return print(e)
    
    
    def get_documents(self, index_name, mapping_key=None):
        logger.info("###### Function Name : get_documents")
        if mapping_key is None :
            search_client = SearchClient(endpoint=self.service_endpoint, index_name=index_name, credential=AzureKeyCredential(self.account_key))
            result = search_client.search(search_text="*")
            output = []
            for document in result:
                output.append(document)
        
        else :
            search_client = SearchClient(endpoint=self.service_endpoint, index_name=index_name, credential=AzureKeyCredential(self.account_key))
            filter_query = f"mapping_key eq '{mapping_key}'"

            result = search_client.search(search_text="*",
                                           vector_filter_mode=VectorFilterMode.PRE_FILTER,
                                           filter=filter_query
                                           )

            output = []
            for document in result:
                output.append(document)

        return output
    
    def item_id_list(self, index_name, query, type, product_group_code_filter, product_code_filter):
        
        search_client = SearchClient(endpoint=self.service_endpoint, index_name=index_name, credential=AzureKeyCredential(self.account_key))

        filter_query = f"type eq '{type}' and iso_cd eq '{query['iso_cd']}' and language eq '{query['language']}' and \
                                    {product_group_code_filter} and \
                                    {product_code_filter}"

        result = search_client.search(search_text="*", select=['data_id'], filter = filter_query)

        output = []
        for document in result:
            output.append(document['data_id'])

        return output

        
    def delete_document(self, index_name, key, key_value) :
        search_client = SearchClient(endpoint=self.service_endpoint, index_name=index_name, credential=AzureKeyCredential(self.account_key))
        result = search_client.delete_documents(documents=[{key: str(key_value)}])
        if result[0].succeeded :
            return print("문서 %s = %s 가 삭제 되었습니다." % (key,key_value))
        else :
            return print("문서 %s = %s 를 찾을 수 없습니다." % (key,key_value))
        
    def update_document(self, index_name, document, key, key_value) :
        search_client = SearchClient(endpoint=self.service_endpoint, index_name=index_name, credential=AzureKeyCredential(self.account_key))
        result = search_client.merge_or_upload_documents(document)
        if result[0].succeeded :
            return print("문서 %s = %s 가 업데이트 되었습니다." % (key,key_value))
        else :
            return print("문서 %s = %s 를 찾을 수 없습니다." % (key,key_value))
    
    def intent_search(self, filter_df, index_name, query, top_k):
        logger = setup_logger(query['chatid'],query['chat_session_id'], query['rag_order'])
        logger.info("###### Function Name : intent_search")
        search_client = SearchClient(self.service_endpoint, index_name, credential=AzureKeyCredential(self.account_key))
        vector_query = VectorizedQuery(vector=self.generate_embeddings_chat(query['trans_question']), k_nearest_neighbors=50, fields="main_text_vector")

        product_group_code_list = filter_df['PROD_GROUP_CD'].drop_duplicates().tolist()
        product_code_list = filter_df['PROD_CD'].drop_duplicates().tolist()
        product_group_code_filter_strings = " or ".join([f"product_group_code eq '{item}'" for item in product_group_code_list])
        product_group_code_filter = f"({product_group_code_filter_strings})"
        product_code_filter_strings = " or ".join([f"product_code eq '{item}'" for item in product_code_list])
        product_code_filter = f"({product_code_filter_strings})"
            
            
        filter_query = f"iso_cd eq '{query['iso_cd']}' and language eq '{query['language']}' and {product_group_code_filter} and {product_code_filter}"
        
        logger.info("=" * 30 + " Intent Filtering " + "=" * 30)
        logger.info(f"filter_query : {filter_query}")
            
            
        results = search_client.search(search_text=query['trans_question'],
                                        search_fields=['title'],
                                        top=50,
                                        vector_queries= [vector_query],
                                        vector_filter_mode=VectorFilterMode.PRE_FILTER,
                                        filter=filter_query
                                        )
        
        korea_timezone = pytz.timezone('Asia/Seoul')
        korea_time = datetime.now(korea_timezone)
        current_time = korea_time.strftime('%Y-%m-%d %H:%M:%S')
        as_instance = AzureStorage(container_name='documents', storage_type='docst')
        preprocessed = []

        sql_instacne = MySql()
        intent_table = sql_instacne.get_table("""
        SELECT * FROM tb_chat_intent_mst""")

        for idx_result, result in enumerate(results):

            if idx_result >= 3:
                break
        # for result in results :

            if result["type"] == "general-inquiry" :
                
                mapping_key = '_'.join(result['mapping_key'].split('_')[5:])
                match_intent = [item for item in intent_table if item['INTENT_CODE'] == mapping_key and item['LOCALE_CD'] == query['locale_cd']]
                
                EVENT_CODE = match_intent[0]['EVENT_CD']
                INTENT_CODE = match_intent[0]['INTENT_CODE']
                related_link_url = match_intent[0]['RELATED_LINK_URL']
                answer = match_intent[0]['CHATBOT_RESPONSE']
                related_link_name = match_intent[0]['RELATED_LINK_NAME']
                
                data = {"timestamp" : current_time,
                        "id": result['id'],
                        "type": result['type'],
                        "iso_cd": result['iso_cd'],
                        "language": result['language'],
                        "product_group_code": result['product_group_code'],
                        "product_code": result['product_code'],
                        "product_model_code": result['product_model_code'],
                        "data_id" :  result['data_id'],
                        "mapping_key": result['mapping_key'],
                        "chunk_num" : result['chunk_num'],
                        "file_name": EVENT_CODE,
                        "pages" : INTENT_CODE,
                        "url": related_link_url if related_link_url != None else "",
                        "title": related_link_name,
                        "main_text_path" : result['main_text_path'],
                        "main_text": answer,
                        "question": query['question'],
                        "score": result['@search.score']}

                logger.info(f"###### search_result : {json.dumps(data,indent=4,ensure_ascii=False)}")
                if data["score"] >= 0.03 :
                    preprocessed.append(data)
                    break

            elif result["type"] == "contents" :
                
                try : 
                    text_data = as_instance.read_file(result['main_text_path'].split('documents/')[1])

                except :
                    text_data = ""

                data = {"timestamp" : current_time,
                        "id": result['id'],
                        "type": result['type'],
                        "iso_cd": result['iso_cd'],
                        "language": result['language'],
                        "product_group_code": result['product_group_code'],
                        "product_code": result['product_code'],
                        "product_model_code": result['product_model_code'],
                        "data_id" :  result['data_id'],
                        "mapping_key": result['mapping_key'],
                        "chunk_num" : result['chunk_num'],
                        "file_name": result['file_name'],
                        "pages" : result['pages'],
                        "url": result['url'],
                        "title": result['title'],
                        "main_text_path" : result['main_text_path'],
                        "main_text": text_data,
                        "question": query['question'],
                        "score": result['@search.score']}
                logger.info(f"###### search_result : {json.dumps(data,indent=4,ensure_ascii=False)}")
                preprocessed.append(data)

            else :
                try :
                    text_data = json.load(as_instance.read_file(result['main_text_path'].split('documents/')[1]))
                    text_data = text_data['main_text']
            
                except :
                    text_data = ""

                data = {"timestamp" : current_time,
                        "id": result['id'],
                        "type": result['type'],
                        "iso_cd": result['iso_cd'],
                        "language": result['language'],
                        "product_group_code": result['product_group_code'],
                        "product_code": result['product_code'],
                        "product_model_code": result['product_model_code'],
                        "data_id" :  result['data_id'],
                        "mapping_key": result['mapping_key'],
                        "chunk_num" : result['chunk_num'],
                        "file_name": result['file_name'],
                        "pages" : result['pages'],
                        "url": result['url'],
                        "title": result['title'],
                        "main_text_path" : result['main_text_path'],
                        "main_text": text_data,
                        "question": query['question'],
                        "score": result['@search.score']}
                logger.info(f"###### search_result : {json.dumps(data,indent=4,ensure_ascii=False)}")
                preprocessed.append(data)
       
        return preprocessed
    
    def spec_search(self, filter_df, index_name, query, top_k):

        logger = setup_logger(query['chatid'],query['chat_session_id'], query['rag_order'])
        logger.info("###### Function Name : spec_search")
        search_client = SearchClient(self.service_endpoint, index_name, credential=AzureKeyCredential(self.account_key))

        type = "spec"

        product_group_code_list = filter_df['PROD_GROUP_CD'].drop_duplicates().tolist()
        product_code_list = filter_df['PROD_CD'].drop_duplicates().tolist()
        product_group_code_filter_strings = " or ".join([f"product_group_code eq '{item}'" for item in product_group_code_list])
        product_group_code_filter = f"({product_group_code_filter_strings})"
        product_code_filter_strings = " or ".join([f"product_code eq '{item}'" for item in product_code_list])
        product_code_filter = f"({product_code_filter_strings})"

        corp_cd = query['corp_cd'].upper()
        local_cd = query['locale_cd'].upper()
        lang = query['language']
        
        if len(query['product_model_code']) == 0:
            data_id_filter = f"data_id eq '{str(uuid.uuid4())}'"

        else :
            product_model_code_list = query['product_model_code'].split(',')
            
            
            if len(product_model_code_list) > 1 :

                SALES_CODES = product_model_code_list 
                patterns = [f'%{code.strip()}%' for code in SALES_CODES]
                parameters = [corp_cd, local_cd] + patterns

                sql_instance = MySql()
                sql_query = f"""
                SELECT DISTINCT MATCHED_MODEL_CD
                FROM tb_sales_prod_map
                WHERE CORP_CD = %s AND LOCALE_CD = %s AND ({' OR '.join([f'SALES_CD LIKE %s' for _ in patterns])})
                """
                sales_code_mst = sql_instance.get_table(query=sql_query, parameters=parameters)

            else :
                SALES_CODE = query['product_model_code']
                pattern = f'%{SALES_CODE}%'
                parameters = (corp_cd, local_cd, pattern)
                sql_instance = MySql()
                sales_code_mst = sql_instance.get_table(query ="""
                                                        SELECT DISTINCT MATCHED_MODEL_CD
                                                        FROM tb_sales_prod_map
                                                        WHERE CORP_CD = %s AND LOCALE_CD = %s AND SALES_CD LIKE %s
                                                        """, parameters=parameters)
                logger.info(sales_code_mst)

            prod_model_cd_list = [item['MATCHED_MODEL_CD'] for item in sales_code_mst if item['MATCHED_MODEL_CD'] != 'NOT_MATCHED']
            prod_model_cd_list = list(set(prod_model_cd_list))

            if len(prod_model_cd_list) == 0 :
                data_id_filter = f"data_id eq '{str(uuid.uuid4())}'"
            else:
                data_id_list_mst = prod_model_cd_list

                data_id_list = data_id_list_mst[:30]

                data_id_filter_strings = ','.join(data_id_list)
                data_id_filter = f"search.in(data_id, '{data_id_filter_strings}', ',')"

        logger.info(query)
        logger.info(data_id_filter)
        filter_query = f"type eq '{type}' and type ne 'general-inquiry' and iso_cd eq '{query['iso_cd']}' and language eq '{query['language']}' and {product_group_code_filter} and {product_code_filter} and {data_id_filter}"
        
        logger.info("=" * 30 + " Spec Filtering " + "=" * 30)
        logger.info(f"filter_query : {filter_query}")

        if 'retrieve_error_code:' in query['refined_query'] :
            
            vector_query = VectorizedQuery(vector=self.generate_embeddings_chat(query['refined_query']), k_nearest_neighbors=50, fields="main_text_vector")
            search_text = extract_data(query['refined_query'],start="etrieve_error_code:")[0].strip()

            logger.info(f"############# ERROR_CODE : {search_text}")

            results = search_client.search(search_text=search_text,
                        search_fields=['title'],
                        top=50,
                        vector_queries= [vector_query],
                        vector_filter_mode=VectorFilterMode.PRE_FILTER,
                        filter=filter_query
                        )
        else :
            vector_query = VectorizedQuery(vector=self.generate_embeddings_chat(query['refined_query']), k_nearest_neighbors=top_k, fields="main_text_vector")
            results = search_client.search(search_text=None,
                                            vector_queries= [vector_query],
                                            vector_filter_mode=VectorFilterMode.PRE_FILTER,
                                            filter=filter_query
                                            )

        korea_timezone = pytz.timezone('Asia/Seoul')
        korea_time = datetime.now(korea_timezone)
        current_time = korea_time.strftime('%Y-%m-%d %H:%M:%S')

        as_instance = AzureStorage(container_name='documents', storage_type='docst')
        preprocessed = []

        for i, result in enumerate(results) :

            if i ==3 :
                break     

            try : 
                text_data = as_instance.read_file(result['main_text_path'].split('documents/')[1])

            except :
                text_data = ""

            data = {"timestamp" : current_time,
                    "id": result['id'],
                    "type": result['type'],
                    "iso_cd": result['iso_cd'],
                    "language": result['language'],
                    "product_group_code": result['product_group_code'],
                    "product_code": result['product_code'],
                    "product_model_code": result['product_model_code'],
                    "data_id" :  result['data_id'],
                    "mapping_key": result['mapping_key'],
                    "chunk_num" : result['chunk_num'],
                    "file_name": result['file_name'],
                    "pages" : result['pages'],
                    "url": result['url'],
                    "title": result['title'],
                    "main_text_path" : result['main_text_path'],
                    "main_text": text_data,
                    "question": query['question'],
                    "score": result['@search.score']}

            preprocessed.append(data)

            return preprocessed
    
    def contents_search(self, filter_df, index_name, query, top_k):
        logger = setup_logger(query['chatid'],query['chat_session_id'], query['rag_order'])
        logger.info("###### Function Name : contents_search")
        search_client = SearchClient(self.service_endpoint, index_name, credential=AzureKeyCredential(self.account_key))

        type = "contents"

        product_group_code_list = filter_df['PROD_GROUP_CD'].drop_duplicates().tolist()
        product_code_list = filter_df['PROD_CD'].drop_duplicates().tolist()
        product_group_code_filter_strings = " or ".join([f"product_group_code eq '{item}'" for item in product_group_code_list])
        product_group_code_filter = f"({product_group_code_filter_strings})"
        product_code_filter_strings = " or ".join([f"product_code eq '{item}'" for item in product_code_list])
        product_code_filter = f"({product_code_filter_strings})"
            
            
        filter_query = f"(type eq '{type}' or type eq 'microsites') and type ne 'general-inquiry' and iso_cd eq '{query['iso_cd']}' and language eq '{query['language']}' and {product_group_code_filter} and {product_code_filter}"
        
        logger.info("=" * 30 + " Contents Filtering " + "=" * 30)
        logger.info(f"filter_query : {filter_query}")

        if 'retrieve_error_code:' in query['refined_query'] :
            
            vector_query = VectorizedQuery(vector=self.generate_embeddings_chat(query['refined_query']), k_nearest_neighbors=50, fields="main_text_vector")
            search_text = extract_data(query['refined_query'],start="etrieve_error_code:")[0].strip()

            logger.info(f"############# ERROR_CODE : {search_text}")

            results = search_client.search(search_text=search_text,
                        search_fields=['title'],
                        top=50,
                        vector_queries= [vector_query],
                        vector_filter_mode=VectorFilterMode.PRE_FILTER,
                        filter=filter_query
                        )
        else :
            vector_query = VectorizedQuery(vector=self.generate_embeddings_chat(query['refined_query']), k_nearest_neighbors=top_k, fields="main_text_vector")
            results = search_client.search(search_text=None,
                                            vector_queries= [vector_query],
                                            vector_filter_mode=VectorFilterMode.PRE_FILTER,
                                            filter=filter_query
                                            )
        
        korea_timezone = pytz.timezone('Asia/Seoul')
        korea_time = datetime.now(korea_timezone)
        current_time = korea_time.strftime('%Y-%m-%d %H:%M:%S')
        
        as_instance = AzureStorage(container_name='documents', storage_type='docst')
        preprocessed = []

        for i, result in enumerate(results) :
            
            if i ==3 :
                break

            try : 
                text_data = as_instance.read_file(result['main_text_path'].split('documents/')[1])

            except :
                text_data = ""

            data = {"timestamp" : current_time,
                    "id": result['id'],
                    "type": result['type'],
                    "iso_cd": result['iso_cd'],
                    "language": result['language'],
                    "product_group_code": result['product_group_code'],
                    "product_code": result['product_code'],
                    "product_model_code": result['product_model_code'],
                    "data_id" :  result['data_id'],
                    "mapping_key": result['mapping_key'],
                    "chunk_num" : result['chunk_num'],
                    "file_name": result['file_name'],
                    "pages" : result['pages'],
                    "url": result['url'],
                    "title": result['title'],
                    "main_text_path" : result['main_text_path'],
                    "main_text": text_data,
                    "question": query['question'],
                    "score": result['@search.score']}
            
            preprocessed.append(data)

        return preprocessed
    
    def youtube_search(self, filter_df, index_name, query, top_k):
        logger = setup_logger(query['chatid'],query['chat_session_id'], query['rag_order'])
        logger.info("###### Function Name : youtube_search")
        search_client = SearchClient(self.service_endpoint, index_name, credential=AzureKeyCredential(self.account_key))

        type = "youtube"

        product_group_code_list = filter_df['PROD_GROUP_CD'].drop_duplicates().tolist()
        product_code_list = filter_df['PROD_CD'].drop_duplicates().tolist()
        product_group_code_filter_strings = " or ".join([f"product_group_code eq '{item}'" for item in product_group_code_list])
        product_group_code_filter = f"({product_group_code_filter_strings})"
        product_code_filter_strings = " or ".join([f"product_code eq '{item}'" for item in product_code_list])
        product_code_filter = f"({product_code_filter_strings})"
            
        filter_query = f"type eq '{type}' and type ne 'general-inquiry' and iso_cd eq '{query['iso_cd']}' and language eq '{query['language']}' and {product_group_code_filter} and {product_code_filter}"
        
        logger.info("=" * 30 + " Youtube Filtering " + "=" * 30)
        logger.info(f"filter_query : {filter_query}")

        if 'retrieve_error_code:' in query['refined_query'] :
            
            vector_query = VectorizedQuery(vector=self.generate_embeddings_chat(query['refined_query']), k_nearest_neighbors=50, fields="main_text_vector")
            search_text = extract_data(query['refined_query'],start="etrieve_error_code:")[0].strip()

            logger.info(f"############# ERROR_CODE : {search_text}")

            results = search_client.search(search_text=search_text,
                        search_fields=['title'],
                        top=50,
                        vector_queries= [vector_query],
                        vector_filter_mode=VectorFilterMode.PRE_FILTER,
                        filter=filter_query
                        )
        else :
            vector_query = VectorizedQuery(vector=self.generate_embeddings_chat(query['refined_query']), k_nearest_neighbors=top_k, fields="main_text_vector")
            results = search_client.search(search_text=None,
                                            vector_queries= [vector_query],
                                            vector_filter_mode=VectorFilterMode.PRE_FILTER,
                                            filter=filter_query
                                            )

        korea_timezone = pytz.timezone('Asia/Seoul')
        korea_time = datetime.now(korea_timezone)
        current_time = korea_time.strftime('%Y-%m-%d %H:%M:%S')
        
        as_instance = AzureStorage(container_name='documents', storage_type='docst')
        preprocessed = []

        for i, result in enumerate(results) :

            if i ==3 :
                break   
            
            try :
                text_data = json.load(as_instance.read_file(result['main_text_path'].split('documents/')[1]))
                text_data = text_data['main_text']
            
            except :
                text_data = ""

            data = {"timestamp" : current_time,
                    "id": result['id'],
                    "type": result['type'],
                    "iso_cd": result['iso_cd'],
                    "language": result['language'],
                    "product_group_code": result['product_group_code'],
                    "product_code": result['product_code'],
                    "product_model_code": result['product_model_code'],
                    "data_id" :  result['data_id'],
                    "mapping_key": result['mapping_key'],
                    "chunk_num" : result['chunk_num'],
                    "file_name": result['file_name'],
                    "pages" : result['pages'],
                    "url": result['url'],
                    "title": result['title'],
                    "main_text_path" : result['main_text_path'],
                    "main_text": text_data,
                    "question": query['question'],
                    "score": result['@search.score']}
            
            preprocessed.append(data)

        return preprocessed
    
    def manual_search(self, filter_df, index_name, query, top_k):
        logger = setup_logger(query['chatid'],query['chat_session_id'], query['rag_order'])
        logger.info("###### Function Name : manual_search")
        search_client = SearchClient(self.service_endpoint, index_name, credential=AzureKeyCredential(self.account_key))

        type = "manual"

        product_group_code_list = filter_df['PROD_GROUP_CD'].drop_duplicates().tolist()
        product_code_list = filter_df['PROD_CD'].drop_duplicates().tolist()
        product_group_code_filter_strings = " or ".join([f"product_group_code eq '{item}'" for item in product_group_code_list])
        product_group_code_filter = f"({product_group_code_filter_strings})"
        product_code_filter_strings = " or ".join([f"product_code eq '{item}'" for item in product_code_list])
        product_code_filter = f"({product_code_filter_strings})"
        
        corp_cd = query['corp_cd'].upper()
        local_cd = query['locale_cd'].upper()
        lang = query['language']
        type = "manual"
        
        if len(query['product_model_code']) == 0:
            data_id_filter = f"data_id eq '{str(uuid.uuid4())}'"

        else :
            product_model_code_list = query['product_model_code'].split(',')
            
            if len(product_model_code_list) > 1 :

                SALES_CODES = product_model_code_list 
                patterns = [f'%{code.strip()}%' for code in SALES_CODES]
                parameters = [corp_cd, local_cd] + patterns

                sql_instance = MySql()
                sql_query = f"""
                SELECT DISTINCT PROD_MODEL_CD, SUFFIX_CD, MDMS_PROD_CD
                FROM tb_sales_prod_map
                WHERE CORP_CD = %s AND LOCALE_CD = %s AND ({' OR '.join([f'SALES_CD LIKE %s' for _ in patterns])})
                """
                sales_code_mst = sql_instance.get_table(query=sql_query, parameters=parameters)

            else :
                SALES_CODE = query['product_model_code']
                pattern = f'%{SALES_CODE}%'
                parameters = (corp_cd, local_cd, pattern)
                sql_instance = MySql()
                sales_code_mst = sql_instance.get_table(query ="""
                                                        SELECT DISTINCT PROD_MODEL_CD, SUFFIX_CD, MDMS_PROD_CD
                                                        FROM tb_sales_prod_map
                                                        WHERE CORP_CD = %s AND LOCALE_CD = %s AND SALES_CD LIKE %s
                                                        """, parameters=parameters)
            
            prod_model_cd_list = [item['PROD_MODEL_CD'] for item in sales_code_mst]

            if len(prod_model_cd_list) == 0 :
                data_id_filter = f"data_id eq '{str(uuid.uuid4())}'"
            else:
                #item_id_mst_2 = [[f"{item['PROD_MODEL_CD']}.{item['SUFFIX_CD']}",item['MDMS_PROD_CD'],item['ITEM_ID']] for item in product_model_code_mst]
                #data_id_list = [item[2] for item in item_id_mst_2]
                # item id를 이용해서 filtering
                #data_id_list_index = self.item_id_list(index_name, query, type, product_group_code_filter, product_code_filter)

                sql_instance = MySql()
                format_strings = ','.join(['%s'] * len(prod_model_cd_list))
                parameters = (corp_cd, lang) + tuple(prod_model_cd_list)

                product_model_code_mst = sql_instance.get_table(query=f"""
                                                                        SELECT DISTINCT ITEM_ID
                                                                        FROM tb_if_manual_list
                                                                        WHERE CORP_CD = %s AND LANGUAGE_LIST = %s AND PROD_MODEL_CD IN ({format_strings})
                                                                    """, parameters=parameters)
                if len(product_model_code_mst) == 0:
                    data_id_filter = f"data_id eq '{str(uuid.uuid4())}'"
                else :
                    data_id_list_mst = [item['ITEM_ID'] for item in product_model_code_mst]
                    
                    # set_data_id_list_index = set(data_id_list_index)
                    # set_data_id_list_mst = set(data_id_list_mst)

                    # intersection = set_data_id_list_index.intersection(set_data_id_list_mst)
                    # data_id_list = list(intersection)

                    data_id_list = data_id_list_mst[:30]
                    
                    # data_id_filter_strings = " or ".join([f"data_id eq '{item}'" for item in data_id_list])
                    # data_id_filter = f"({data_id_filter_strings})"

                    data_id_filter_strings = ','.join(data_id_list)
                    data_id_filter = f"search.in(data_id, '{data_id_filter_strings}', ',')"
        
        logger.info(data_id_filter)
        logger.info(query)
        filter_query = f"type eq '{type}' and type ne 'general-inquiry' and iso_cd eq '{query['iso_cd']}' and language eq '{query['language']}' and {product_group_code_filter} and {product_code_filter} and {data_id_filter}"
        
        logger.info("=" * 30 + " Manual Filtering " + "=" * 30)
        logger.info(f"filter_query : {filter_query}")

        if 'retrieve_error_code:' in query['refined_query'] :
            
            vector_query = VectorizedQuery(vector=self.generate_embeddings_chat(query['refined_query']), k_nearest_neighbors=50, fields="main_text_vector")
            search_text = extract_data(query['refined_query'],start="etrieve_error_code:")[0].strip()

            logger.info(f"############# ERROR_CODE : {search_text}")

            results = search_client.search(search_text=search_text,
                        search_fields=['title'],
                        top=50,
                        vector_queries= [vector_query],
                        vector_filter_mode=VectorFilterMode.PRE_FILTER,
                        filter=filter_query
                        )
        else :
            vector_query = VectorizedQuery(vector=self.generate_embeddings_chat(query['refined_query']), k_nearest_neighbors=top_k, fields="main_text_vector")
            results = search_client.search(search_text=None,
                                            vector_queries= [vector_query],
                                            vector_filter_mode=VectorFilterMode.PRE_FILTER,
                                            filter=filter_query
                                            )

        korea_timezone = pytz.timezone('Asia/Seoul')
        korea_time = datetime.now(korea_timezone)
        current_time = korea_time.strftime('%Y-%m-%d %H:%M:%S')
        
        as_instance = AzureStorage(container_name='documents', storage_type='docst')
        preprocessed = []

        for i, result in enumerate(results) :

            if i ==3 :
                break   
            
            try :
                text_data = json.load(as_instance.read_file(result['main_text_path'].split('documents/')[1]))
                text_data = text_data['main_text']
            except :
                text_data = ""

            data = {"timestamp" : current_time,
                    "id": result['id'],
                    "type": result['type'],
                    "iso_cd": result['iso_cd'],
                    "language": result['language'],
                    "product_group_code": result['product_group_code'],
                    "product_code": result['product_code'],
                    "product_model_code": result['product_model_code'],
                    "data_id" :  result['data_id'],
                    "mapping_key": result['mapping_key'],
                    "chunk_num" : result['chunk_num'],
                    "file_name": result['file_name'],
                    "pages" : result['pages'],
                    "url": result['url'],
                    "title": result['title'],
                    "main_text_path" : result['main_text_path'],
                    "main_text": text_data,
                    "question": query['question'],
                    "score": result['@search.score']}
            
            preprocessed.append(data)

        return preprocessed
        