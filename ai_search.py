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
import requests

from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient  
from azure.core.credentials import AzureKeyCredential  
from azure.search.documents import SearchClient, SearchIndexingBufferedSender
from azure.search.documents.models import VectorizedQuery,VectorFilterMode
from azure.core.exceptions import ResourceNotFoundError
from azure.search.documents.indexes.models import (  
    ExhaustiveKnnParameters,  
    ExhaustiveKnnAlgorithmConfiguration,
    FieldMapping,  
    HnswParameters,  
    HnswAlgorithmConfiguration,  
    InputFieldMappingEntry,  
    OutputFieldMappingEntry,  
    SimpleField,
    SearchField,  
    SearchFieldDataType,  
    SearchIndex,  
    SearchIndexer,  
    SearchIndexerDataContainer,  
    SearchIndexerDataSourceConnection,  
    SearchIndexerSkillset,  
    VectorSearch,  
    VectorSearchAlgorithmKind,  
    VectorSearchAlgorithmMetric,
    VectorSearchProfile,
    WebApiSkill,
    SearchableField
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
        
        self.index_client = SearchIndexClient(endpoint=self.service_endpoint, credential=AzureKeyCredential(self.account_key))
        self.indexer_client = SearchIndexerClient(endpoint=self.service_endpoint, credential=AzureKeyCredential(self.account_key))  

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
            existing_index = self.index_client.get_index(index_name)
            print(f"인덱스 '{index_name}'가 이미 존재합니다. 업데이트를 진행합니다.")

            index = SearchIndex(name=index_name, fields=fields, vector_search=vector_search)
            result = self.index_client.create_or_update_index(index)
            return print(f"인덱스 '{result.name}' 업데이트 완료")
            

        except Exception as e:
            print(f"인덱스 ( {index_name} ) 가 존재하지 않습니다. 새로운 인덱스를 생성합니다.")
            index = SearchIndex(name=index_name, fields=fields, vector_search=vector_search)
            result = self.index_client.create_or_update_index(index)
            return print(f"인덱스 ( {result.name} ) 생성 완료.")
        
    def delete_index(self,index_name) :
        
        try:
            self.index_client.get_index(index_name)
            self.index_client.delete_index(index_name)
            print(f"인덱스 ( {index_name} ) 삭제 완료.")

        except ResourceNotFoundError:
            print(f"인덱스 ( {index_name} ) 가 이미 존재하지 않습니다.")

    def create_data_source(self, index_name, directory_path):
        # Create a data source
        ds_client = SearchIndexerClient(self.service_endpoint, AzureKeyCredential(self.account_key))
        container = SearchIndexerDataContainer(name="documents", query=directory_path)
        data_source_connection = SearchIndexerDataSourceConnection(
            name=f"{index_name}-blob",
            type="azureblob",
            connection_string=self.as_account_str,
            container=container,
        )
        data_source = ds_client.create_or_update_data_source_connection(data_source_connection)
        
        print(f"Data source '{data_source.name}' created or updated")
    
    def update_data_source(self, index_name, directory_path):
        api_version = "2023-11-01"  

        data_source_connection_payload = {
            "name": f"{index_name}-blob",
            "type": "azureblob",
            "credentials": {
                "connectionString": self.as_account_str
            },
            "container": {
                "name": "documents",
                "query": directory_path
            },
            "dataChangeDetectionPolicy": None,
            "dataDeletionDetectionPolicy": {
                "@odata.type": "#Microsoft.Azure.Search.NativeBlobSoftDeleteDeletionDetectionPolicy"
            }
        }

        # HTTP 헤더 구성
        headers = {
            "Content-Type": "application/json",
            "api-key": self.account_key
        }

        # 데이터 소스 연결을 Azure Search에 생성 또는 업데이트
        response = requests.put(
            f"{self.service_endpoint}/datasources/{index_name}-blob?api-version={api_version}",
            headers=headers,
            json=data_source_connection_payload
        )

        # 요청이 성공했는지 확인
        if response.status_code == 200 or response.status_code == 201 or response.status_code == 204:
            print(f"Data source '{data_source_connection_payload['name']}' created or updated successfully.")
        else:
            print("Failed to create or update data source. Response:", response)

    
    def delete_data_source(self, index_name):
        api_version = "2023-11-01"
    
        # HTTP 헤더 구성
        headers = {
            "Content-Type": "application/json",
            "api-key": self.account_key
        }
    
        # Azure Search에서 데이터 소스 연결 삭제
        response = requests.delete(
            f"{self.service_endpoint}/datasources/{index_name}-blob?api-version={api_version}",
            headers=headers
        )
    
        # 요청이 성공했는지 확인
        if response.status_code == 200 or response.status_code == 204:
            print(f"Data source '{index_name}-blob' deleted successfully.")
        else:
            print("Failed to delete data source. Response:", response)



    def create_indexer(self, index_name) :

        # Create an indexer  
        indexer_name = f"{index_name}-indexer"
        indexer = SearchIndexer(  
            name=indexer_name,  
            description=f"{index_name}-Indexer",  
            skillset_name = "upload-data-to-index", 
            target_index_name=index_name,  
            data_source_name=f"{index_name}-blob",
            field_mappings=[
                FieldMapping(source_field_name="metadata_storage_path", target_field_name="main_text_path"),  
                FieldMapping(source_field_name="metadata_storage_name", target_field_name="file_name")  
            ],  
            output_field_mappings=[
                FieldMapping(source_field_name="/document/type", target_field_name="type"),
                FieldMapping(source_field_name="/document/iso_cd", target_field_name="iso_cd"),
                FieldMapping(source_field_name="/document/language", target_field_name="language"),
                FieldMapping(source_field_name="/document/product_group_code", target_field_name="product_group_code"),
                FieldMapping(source_field_name="/document/product_code", target_field_name="product_code"),
                FieldMapping(source_field_name="/document/product_model_code", target_field_name="product_model_code"),
                FieldMapping(source_field_name="/document/data_id", target_field_name="data_id"),
                FieldMapping(source_field_name="/document/mapping_key", target_field_name="mapping_key"),
                FieldMapping(source_field_name="/document/chunk_num", target_field_name="chunk_num"),
                FieldMapping(source_field_name="/document/pages", target_field_name="pages"),
                FieldMapping(source_field_name="/document/url", target_field_name="url"),
                FieldMapping(source_field_name="/document/title", target_field_name="title"),
                FieldMapping(source_field_name="/document/main_text_vector", target_field_name="main_text_vector")  
            ]  
        )  

        indexer_result = self.indexer_client.create_or_update_indexer(indexer)

    def update_indexer(self, index_name) :

        # Create an indexer  
        indexer_name = f"{index_name}-indexer"
        indexer = SearchIndexer(  
            name=indexer_name,  
            description=f"{index_name}-Indexer",  
            skillset_name = "upload-data-to-index", 
            target_index_name=index_name,  
            data_source_name=f"{index_name}-blob",
            parameters={
                "configuration": {
                    "executionEnvironment": "private"
                }
            },
            field_mappings=[
                FieldMapping(source_field_name="metadata_storage_path", target_field_name="main_text_path"),  
                FieldMapping(source_field_name="metadata_storage_name", target_field_name="file_name")  
            ],  
            output_field_mappings=[
                FieldMapping(source_field_name="/document/type", target_field_name="type"),
                FieldMapping(source_field_name="/document/iso_cd", target_field_name="iso_cd"),
                FieldMapping(source_field_name="/document/language", target_field_name="language"),
                FieldMapping(source_field_name="/document/product_group_code", target_field_name="product_group_code"),
                FieldMapping(source_field_name="/document/product_code", target_field_name="product_code"),
                FieldMapping(source_field_name="/document/product_model_code", target_field_name="product_model_code"),
                FieldMapping(source_field_name="/document/data_id", target_field_name="data_id"),
                FieldMapping(source_field_name="/document/mapping_key", target_field_name="mapping_key"),
                FieldMapping(source_field_name="/document/chunk_num", target_field_name="chunk_num"),
                FieldMapping(source_field_name="/document/pages", target_field_name="pages"),
                FieldMapping(source_field_name="/document/url", target_field_name="url"),
                FieldMapping(source_field_name="/document/title", target_field_name="title"),
                FieldMapping(source_field_name="/document/main_text_vector", target_field_name="main_text_vector")  
            ]  
        )  

        indexer_result = self.indexer_client.create_or_update_indexer(indexer)

    # def create_indexer(self, index_name):
    #     try:
    #         # Create an indexer
    #         indexer_name = f"{index_name}-indexer"
    #         indexer = {
    #             "name": indexer_name,
    #             "description": f"{index_name}-Indexer",
    #             "skillsetName": "upload-data-to-index",
    #             "targetIndexName": index_name,
    #             "dataSourceName": f"{index_name}-blob",
    #             "parameters": {
    #                 "configuration": {
    #                     "executionEnvironment": "Private"
    #                 }
    #             },
    #             "fieldMappings": [
    #                 {"sourceFieldName": "metadata_storage_path", "targetFieldName": "main_text_path"},
    #                 {"sourceFieldName": "metadata_storage_name", "targetFieldName": "file_name"}
    #             ],
    #             "outputFieldMappings": [
    #                 {"sourceFieldName": "/document/type", "targetFieldName":"type"},
    #                 {"sourceFieldName": "/document/iso_cd", "targetFieldName":"iso_cd"},
    #                 {"sourceFieldName": "/document/language", "targetFieldName":"language"},
    #                 {"sourceFieldName": "/document/product_group_code", "targetFieldName":"product_group_code"},
    #                 {"sourceFieldName": "/document/product_code", "targetFieldName":"product_code"},
    #                 {"sourceFieldName": "/document/product_model_code", "targetFieldName":"product_model_code"},
    #                 {"sourceFieldName": "/document/data_id", "targetFieldName":"data_id"},
    #                 {"sourceFieldName": "/document/mapping_key", "targetFieldName":"mapping_key"},
    #                 {"sourceFieldName": "/document/chunk_num", "targetFieldName":"chunk_num"},
    #                 {"sourceFieldName": "/document/pages", "targetFieldName":"pages"},
    #                 {"sourceFieldName": "/document/url", "targetFieldName":"url"},
    #                 {"sourceFieldName": "/document/title", "targetFieldName":"title"},
    #                 {"sourceFieldName": "/document/main_text_vector", "targetFieldName":"main_text_vector"}  
    #             ]
    #         }
    
    #         # HTTP 헤더 구성
    #         headers = {
    #             "Content-Type": "application/json",
    #             "api-key": self.account_key
    #         }
    
    #         # API 버전
    #         api_version = "2023-11-01"
            
    #         # Azure Search에 indexer 생성 또는 업데이트
    #         response = requests.put(
    #             f"{self.service_endpoint}/indexers/{indexer_name}?api-version={api_version}",
    #             headers=headers,
    #             json=indexer
    #         )
    
    #         # 요청이 성공했는지 확인
    #         if response.status_code in [200, 201, 204]:
    #             print(f"Indexer '{indexer_name}' created or updated successfully.")
    #         else:
    #             print("Failed to create or update indexer. Response:", response.text)
    
    #     except Exception as e:
    #         print(f"Failed to create or update indexer. Error: {e}")

    def create_skillset(self, skillset_name=None, skill_uri=None) :
        
        if skillset_name is None :
            skillset_name = "upload-data-to-index"

        if skill_uri is None :
            skill_uri = "https://lge-ai-prd-func.azurewebsites.net/aoai/api/upload_index"
    
        skill = WebApiSkill(
            uri=skill_uri,  
            inputs=[  
                InputFieldMappingEntry(name="file_path", source="/document/metadata_storage_path"),  
                InputFieldMappingEntry(name="recordId", source="/document/metadata_storage_name"),
            ],  
            outputs=[OutputFieldMappingEntry(name="type", target_name="type"),
                    OutputFieldMappingEntry(name="iso_cd", target_name="iso_cd"),
                    OutputFieldMappingEntry(name="language", target_name="language"),
                    OutputFieldMappingEntry(name="product_group_code", target_name="product_group_code"),
                    OutputFieldMappingEntry(name="product_code", target_name="product_code"),
                    OutputFieldMappingEntry(name="product_model_code", target_name="product_model_code"),
                    OutputFieldMappingEntry(name="data_id", target_name="data_id"),
                    OutputFieldMappingEntry(name="mapping_key", target_name="mapping_key"),
                    OutputFieldMappingEntry(name="chunk_num", target_name="chunk_num"),
                    OutputFieldMappingEntry(name="pages", target_name="pages"),
                    OutputFieldMappingEntry(name="url", target_name="url"),
                    OutputFieldMappingEntry(name="title", target_name="title"),
                    OutputFieldMappingEntry(name="main_text_vector", target_name="main_text_vector")],  
        )  
        
        skillset = SearchIndexerSkillset(  
            name=skillset_name,  
            description="Skillset to extract text vector",  
            skills=[skill],  
        )  

        self.indexer_client.create_or_update_skillset(skillset)  
        print(f' {skillset.name} created')  
        
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
    
    
    def get_documents(self, index_name, type=None, mapping_key=None):
        logger.info("###### Function Name : get_documents")
        if mapping_key is None :
            search_client = SearchClient(endpoint=self.service_endpoint, index_name=index_name, credential=AzureKeyCredential(self.account_key))
            result = search_client.search(search_text="*")
            output = []
            for document in result:
                output.append(document)
        
        else :
            search_client = SearchClient(endpoint=self.service_endpoint, index_name=index_name, credential=AzureKeyCredential(self.account_key))
            filter_query = f"{type} eq '{mapping_key}'"

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

    def get_num_documents(self, index_name) :
        # Azure Cognitive Search 서비스 및 인덱스 정보
        service_name = self.account_name
        index_name = index_name
        api_version = '2020-06-30'
        api_key = self.account_key  # Azure Cognitive Search 서비스의 API 키

        # REST API 요청을 보낼 URL
        url = f'https://{service_name}.search.windows.net/indexes/{index_name}/docs/$count?api-version={api_version}'

        # 인덱스 문서 수 확인
        headers = {
            'Content-Type': 'application/json',
            'api-key': api_key
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            document_count = int(response.content)
            print(f"인덱스 '{index_name}'의 문서 수: {document_count}")
            return document_count
        else:
            print(f"오류 발생: {response.status_code} - {response.content}")
            return None

    def backup_index(self, logger, index_name, batch_size):
        
        search_client = SearchClient(endpoint=self.service_endpoint,
                                 index_name=index_name,
                                 credential=AzureKeyCredential(self.account_key))
        
        as_instance = AzureStorage(container_name='preprocessed-data', storage_type='datast')
        
        korea_timezone = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_timezone)
        formatted_date = now.strftime('%Y-%m-%d')
        continue_search = True
        total_retrieved = 0
        
        while continue_search:
            response = search_client.search(
                search_text="*",  # 모든 문서를 가져오기 위한 쿼리
                top=batch_size,
                skip=total_retrieved,
                include_total_count=True
            )
        
            batch_results = [doc for doc in response]
            batch_size_current = len(batch_results)
            total_retrieved += batch_size_current
            json_bytes = json.dumps(batch_results).encode('utf-8')
            
            filename = f'backup_index/{index_name}/{formatted_date}/{total_retrieved - batch_size_current + 1}_to_{total_retrieved}.json'
            as_instance.upload_file(json_bytes, file_path=filename, overwrite=True)
            logger.info(filename)
            if batch_size_current < batch_size:
                continue_search = False
    
        logger.info(f"Total documents retrieved: {total_retrieved}")

    def recover_index(self, index_name, date) :

        as_instance = AzureStorage(container_name='preprocessed-data', storage_type='datast')

        blob_list = self.container_client.list_blobs(name_starts_with=f"backup_index/{index_name}/{date}/")
        file_path_list = []
        for blob in blob_list:
            file_path_list.append(blob.name)

        for file in file_path_list :
            documents = json.load(as_instance.read_file(file_path=file))
            self.upload_documents(index_name=index_name, documents=documents)

        print(f"Total documents recoverd : {self.get_num_documents(index_name=index_name)}")

        return self.get_num_documents(index_name=index_name)

    def reset_indexer(self, index_name):

        api_version='2020-06-30'
        # REST API URL 설정
        url = f"https://{self.account_name}.search.windows.net/indexers/{index_name}-indexer/reset?api-version={api_version}"
    
        # 요청 헤더에 API 키 포함
        headers = {
            "Content-Type": "application/json",
            "api-key": self.account_key
        }
    
        # POST 요청을 통해 인덱서 리셋
        response = requests.post(url, headers=headers)
    
        # 결과 출력
        if response.status_code == 204:
            print("Indexer has been successfully reset.")
        else:
            print(f"Failed to reset indexer: {response.status_code} - {response.text}")

        return response
        