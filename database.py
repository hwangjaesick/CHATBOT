from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions, ContentSettings, generate_container_sas, ContainerSasPermissions
from azure.core.exceptions import *
from azure.storage.fileshare import ShareServiceClient, ShareClient, ShareFileClient

from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import *

import mysql.connector
from mysql.connector import Error

import yaml
import datetime
import io
import pandas as pd
import json
from dateutil import parser
from credential import keyvault_values
import logging
logger = logging.getLogger(__name__)

class AzureStorage:

    def __init__(self, container_name='sample-data', storage_type = None):
    
        if storage_type == 'docst' :

            self.account_key = keyvault_values["storage-doc-account-key"]
            self.account_name = keyvault_values["storage-doc-account-name"]
            self.account_str = keyvault_values["storage-doc-account-str"]
        
        elif storage_type == 'log' :
            self.account_key = keyvault_values["storage-log-account-key"]
            self.account_name = keyvault_values["storage-log-account-name"]
            self.account_str = keyvault_values["storage-log-account-str"]
        else :
            self.account_key = keyvault_values["storage-raw-account-key"]
            self.account_name = keyvault_values["storage-raw-account-name"]
            self.account_str = keyvault_values["storage-raw-account-str"]

        if storage_type == 'log' :

            self.log_service_client = ShareServiceClient.from_connection_string(self.account_str)
            self.log_share_client = self.log_service_client.get_share_client(container_name)

        else :
            # client 초기화
            self.blob_service_client = BlobServiceClient.from_connection_string(self.account_str)
            self.container_name = container_name
            try : 
                self.container_client = self.blob_service_client.get_container_client(self.container_name)
                logger.info(f"Container connection completed \nContainer name : {container_name}")
            
            except Exception as e :
                logger.error(f"Container connection error \nContainer name : {container_name} \nError message : {str(e)}")

    def read_file(self, file_path, sheet_name=None):
        logger.info("###### Function Name : read_file")
        try:
            file_client = self.container_client.get_blob_client(file_path)

            if file_path.split('.')[-1] == 'xlsx' or file_path.split('.')[-1] == 'csv':
                with io.BytesIO() as blob_io:
                    file_client.download_blob().readinto(blob_io)
                    blob_io.seek(0)
                    if file_path.split('.')[-1] == 'xlsx' : 
                        file = pd.read_excel(blob_io, sheet_name=sheet_name if sheet_name is not None else 0)
                    else :
                        file = pd.read_csv(blob_io)
                logger.info(f"File load completed \nPath : {file_path}")
                return file
            
            elif file_path.split('.')[-1] == 'pdf' or file_path.split('.')[-1] == 'json':
                download = file_client.download_blob()
                file = io.BytesIO(download.readall())
                logger.info(f"File load completed \nPath : {file_path}")
                return file
            
            else:
                download = file_client.download_blob()
                downloaded_bytes = download.readall()
                logger.info(f"File load completed \nPath : {file_path}")
                return downloaded_bytes.decode("utf-8")

        except Exception as e:
            logger.error(f"File load error \nPath : {file_path} \nError message : {str(e)}")
            return None
        
    def upload_log_file_share(self, log_data, directory_name, file_name):
        logger.info("###### Function Name : upload_log_file_share")
        file_client = self.log_share_client.get_file_client(directory_name+'/'+file_name)

        directories = directory_name.split('/')
        current_path = ""
        for directory in directories:
            current_path = f"{current_path}/{directory}" if current_path else directory
            try:
                directory_client = self.log_share_client.get_directory_client(current_path)
                directory_client.get_directory_properties()
            except Exception as e:
                self.log_share_client.create_directory(current_path)

        try:
            # 파일의 현재 속성을 가져와 현재 크기를 확인
            file_props = file_client.get_file_properties()
            current_length = file_props.size
            new_data = ('\n\n' + log_data).encode('utf-8')  # 새 데이터 앞에 개행 문자 추가
            new_length = len(new_data)
            
            # 기존 파일의 크기를 새 데이터의 길이만큼 증가
            file_client.resize_file(current_length + new_length)
            
            # 새 데이터를 파일의 끝에 추가
            file_client.upload_range(new_data, offset=current_length, length=new_length)

        except Exception as e:
            # 파일이 없으면 새로 생성
            new_data = log_data.encode('utf-8')
            new_length = len(new_data)
            file_client.create_file(size=new_length)
            file_client.upload_range(new_data, offset=0, length=new_length)

        return logger.info(f"File upload completed, Path: {directory_name+'/'+file_name}")
        
    def upload_file(self, data, file_path, content_type=None, overwrite=False):
        logger.info("###### Function Name : upload_file")
        try:
            content_settings = None
            if content_type is not None:
                content_settings = ContentSettings(content_type=content_type)

            blob_client = self.container_client.upload_blob(
                name=file_path,
                data=data,
                content_settings=content_settings,
                overwrite=overwrite
            )
            return logger.info(f"File upload completed, Path: {blob_client.url}")

        except ResourceExistsError as e:
            blob_client = self.container_client.upload_blob(
                name=file_path,
                data=data,
                content_settings=content_settings,
                overwrite=overwrite
            )
            return logger.error(f"File already exists, Path: {blob_client.url}\n Please set the overwrite variable to True.")

        except Exception as e: 
            return logger.error(f"File upload error, Path: {file_path}\n Error message : {str(e)}")
        
    def upload_log(self, data, file_path, content_type=None):
        logger.info("###### Function Name : upload_log")
        try:
            content_settings = None
            if content_type is not None:
                content_settings = ContentSettings(content_type=content_type)

            # BlobClient를 사용하여 append blob 관련 작업을 수행합니다.
            blob_client = self.container_client.get_blob_client(blob=file_path)

            # Append blob이 존재하지 않으면 새로 생성합니다.
            if not blob_client.exists():
                blob_client.create_append_blob(content_settings=content_settings)
                logger.info("Created new append blob.")

            # 데이터를 append blob에 추가합니다.
            blob_client.append_block(data)
            return logger.info(f"File upload completed, Path: {blob_client.url}")
        except Exception as e:
            logger.error(f"Failed to upload file: {str(e)}")
            return None

    def get_sas_url_container(self, expiry_time=True) :
        
        logger.info("###### Function Name : get_sas_url")
        if expiry_time :
            kst = datetime.timezone(datetime.timedelta(hours=9))
            start_time_kst = datetime.datetime.now(kst)

            expiry_time_kst = start_time_kst + datetime.timedelta(minutes=20)
            start_time_utc = start_time_kst.astimezone(datetime.timezone.utc)
            expiry_time_utc = expiry_time_kst.astimezone(datetime.timezone.utc)
        
        else :
            kst = datetime.timezone(datetime.timedelta(hours=9))
            start_time_kst = datetime.datetime.now(kst)
            start_time_utc = start_time_kst.astimezone(datetime.timezone.utc)
            expiry_time_utc = datetime.datetime(9999, 12, 31)

        try: 
            sas_token = generate_container_sas(
                account_name=self.account_name,
                container_name=self.container_name,
                account_key=self.account_key,
                permission=ContainerSasPermissions(read=True, write=True, delete=True, list=True),
                start=start_time_utc,
                expiry=expiry_time_utc  # 20분 동안 유효
            )
            sas_url = f"https://{self.account_name}.blob.core.windows.net/{self.container_name}?{sas_token}"
            logger.info(f"SAS URL issued completed \nUrl: {sas_url}")
            return sas_url

        except Exception as e:
            sas_url = None  
            logger.error(f"SAS URL issued error \nUrl: {sas_url} \nError message: {str(e)}")
            return None  
    
    def get_sas_url(self, blob_path, expiry_time=True) :
        
        logger.info("###### Function Name : get_sas_url")
        blob_client = self.container_client.get_blob_client(blob_path)
        if expiry_time :
            kst = datetime.timezone(datetime.timedelta(hours=9))
            start_time_kst = datetime.datetime.now(kst) - datetime.timedelta(minutes=5)

            expiry_time_kst = start_time_kst + datetime.timedelta(days=1)
            start_time_utc = start_time_kst.astimezone(datetime.timezone.utc)
            expiry_time_utc = expiry_time_kst.astimezone(datetime.timezone.utc)
        
        else :
            kst = datetime.timezone(datetime.timedelta(hours=9))
            start_time_kst = datetime.datetime.now(kst) - datetime.timedelta(minutes=5)
            start_time_utc = start_time_kst.astimezone(datetime.timezone.utc)
            expiry_time_utc = datetime.datetime(9999, 12, 31)

        try: 
            sas_token = generate_blob_sas(
                account_name=self.account_name,
                account_key=self.account_key,
                container_name=self.container_name,
                blob_name=blob_path,
                permission=BlobSasPermissions(read=True, write=True, delete=True, add=True, create=True, update=True, process=True),
                start=start_time_utc,
                expiry=expiry_time_utc                  
            )
            sas_url = blob_client.url + '?' + sas_token
            logger.info(f"SAS URL issued completed \nUrl: {blob_client.url}")
            return sas_url

        except Exception as e:
            sas_url = None  
            logger.error(f"SAS URL issued error \nUrl: {blob_client.url} \nError message: {str(e)}")
            return None  

    
    # 특정 경로 blob 파일 추출

    def extract_blob_list(self, blob_path, file_type=None) :
        logger.info("###### Function Name : extract_blob_list")
        container_name = self.container_client.container_name
        file_list = self.container_client.list_blobs(name_starts_with=blob_path)
        if file_type :
            filter_list = [item['name'] for item in file_list if f'.{file_type}' in item['name']]
            logger.info(f"File list extraction completed, File extension : {file_type} \nPath: {container_name}/{blob_path}")
            return filter_list
        else :
            file_list = [item['name'] for item in file_list]
            logger.info(f"File list extraction completed, File extension : ALL \nPath: {container_name}/{blob_path}")
            return file_list

    
    # last_update_time 기준으로 이후에 들어온 파일 스트리밍
    def file_streaming(self, blob_path, last_update_time=None):
        logger.info("###### Function Name : file_streaming")
        file_list = []

        for blob in self.container_client.list_blobs(name_starts_with=blob_path) :
            # 실시간으로 업데이트시 데이터 누락 가능성이 있어서 보수적으로 최신 업데이트 시간에서 1시간 뺌
            if blob['last_modified'].replace(tzinfo=None) + datetime.timedelta(hours=9) > last_update_time - datetime.timedelta(hours=1) :
                file_list.append(blob['name'])
        logger.info("End file streaming")
        return file_list
    
class CosmosDB:
    def __init__(self):
        
        self.url = keyvault_values["cosmosdb-url"]
        self.key = keyvault_values["cosmosdb-key"]

        # client 초기화
        self.client = CosmosClient(self.url, credential=self.key)
    
    def create_database(self, database_name):
        logger.info("###### Function Name : create_database")
        try:
            database_client = self.client.create_database(id=database_name)
            return logger.info(f"Database ( {database_name} ) creation complete.")
        except CosmosResourceExistsError:
            return logger.info(f"Database ( {database_name} ) already exists.")
    
    # 컨테이너 확인 및 생성
    def create_container(self, database_name, container_name, partition_key_path):
        logger.info("###### Function Name : create_container")
        try:
            database_client = self.client.get_database_client(database_name)
            container_client = database_client.create_container(id=container_name, partition_key=PartitionKey(path=partition_key_path))
            return logger.info(f"Container ( {container_name} ) creation complete.")
        except CosmosResourceExistsError:
            return logger.info(f"Container ( {container_name} ) already exists.")
        
    # 데이터베이스 확인 및 삭제
    def delete_database(self, database_name):
        logger.info("###### Function Name : delete_database")
        database_client = self.client.create_database_if_not_exists(id=database_name)
        if database_client.read():
            database_client.delete()
            logger.info(f"Database ( {database_name} ) deletion complete.")
        else:
            logger.info(f"Database ( {database_name} ) does not exist.") 
    
    # 컨테이너 확인 및 삭제
    def delete_container(self, database_name, container_name) :
        logger.info("###### Function Name : delete_container")
        database_client = self.client.create_database_if_not_exists(id=database_name)
        container_client = database_client.get_container_client(container_name)
        if container_client.read():
            container_client.delete_container()
            logger.info(f"Container ( {database_name} ) deletion complete.")
        else:
            logger.info(f"Container ( {database_name} ) does not exist.") 

    # data 저장
    def upload_data(self, database_name, container_name, data):
        logger.info("###### Function Name : upload_data")
        try : 
            database = self.client.get_database_client(database_name)
            container = database.get_container_client(container_name)
            container.create_item(body=data)
            return logger.info("Data upload completed")   
        except Error as e :
            return logger.error(str(e))

    # data 삭제 ( item_value : id의 value, partition_key_value : /chatid value)
    def delete_data(self, database_name, container_name, item_value, partition_key_value):
        logger.info("###### Function Name : delete_data")

        database = self.client.get_database_client(database_name)
        container = database.get_container_client(container_name)
        container.delete_item(item=item_value,partition_key=partition_key_value)
        return logger.info("Data deletion complete")
    
    def read_data(self, database_name, container_name, user_id, chat_session_id) :
        logger.info("###### Function Name : read_data")

        result = []
        database = self.client.get_database_client(database_name)
        container = database.get_container_client(container_name)
        try:
            query = f"SELECT * FROM c WHERE c.chatid = '{user_id}' and c.chat_session_id = {chat_session_id}"

            items = container.query_items(query, enable_cross_partition_query=True)
            for item in items:
                result.append(item)
        except CosmosResourceNotFoundError as e:
            logger.error(f"Error: {e}")
        
        return result
        
class MySql:
    def __init__(self, database_name='lgchatbot'):
   
        self.host = keyvault_values["mysql-host"]
        self.user = keyvault_values["mysql-user"]
        self.password = keyvault_values["mysql-password"]
        self.database = database_name
        
        try :
            config = {
            'user': self.user,
            'password': self.password,
            'host': self.host,
            'database': self.database,
            'ssl_verify_identity': True,  # 인증서 신뢰 여부 설정
            'ssl_ca': './config/DigiCertGlobalRootCA.crt.pem'  # CA 인증서 파일 경로 설정
            }

            # Azure MySQL에 연결
            self.connection = mysql.connector.connect(**config)
            
        except Error as e:
            logger.error(f"Database connection error : {e}")

    def get_table(self, query, parameters=None):
        logger.info("###### Function Name : get_table")
        try:
            #db_info = self.connection.get_server_info()
            #print(f"MySQL 서버 버전: {db_info}")
            cursor = self.connection.cursor()
            
            if parameters is None:
                cursor.execute(query)
            else:
                cursor.execute(query, parameters)
            
            rows = cursor.fetchall()
            
            columns = [column[0] for column in cursor.description]
            
            result = [dict(zip(columns, row)) for row in rows]
            logger.info("Table load complete")
            return result
        except Exception as e :
            return logger.error(f"{str(e)}")
        
        finally:
            if cursor:
                cursor.close()
            if self.connection:
                self.connection.close()

    def insert_data(self, query, parameters=None):
        
        logger.info("###### Function Name : insert_data")
        
        try :
            cursor = self.connection.cursor()
            cursor.execute(query, parameters)
            self.connection.commit()
            logger.info("Data inserted successfully")
        except Error as e:
            logger.error("Error while connecting to MySQL", e)
        finally:
            if self.connection.is_connected():
                cursor.close()
                self.connection.close()



    