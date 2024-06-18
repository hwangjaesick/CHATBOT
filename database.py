from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions, ContentSettings
from azure.core.exceptions import *

from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import *

import mysql.connector
from mysql.connector import Error

from sqlalchemy import create_engine, engine
from sqlalchemy.dialects.mysql import *

import yaml
import datetime
import io
import os
import pandas as pd
import json
import pytz
from dateutil import parser
from credential import keyvault_values
# from log import make_logger
import logging
logger = logging.getLogger(__name__)

class AzureStorage:

    def __init__(self, container_name='sample-data', storage_type = None):

        if storage_type == 'docst' :

            self.account_key = keyvault_values["storage-doc-account-key"]
            self.account_name = keyvault_values["storage-doc-account-name"]
            self.account_str = keyvault_values["storage-doc-account-str"]
            
        elif storage_type == 'docst-prd' :
            self.account_key = keyvault_values["prd-storage-doc-account-key"]
            self.account_name = keyvault_values["prd-storage-doc-account-name"]
            self.account_str = keyvault_values["prd-storage-doc-account-str"]
            
        elif storage_type == 'datast' :
            self.account_key = keyvault_values["storage-raw-account-key"]
            self.account_name = keyvault_values["storage-raw-account-name"]
            self.account_str = keyvault_values["storage-raw-account-str"]
        
        else :
            self.account_key = keyvault_values["prd-storage-doc-account-key"]
            self.account_name = keyvault_values["prd-storage-doc-account-name"]
            self.account_str = keyvault_values["prd-storage-doc-account-str"]

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
        
    def delete_file(self, container, blob):

        container_client = self.blob_service_client.get_container_client(container)
        blob_client = container_client.get_blob_client(blob)
        try :
            properties = blob_client.get_blob_properties()
        except ResourceNotFoundError :
            return logger.info(f"Blob '{blob}' does not exist.")

        blob_client.delete_blob()
        return logger.info(f"Blob '{blob}' delete complete.")
    
    def delete_file_with_date(self, blob_path, date):

        korea_zone = pytz.timezone('Asia/Seoul')
        cutoff_date = datetime.strptime(date, '%Y-%m-%d').replace(tzinfo=korea_zone)
        
        blob_list = self.container_client.list_blobs(name_starts_with=blob_path)

        for blob in blob_list:
            blob_creation_time_kst = blob.creation_time.astimezone(korea_zone)
            if blob_creation_time_kst > cutoff_date and blob.name.endswith('.txt'):
                blob_client = self.container_client.get_blob_client(blob=blob.name)
                blob_client.delete_blob()
                print(f"Deleted: {blob.name}")
                logger.info(f"Deleted: {blob.name}")
    
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
    
    def delete_blobs(self, name_starts_with, backup='Y'):
        logger.info("###### Function Name : delete_blobs")
        blob_list = self.container_client.list_blob_names(name_starts_with=name_starts_with)
        del_blob_list = [x for x in blob_list if os.path.splitext(x)[-1] != ''] # 파일인 blob 삭제
        
        today_str = datetime.date.today().strftime("%Y%m%d")
        for bname in del_blob_list:
            if backup == 'Y':
                source_sas_url = self.get_sas_url(bname)
                backup_bname = f"Backup/{today_str}/{bname}"
                backup_blob  = self.container_client.get_blob_client(backup_bname)
                backup_blob.upload_blob_from_url(source_url = source_sas_url, overwrite=True)

            del_blob_client = self.container_client.get_blob_client(bname)
            del_blob_client.delete_blob()

    def delete_directory(self, directory_path, file_type) :
        # 폴더 경로 내 모든 blobs 열거
        blob_list = self.container_client.list_blobs(name_starts_with=directory_path)
        file_list = []
        for blob in blob_list:
            if blob.name.endswith(file_type):
                file_list.append(blob)
        for file in file_list:
            blob_client = self.container_client.get_blob_client(file)
            blob_client.delete_blob()
            print(f"delete file : {file.name}")
    
    # [2024.04.25] 추가
    def save_section(self, res_dict:dict, texts:list, base_path:str):
        logger.info("###### Function Name : save_section")
        for g_name, pnum_list in res_dict.items():
            group_text = ""
            for pnum in pnum_list :
                ptext = texts[pnum-1]
                group_text = group_text + f"<page:{pnum}>\n{ptext.strip()}\n</page:{pnum}>\n"
            
            blob_client = self.container_client.get_blob_client(f"{base_path}/{g_name}.txt")
            blob_client.upload_blob(group_text, overwrite = True)
    
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

    def update_or_insert_data_contents(self, data_list):
        logger.info("###### Function Name : update_or_insert_data_contents")
        
        cursor = self.connection.cursor()
        data = data_list[0]
        try:
            cursor.execute('SELECT prc_flag, prc_status FROM tb_chat_content_prc WHERE contents_id = %s AND locale_cd = %s AND prod_group_cd = %s AND prod_cd = %s', 
                            (data['contents_id'], data['locale_cd'],data['prod_group_cd'],data['prod_cd']))
            result = cursor.fetchall()  # Ensuring that all results are fetched to avoid 'Unread result found' error.
            if result:
                logger.info(f"update")
                # Prepare update statements and data for each dictionary in the data list
                update_queries = []

                # Prepare update statements and data for each dictionary in the data list
                for data in data_list:
                    # Create the SET part of the update statement excluding keys used in the WHERE clause
                    update_set = ', '.join([f"{key} = %s" for key in data.keys() if key not in ['contents_id', 'locale_cd','prod_group_cd','prod_cd']])
                    update_params = [data[key] for key in data.keys() if key not in ['contents_id', 'locale_cd','prod_group_cd','prod_cd']]
                    update_params.extend([data['contents_id'], data['locale_cd'],data['prod_group_cd'],data['prod_cd']])

                    update_query = f"UPDATE tb_chat_content_prc SET {update_set} WHERE contents_id = %s AND locale_cd = %s AND prod_group_cd = %s AND prod_cd = %s"
                    update_queries.append((update_query, tuple(update_params)))

                # Execute all updates using executemany for better performance
                for query, params in update_queries:
                    cursor.execute(query, params)

                self.connection.commit()

            else:
                logger.info("insert")
                # Prepare for batch insertion if no results are found
                columns = ', '.join(data.keys())
                placeholders = ', '.join(['%s'] * len(data))

                # Data for insertion
                insert_data = [tuple(d.values()) for d in data_list]
                insert_query = f"INSERT INTO tb_chat_content_prc ({columns}) VALUES ({placeholders})"
                cursor.executemany(insert_query, insert_data)
                self.connection.commit()

        except Exception as e:
            logger.error(f"An error occurred: {e}")
            print(f"An error occurred: {e}")
            self.connection.rollback()
        finally:
            cursor.close()


    def update_or_insert_data_spec(self, data_list):
        logger.info("###### Function Name : update_or_insert_data_spec")
        
        cursor = self.connection.cursor()
        data = data_list[0]
        try:
            cursor.execute('SELECT prc_flag, prc_status FROM tb_chat_spec_prc WHERE sale_cd = %s AND locale_cd = %s AND prod_group_cd = %s AND prod_cd = %s', 
                            (data['SALE_CD'], data['LOCALE_CD'],data['PROD_GROUP_CD'],data['PROD_CD']))
            result = cursor.fetchall()  # Ensuring that all results are fetched to avoid 'Unread result found' error.
            if result:
                logger.info(f"update")
                # Prepare update statements and data for each dictionary in the data list
                update_queries = []

                # Prepare update statements and data for each dictionary in the data list
                for data in data_list:
                    # Create the SET part of the update statement excluding keys used in the WHERE clause
                    update_set = ', '.join([f"{key} = %s" for key in data.keys() if key not in ['SALE_CD', 'LOCALE_CD','PROD_GROUP_CD','PROD_CD']])
                    update_params = [data[key] for key in data.keys() if key not in ['SALE_CD', 'LOCALE_CD','PROD_GROUP_CD','PROD_CD']]
                    update_params.extend([data['SALE_CD'], data['LOCALE_CD'],data['PROD_GROUP_CD'],data['PROD_CD']])

                    update_query = f"UPDATE tb_chat_spec_prc SET {update_set} WHERE sale_cd = %s AND locale_cd = %s AND prod_group_cd = %s AND prod_cd = %s"
                    update_queries.append((update_query, tuple(update_params)))

                # Execute all updates using executemany for better performance
                for query, params in update_queries:
                    cursor.execute(query, params)

                self.connection.commit()

            else:
                logger.info("insert")
                # Prepare for batch insertion if no results are found
                columns = ', '.join(data.keys())
                placeholders = ', '.join(['%s'] * len(data))

                # Data for insertion
                insert_data = [tuple(d.values()) for d in data_list]
                insert_query = f"INSERT INTO tb_chat_spec_prc ({columns}) VALUES ({placeholders})"
                cursor.executemany(insert_query, insert_data)
                self.connection.commit()

        except Exception as e:
            logger.error(f"An error occurred: {e}")
            self.connection.rollback()
        finally:
            cursor.close()

    def update_or_insert_data_microsite(self, data_list):
        logger.info("###### Function Name : update_or_insert_data_microsite")
        
        cursor = self.connection.cursor()
        data = data_list[0]
        try:
            cursor.execute('SELECT PRC_FLAG, PRC_STATUS FROM tb_chat_microsite_prc WHERE MICRO_ID = %s AND CORP_CD = %s AND PROD_GROUP_CD = %s AND PROD_CD = %s AND LANGUAGE_CD = %s', 
                            (data['MICRO_ID'], data['CORP_CD'],data['PROD_GROUP_CD'],data['PROD_CD'],data['LANGUAGE_CD']))
            result = cursor.fetchall()  
            if result:
                logger.info(f"update")
                update_queries = []
    
                for data in data_list:
                    update_set = ', '.join([f"{key} = %s" for key in data.keys() if key not in ['MICRO_ID', 'CORP_CD','PROD_GROUP_CD','PROD_CD','LANGUAGE_CD']])
                    update_params = [data[key] for key in data.keys() if key not in ['MICRO_ID', 'CORP_CD','PROD_GROUP_CD','PROD_CD','LANGUAGE_CD']]
                    update_params.extend([data['MICRO_ID'], data['CORP_CD'],data['PROD_GROUP_CD'],data['PROD_CD'],data['LANGUAGE_CD']])
    
                    update_query = f"UPDATE tb_chat_microsite_prc SET {update_set} WHERE MICRO_ID = %s AND CORP_CD = %s AND PROD_GROUP_CD = %s AND PROD_CD = %s AND LANGUAGE_CD = %s"
                    update_queries.append((update_query, tuple(update_params)))
    
                # Execute all updates using executemany for better performance
                for query, params in update_queries:
                    cursor.execute(query, params)
    
                self.connection.commit()
    
            else:
                logger.info("insert")
                # Prepare for batch insertion if no results are found
                columns = ', '.join(data.keys())
                placeholders = ', '.join(['%s'] * len(data))
    
                # Data for insertion
                insert_data = [tuple(d.values()) for d in data_list]
                insert_query = f"INSERT INTO tb_chat_microsite_prc ({columns}) VALUES ({placeholders})"
                cursor.executemany(insert_query, insert_data)
                self.connection.commit()
    
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            self.connection.rollback()
        finally:
            cursor.close()

    def update_or_insert_data_microsite_general(self, data_list):
        logger.info("###### Function Name : update_or_insert_data_microsite")
        
        cursor = self.connection.cursor()
        data = data_list[0]
        try:
            cursor.execute('SELECT PRC_FLAG, PRC_STATUS FROM tb_chat_microsite_prc WHERE MICRO_ID = %s AND CORP_CD = %s AND LANGUAGE_CD = %s', 
                            (data['MICRO_ID'], data['CORP_CD'],data['LANGUAGE_CD']))
            result = cursor.fetchall()  
            if result:
                logger.info(f"update")
                update_queries = []
    
                for data in data_list:
                    update_set = ', '.join([f"{key} = %s" for key in data.keys() if key not in ['MICRO_ID', 'CORP_CD','LANGUAGE_CD']])
                    update_params = [data[key] for key in data.keys() if key not in ['MICRO_ID', 'CORP_CD','LANGUAGE_CD']]
                    update_params.extend([data['MICRO_ID'], data['CORP_CD'],data['LANGUAGE_CD']])
    
                    update_query = f"UPDATE tb_chat_microsite_prc SET {update_set} WHERE MICRO_ID = %s AND CORP_CD = %s AND LANGUAGE_CD = %s"
                    update_queries.append((update_query, tuple(update_params)))
    
                # Execute all updates using executemany for better performance
                for query, params in update_queries:
                    cursor.execute(query, params)
    
                self.connection.commit()
    
            else:
                logger.info("insert")
                # Prepare for batch insertion if no results are found
                columns = ', '.join(data.keys())
                placeholders = ', '.join(['%s'] * len(data))
    
                # Data for insertion
                insert_data = [tuple(d.values()) for d in data_list]
                insert_query = f"INSERT INTO tb_chat_microsite_prc ({columns}) VALUES ({placeholders})"
                cursor.executemany(insert_query, insert_data)
                self.connection.commit()
    
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            self.connection.rollback()
        finally:
            cursor.close()

    # [2024.04.25] 수정 - try exception 추가
    def load_table_as_df(self, query):
        logger.info("###### Function Name : load_table_as_df")
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns = col_names)
            return df
        except Exception as e :
            return logger.error(f"{str(e)}")
        finally:
            if cursor:
                cursor.close()
            if self.connection:
                self.connection.close()
                    
    # [2024.04.25] 수정 - try exception 추가
    def commit_query_w_vals(self, query, vals, many_tf=False):
        # INSERT UPDATE 등 수행
        logger.info("###### Function Name : commit_query_w_vals")
        try :
            cursor = self.connection.cursor()
            if many_tf:
                cursor.executemany(query, vals)
            else :
                cursor.execute(query, vals)
            self.connection.commit()
            logger.info(cursor.rowcount, "record(s)") # logging
        except Exception as e :
            logger.error("Error while connecting to MySQL", e)
        finally:
            if self.connection.is_connected():
                cursor.close()
                self.connection.close()
    
    # [2024.04.25] 수정 - try exception 추가
    def commit_query(self, query):
        # DELETE, UPDATE 등 수행
        logger.info("###### Function Name : commit_query")
        try :
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
            logger.info(cursor.rowcount, "record(s)") # logging
        except Exception as e :
            logger.error("Error while connecting to MySQL", e)
        finally:
            if self.connection.is_connected():
                cursor.close()
                self.connection.close()

    def query_executor(self, query):
        logger.info("###### Function Name : query_executor")
        cursor = self.connection.cursor()
 
        try:
            cursor.execute(query)
            self.connection.commit()
            print("Query executed successfully.")
        except Exception as e :
            logger.error(f"{str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
            if self.connection:
                self.connection.close()

    # [2024.04.24] temp_modify 추가
    # [2024.04.25] tb_corp_lan_map 테이블 사용시 USE_YN 추가
    def get_tar_language(self, temp_modify) :
        logger.info("###### Function Name : get_tar_language")
        query = f"""
                SELECT
                    A.*
                    , B.LANGUAGE_NAME
                FROM (
                    SELECT 
                        DISTINCT 
                            TRIM(CORP_CD) AS CORP_CD
                            , TRIM(LANGUAGE_CD) AS LANGUAGE_CD
                    FROM tb_corp_lan_map
                    WHERE 1=1
                        AND USE_YN = 'Y'
                ) A
                LEFT JOIN (
                    SELECT 
                        TRIM(CODE_CD)     AS LANGUAGE_CD
                        , TRIM(CODE_NAME) AS LANGUAGE_NAME
                    FROM tb_code_mst
                    WHERE 1=1
                        AND GROUP_CD = 'B00003'
                        AND USE_YN = 'Y'
                ) B
                ON 1=1
                    AND A.LANGUAGE_CD = B.LANGUAGE_CD
                ;
            """
        df = self.load_table_as_df(query)
        if temp_modify :
            df = df.replace({"CORP_CD":["LGESK", "LGECZ"]}, {"CORP_CD":"LGEPL"})
        df = df.groupby('CORP_CD')['LANGUAGE_NAME'].apply(list).reset_index(name='TARGET_LANGUAGE_NAMES')
        return df
    
    # [2024.04.25] 추가
    def get_subs_info(self, temp_modify):
        query = f"""
            SELECT
                A.CORP_CD
                , B.ISO_CD
                , A.LOCALE_CD
                , A.LANGUAGE_CD
                , C.LANGUAGE_NAME
            FROM (
                SELECT 
                    DISTINCT
                        TRIM(CORP_CD)       AS CORP_CD
                        , TRIM(LOCALE_CD)   AS LOCALE_CD
                        , TRIM(LANGUAGE_CD) AS LANGUAGE_CD
                FROM tb_corp_lan_map
                WHERE 1=1
                    AND USE_YN = 'Y'
            ) A
            LEFT JOIN (
                SELECT 
                    DISTINCT 
                    TRIM(CODE_CD)    AS ISO_CD
                    , TRIM(ATTRIBUTE3) AS LOCALE_CD
                    , TRIM(ATTRIBUTE4) AS LANGUAGE_CD
                FROM tb_code_mst A
                WHERE 1=1
                    AND GROUP_CD = 'B00004'
                    AND USE_YN = 'Y'
            ) B
            ON 1=1
                AND A.LOCALE_CD   = B.LOCALE_CD
                AND A.LANGUAGE_CD = B.LANGUAGE_CD
            LEFT JOIN (
                SELECT 
                    TRIM(CODE_CD)     AS LANGUAGE_CD
                    , TRIM(CODE_NAME) AS LANGUAGE_NAME
                FROM tb_code_mst
                WHERE 1=1
                    AND GROUP_CD = 'B00003'
                    AND USE_YN = 'Y'
            ) C
            ON 1=1
                AND A.LANGUAGE_CD = C.LANGUAGE_CD
        """
        df = self.load_table_as_df(query)
        if temp_modify:
            df = df.replace({"CORP_CD":["LGESK", "LGECZ"]}, {"CORP_CD":"LGEPL"})    
        return df
    
    def create_table(self, table, table_name, dtype_mapping=None, chunk_size=None) :
        engine = create_engine(f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}:3306/{self.database}')

        if dtype_mapping is not None :
            if chunk_size is not None :
                df_chunks = [table[i:i+chunk_size] for i in range(0, len(table), chunk_size)]
                for chunk in df_chunks:
                    chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False,  dtype=dtype_mapping)
                    print(chunk)
            else :
                table.to_sql(name=table_name, con=engine, if_exists='replace', index=False, dtype=dtype_mapping)
        else :
            if chunk_size is not None :
                df_chunks = [table[i:i+chunk_size] for i in range(0, len(table), chunk_size)]
                for chunk in df_chunks:
                    chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
                    print(chunk)
            else :
                table.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        
        return logger.info(f"{table_name} create complete")
