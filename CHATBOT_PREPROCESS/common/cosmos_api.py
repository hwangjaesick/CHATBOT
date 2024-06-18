import os, logging
# from pymongo import MongoClient
from azure.cosmos import CosmosClient, PartitionKey
import azure.cosmos.exceptions as exceptions

def get_db(logger, url, key, db_name):

    try:

        client = CosmosClient(url, credential=key, logging_enable=False, enable_diagnostics_logging=False)
        
        db = client.create_database_if_not_exists(id=db_name)

        return db

    except Exception as e:
        logger.error(f'==================== Error connecting to CosmosDB: {e}')
        raise e 

def get_container(logger, db, con_name, partition_key=None):

    try:

        try:
            container = db.get_container_client(con_name)
            container.read()
            
            return container
        
        except exceptions.CosmosResourceNotFoundError:

            key_path = PartitionKey(path=partition_key)

            container = db.create_container_if_not_exists(
                id=con_name, partition_key=key_path, offer_throughput=400
            )
        
            return container

    except Exception as e:
        logger.error(f'==================== Error getting {con_name} container: {e}')
        raise e 

def upsert_item(logger, container, item, item_id, partition_key):

    try:
        read_item = container.read_item(item=item_id, partition_key=partition_key)
        logger.info(f'==================== Upsert item succesfully with {item_id}')

    except exceptions.CosmosResourceNotFoundError:
        response = container.upsert_item(body=item)
        logger.info(f'==================== Upsert item succesfully with {item_id}')

    except Exception as e:
        logger.error(f'==================== Error Upserting with {item_id}')
        raise e 


def create_item(logger, container, item_id, item):

    container.upsert_item(body=item)

    logger.info(f'===== Create item succesfully with {item_id}')

