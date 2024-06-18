from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential
import logging

KVUri = {KVUri}

credential = ClientSecretCredential(tenant_id={tenant_id},client_id={client_id},client_secret={client_secret})
client = SecretClient(vault_url=KVUri, credential=credential)

keyvault_values = {}
logger = logging.getLogger(__name__)
def fetch_keyvault_values():
    logger.info("###### Function Name : fetch_keyvault_values")
    global keyvault_values
    secret_names = ["aisearch-account-key",
                    "aisearch-account-name",
                    "aisearch-endpoint",
                    "cosmosdb-key",
                    "cosmosdb-url",
                    "mysql-host",
                    "mysql-password",
                    "mysql-user",
                    "storage-doc-account-key",
                    "storage-doc-account-name",
                    "storage-doc-account-str",
                    "storage-raw-account-key",
                    "storage-raw-account-name",
                    "storage-raw-account-str",
                    "translator-account-key",
                    "translator-endpoint",
                    "translator-region",
                    "openai-api-key",
                    "openai-api-base",
                    "openai-api-version",
                    #---- [2024.04.25] 매뉴얼 섹션 분할용 GPT 리소스
                    "openai-api-key-m1",
                    "openai-api-base-m1",
                    "openai-api-version-m1",
                    "openai-api-key-m2",
                    "openai-api-base-m2",
                    "openai-api-version-m2",
                ]   
         
    for secret_name in secret_names:
        secret = client.get_secret(secret_name)
        keyvault_values[secret_name] = secret.value

fetch_keyvault_values()
