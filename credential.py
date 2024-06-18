from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential
import logging

KVUri = f"https://lge-ai-dev-kv.vault.azure.net"

credential = ClientSecretCredential(tenant_id='5069cde4-642a-45c0-8094-d0c2dec10be3',client_id='641926ec-ef4e-40d3-87bd-1256d35fbd74',client_secret='n8I8Q~mGtUTnjw7-6AdDxvFpSR-hSmf6--6sIb4b')
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
                    "storage-log-account-key",
                    "storage-log-account-name",
                    "storage-log-account-str",
                    "translator-account-key",
                    "translator-endpoint",
                    "translator-region",
                    "openai-api-key",
                    "openai-api-base",
                    "openai-api-version",
                    "Authorization"]   
         
    for secret_name in secret_names:
        secret = client.get_secret(secret_name)
        keyvault_values[secret_name] = secret.value

fetch_keyvault_values()