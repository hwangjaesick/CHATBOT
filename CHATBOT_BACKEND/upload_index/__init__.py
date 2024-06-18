import json  
import logging  
import azure.functions as func
import credential
import pandas as pd

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    if not credential.keyvault_values:
        credential.fetch_keyvault_values()
    
    from model import OpenAI
    from preprocessing import Preprocessing
    from database import AzureStorage, MySql
    
    openai_instance = OpenAI()
    preprocessing_instance = Preprocessing()
    sql_instance = MySql()
    as_instance = AzureStorage(container_name='documents', storage_type='docst')

    logging.info('Python HTTP trigger function processed a request.')  
  
    # Extract values from request payload  
    req_body = req.get_body().decode('utf-8')  
    logging.info(f"Request body: {req_body}")  
    request = json.loads(req_body)  
    values = request['values']  
    
    # url mapping
    sql_instance = MySql()
    url_table_data = sql_instance.get_table(query="""
    SELECT IF_UPDATE_DATE,CONTENTS_ID, LOCALE_CD, SHORT_URL
    FROM tb_if_content_url
    """)

    url_table_data = [dict(t) for t in {tuple(d.items()) for d in url_table_data}]
    
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
    locale_mst_table = pd.merge(locale_mst_table_1,locale_mst_table_2, left_on='LANGUAGE_CD', right_on='LANGUAGE_CD')

    # Process values and generate the response payload  
    response_values = []
    for value in values:
        url = value['data']['file_path']

        if "contents/" in url and "structured_1/" in url and url.endswith(".txt") : 
            type = "contents"
            response_index = preprocessing_instance.contents_to_index(type, as_instance, openai_instance, value, url_table_data, locale_mst_table)
            response_values.append(response_index)

        elif "manual/" in url and "structured_1/" in url and url.endswith('.json') :
            type = "manual"
            response_index = preprocessing_instance.manual_to_index(type, as_instance, openai_instance, value)
            response_values.append(response_index)


        elif "general-inquiry/" in url and url.endswith(".json") : 
            type = "general-inquiry"
            response_index = preprocessing_instance.general_inquiry_to_index(type, as_instance, openai_instance, value)
            response_values.append(response_index)

        elif "youtube/" in url and url.endswith(".json") : 
            type = "youtube"
            response_index = preprocessing_instance.youtube_to_index(type, as_instance, openai_instance, value)
            response_values.append(response_index)

        else :
            pass
        
    logging.info(f"Request body: {response_values}") 
    # Create the response object  
    response_body = {  
        "values": response_values  
    }  
    logging.info(f"Response body: {response_body}")  
  
    # Return the response  
    return func.HttpResponse(json.dumps(response_body), mimetype="application/json")  