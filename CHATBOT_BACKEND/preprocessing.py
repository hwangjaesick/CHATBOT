import pandas as pd
from database import AzureStorage, MySql
from datetime import datetime
from bs4 import BeautifulSoup
import yaml
import json
import logging
import tiktoken
from tools import extract_data, remove_tag_between, chunked_texts, process_locale

import io
import fitz

logger = logging.getLogger(__name__)

class Preprocessing():
    
    def contents_to_storage(self, corporation_name, language_names, user_type = "C", status_name="Approved", use_yn='N'):
       
        logger.info("###### Function Name : contents_to_storage")
        sql_instance = MySql()
        CONTENTS_META_LIST = sql_instance.get_table("""
                                                SELECT *
                                                FROM tb_if_content_mst
                                                """)
        CONTENTS_META_LIST = pd.DataFrame(CONTENTS_META_LIST)
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
        LOCALE_MST = pd.merge(locale_mst_table_1,locale_mst_table_2, left_on='LANGUAGE_CD', right_on='LANGUAGE_CD')

        sql_instance = MySql()
        locale_code_mst = sql_instance.get_table("""
                                                SELECT CONTENTS_ID, LOCALE_CD
                                                FROM tb_if_content_meta
                                                """)

        as_instance_raw = AzureStorage(container_name='raw-data', storage_type= 'datast')
        as_instance_preprocessed = AzureStorage(container_name='preprocessed-data', storage_type= 'datast')
        as_instance_preprocessed_docst = AzureStorage(container_name='documents', storage_type='docst')

        CONTENTS_HTML_LIST = as_instance_raw.read_file(file_path='Contents_Manual_List_Mst_Data/applied_contents_html_json/GSCS_CONTENTS_HTML_EU_LIST_20240129.json')
        CONTENTS_HTML_LIST = pd.read_json(CONTENTS_HTML_LIST)
        CONTENTS_META_LIST_FILTERED = CONTENTS_META_LIST[
                (CONTENTS_META_LIST['CORP_CD'].isin(corporation_name)) &
                (CONTENTS_META_LIST['LANGUAGE_CD'].isin(language_names)) &
                (CONTENTS_META_LIST['USER_TYPE'].str.contains(user_type)) &
                (pd.to_datetime(CONTENTS_META_LIST['END_DATE']) >= datetime.now()) &
                (CONTENTS_META_LIST['APPROVAL_STATUS'] == status_name) &
                (CONTENTS_META_LIST['USE_YN'] == use_yn)]

        json_contents_id_list = [item['contents_id'] for item in CONTENTS_HTML_LIST['rows']]

        sql_instance = MySql()
        short_url_mst = sql_instance.get_table("""
                                                SELECT CONTENTS_ID, LOCALE_CD, LANGUAGE_CD, SHORT_URL
                                                FROM tb_if_content_url
                                                """)
        short_url_mst = pd.DataFrame(short_url_mst)
        short_url_mst['LOCALE_CD'] = short_url_mst['LOCALE_CD'].apply(lambda x: x.lower())
        sql_instance = MySql()
        language_mst = sql_instance.get_table(  """
                                                SELECT m.CODE_CD,
                                                    m.CODE_NAME
                                                FROM tb_code_mst m,
                                                    tb_group_code_mst g
                                                WHERE 1=1
                                                    AND m.group_cd = g.group_cd
                                                    AND g.GROUP_NAME  = 'Language'
                                                """ )
        
        for index, row in CONTENTS_META_LIST_FILTERED.iterrows():
            
            contents_id = str(row['CONTENTS_ID'])
            corporation_name = str(row['CORP_CD'])
            language_name = str(row['LANGUAGE_CD'])
            language_code = LOCALE_MST[LOCALE_MST['CODE_NAME']=='German']['LANGUAGE_CD'].drop_duplicates().values[0]
            locale_code_list = list(set([item['LOCALE_CD'] for item in locale_code_mst if item['CONTENTS_ID'] == contents_id]))
            matching_locale = LOCALE_MST[LOCALE_MST['LOCALE_CD'].isin(locale_code_list)]

            filter_url = short_url_mst[(short_url_mst['CONTENTS_ID'] == contents_id)&
                                (short_url_mst['LOCALE_CD'].isin(locale_code_list))&
                                (short_url_mst['LANGUAGE_CD'] == language_code)]['SHORT_URL']
            if len(filter_url) == 0:
                continue
            iso_cd_list = matching_locale['CODE_CD'].tolist()
            if contents_id in json_contents_id_list:
                for nation in iso_cd_list :
                    html_data = next(item['contents'] for item in CONTENTS_HTML_LIST['rows'] if item['contents_id'] == contents_id)
                    language = row['LANGUAGE_CD']
                    product_group_code = row['PROD_GROUP_CD']

                    product_code = row['PROD_CD']
                    product_code_list = product_code.split(',')
                    for product_code in product_code_list :
                        if product_code =='W/M' :
                            product_code ='W_M'
                        
                        file_name = contents_id
                        text_data = BeautifulSoup(html_data, 'html.parser').body.get_text(separator='\n', strip=True)
                        
                        html_blob_path = f"data/{nation}/{language}/contents/{product_group_code}/{product_code}/HTML/{file_name}.html"
                        text_blob_path = f"data/{nation}/{language}/contents/{product_group_code}/{product_code}/TEXT/{file_name}.txt"
                        json_blob_path = f"data/{nation}/{language}/contents/{product_group_code}/{product_code}/JSON/{file_name}.json"
                        
                        as_instance_raw.upload_file(text_data, file_path=text_blob_path, overwrite=True)
                        as_instance_raw.upload_file(html_data, file_path=html_blob_path, overwrite=True)

                        content = text_data
                        content_ = content.split("main_text : ")[-1]  
                        content_ = content_.replace('""','').replace(u"\xa0",u"").replace("\t","").replace("\n ","\n").replace(" \n","\n").replace("\n\n\n","\n").replace("\n\n","\n")

                        video_links = []
                        image_links = []
                        
                        content_ = remove_tag_between(content_, start="<img src=", end=">")

                        tokenizer = tiktoken.get_encoding("cl100k_base")
                        tokenizer = tiktoken.encoding_for_model("text-embedding-ada-002")
                        encoding_result = tokenizer.encode(content_)

                        if len(encoding_result) > 8000 :
                            data_type = "chunked_text"
                            chunked_list = chunked_texts(content_, tokenizer=tokenizer, max_tokens=7000, overlap_percentage=0.1)
                            for i, chunk in enumerate(chunked_list) :
                                
                                document_structured_1 = \
f"""
(title) {row["CONTENTS_NAME"]}

(keyword) {row["KEY_LIST"]}

(symptom) {row["SYMP_LEVEL1_NAME"]} - {row["SYMP_LEVEL2_NAME"]}

(content)
{chunk}
""".strip()
                                text_blob_path = f"data/{nation}/{language}/contents/{product_group_code}/{product_code}/TEXT/{file_name}_{i}.txt"
                                save_path = text_blob_path.replace('TEXT','structured_1') 
                                as_instance_preprocessed.upload_file(document_structured_1, file_path = save_path, overwrite=True)
                                as_instance_preprocessed_docst.upload_file(document_structured_1, file_path = save_path, overwrite=True)

                            image_video_list = extract_data(html_data, start="src=\"", end="\"" )

                            for link in image_video_list:
                                if 'image2play' in link:
                                    video_links.append(link)
                                elif 'gscs.lge.com' in link:
                                    image_links.append(link)

                            contains_image2play = any('image2play' in link for link in image_video_list)
                            contains_gscs_lge = any('gscs.lge.com' in link for link in image_video_list)

                            
                        elif len(encoding_result) <= 8000 and len(content_) < 20 :

                            image_video_list = extract_data(html_data, start="src=\"", end="\"" )

                            for link in image_video_list:
                                if 'image2play' in link:
                                    video_links.append(link)
                                elif 'gscs.lge.com' in link:
                                    image_links.append(link)

                            contains_image2play = any('image2play' in link for link in image_video_list)
                            contains_gscs_lge = any('gscs.lge.com' in link for link in image_video_list)

                            if contains_image2play and not contains_gscs_lge:
                                data_type = 'video'
                            elif contains_gscs_lge and not contains_image2play:
                                data_type = 'image'
                            elif contains_image2play and contains_gscs_lge:
                                data_type = 'image_video'
                        
                        else :
                            document_structured_1 = \
f"""
(title) {row["CONTENTS_NAME"]}

(keyword) {row["KEY_LIST"]}

(symptom) {row["SYMP_LEVEL1_NAME"]} - {row["SYMP_LEVEL2_NAME"]}

(content)
{content_}
""".strip()
                            save_path = text_blob_path.replace('TEXT','structured_1') 
                            as_instance_preprocessed.upload_file(document_structured_1, file_path = save_path, overwrite=True)
                            as_instance_preprocessed_docst.upload_file(document_structured_1, file_path = save_path, overwrite=True)
                            data_type = 'text'
                            image_video_list = extract_data(html_data, start="src=\"", end="\"" )

                            for link in image_video_list:
                                if 'image2play' in link:
                                    video_links.append(link)
                                elif 'gscs.lge.com' in link:
                                    image_links.append(link)

                            contains_image2play = any('image2play' in link for link in image_video_list)
                            contains_gscs_lge = any('gscs.lge.com' in link for link in image_video_list)

                        
                        links_data = {"contents_id" : contents_id,
                                        "data_type" : data_type,
                                        "video": video_links,
                                        "image" : image_links,
                                        "text" : content_,
                                        "content_length" : len(content_)}
                        
                        json_data = json.dumps(links_data, ensure_ascii=False, indent=4).encode('utf-8')  
                        as_instance_raw.upload_file(json_data, file_path=json_blob_path, overwrite=True)
                        logger.info(f"{links_data['contents_id']} 전처리 완료, 처리 유형 : {links_data['data_type']}, 경로 :{json_blob_path}")
        return logger.info("Contents data preprocessing completed")
    
    def general_inquiry_to_storage(self, locale_cd):
        logger.info("###### Function Name : general_inquiry_to_storage")
        as_instance_raw = AzureStorage(container_name='raw-data', storage_type='datast')
        as_instance_preprocessed = AzureStorage(container_name='preprocessed-data', storage_type='datast')
        as_instance_preprocessed_docst = AzureStorage(container_name='documents', storage_type='docst')
        PRODUCT_MST = as_instance_raw.read_file(file_path="Contents_Manual_List_Mst_Data/PRODUCT_MST.csv")

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
        LOCALE_MST = pd.merge(locale_mst_table_1,locale_mst_table_2, left_on='LANGUAGE_CD', right_on='LANGUAGE_CD')

        language = LOCALE_MST[LOCALE_MST['LOCALE_CD']=='ch_fr']['CODE_NAME'].values[0]
        nation = locale_cd.split('_')[0].upper()
        language_code = LOCALE_MST[LOCALE_MST['LOCALE_CD']=='ch_fr']['LANGUAGE_CD'].values[0].lower()
        
        product_list = (PRODUCT_MST['PROD_GROUP_CD'] +'/'+ PRODUCT_MST['PROD_CD']).drop_duplicates().tolist()
        sql_instance = MySql()
        parameters = (locale_cd,)
        INTENT_MST = sql_instance.get_table("""
                                            SELECT *
                                            FROM tb_chat_intent_mst
                                            WHERE USE_YN = 'Y' AND LOCALE_CD = %s
                                            """, parameters=parameters)
        for data in INTENT_MST :
            json_data = {"INTENT_NAME" : data['INTENT_NAME'],
                        "INTENT_CODE" : data['INTENT_CODE'],
                        "chatbot_response" : data['CHATBOT_RESPONSE'],
                        "related_link_name" : data['RELATED_LINK_NAME'],
                        "related_link_url" : data['RELATED_LINK_URL'],
                        "EVENT_CD" : data['EVENT_CD'],
                        "symptom" : data['SYMPTOM']}

            json_bytes = json.dumps(json_data).encode('utf-8')
            # 파일저장 임시 수정필요
            for product in product_list :
                as_instance_preprocessed.upload_file(json_bytes, file_path=f"data/{nation}/{language}/general-inquiry/{product}/{json_data['INTENT_CODE']}.json", overwrite=True)
                as_instance_preprocessed_docst.upload_file(json_bytes, file_path=f"data/{nation}/{language}/general-inquiry/{product}/{json_data['INTENT_CODE']}.json", overwrite=True)
                logger.info(f"data/{nation}/{language}/general-inquiry/{product}/{json_data['INTENT_CODE']}.json , upload complete")
                print(f"data/{nation}/{language}/general-inquiry/{product}/{json_data['INTENT_CODE']}.json , upload complete")
            else :
                pass
        
        return logger.info("upload complete")
    
    def contents_to_index(self, type, as_instance, openai_instance, value, url_table_data, locale_mst_table) :
        logger.info("###### Function Name : contents_to_index")
        url = value['data']['file_path']
        recordId = value['recordId']

        main_text_path = url.split('documents/')[1]

        main_text = as_instance.read_file(main_text_path)

        # ex) f"data/{nation}/{language}/contents/{product_group_code}/{product_code}/TEXT/{file_name}_{i}.txt"
        iso_cd = main_text_path.split('/')[1]
        language = main_text_path.split('/')[2]
        product_group_code = main_text_path.split('/')[4]
        product_code = main_text_path.split('/')[5]

        title = extract_data(main_text,start='(title)',end = '(keyword)')[0]
        data_id = main_text_path.split('/')[-1].split('.')[0].split('_')[0]

        if iso_cd == 'CH' :
            locale_cd = iso_cd + '_' +locale_mst_table[locale_mst_table['CODE_NAME'] == language]['LANGUAGE_CD'].iloc[0].upper()
        else :
            locale_cd = iso_cd

        url_list = [item for item in  url_table_data if (item['CONTENTS_ID'] == data_id)& (item['LOCALE_CD'] == locale_cd)]
        if len(url_list) == 0 :
            contents_url = ""
        else :
            sorted_data = sorted(url_list, key=lambda x: x['IF_UPDATE_DATE'], reverse=True)
            contents_url = sorted_data[0]['SHORT_URL']

        product_model_code = ""

        pages = ""
        try:
            chunk_num = main_text_path.split('/')[-1].split('.')[0].split('_')[1]
        except:
            chunk_num = "0"

        mapping_key = f"{type}_{iso_cd}_{language}_{product_group_code}_{product_code}_{data_id}"

        if len(main_text) == 0 :
            main_text = ' '

        main_text_vector = openai_instance.generate_embeddings(main_text)
        
        response_index = {"recordId": recordId,
                            "data": {
                                "type" : type,
                                "iso_cd" : iso_cd,
                                "language" : language,
                                "product_group_code" : product_group_code,
                                "product_code" : product_code,
                                "product_model_code" : product_model_code,
                                "data_id" : data_id,
                                "mapping_key" : mapping_key,
                                "chunk_num" : chunk_num,
                                "pages" : pages,
                                "url" : contents_url,
                                "title" : title,
                                "main_text_vector" : main_text_vector,
                            },  
                            "errors": None,  
                            "warnings": None}
        
        return response_index
    
    def manual_to_index(self, type, as_instance, openai_instance, value) :
        
        logger.info("###### Function Name : manual_to_index")
        url = value['data']['file_path']
        recordId = value['recordId']

        main_text_path = url.split('documents/')[1]
        data = json.load(as_instance.read_file(main_text_path))

        title = data['title']
        product_model_code = data['product_model_code']
        main_text = data['main_text']
        pages = data['pages']

        # ex) f"data/AT/de/manual/ACN/CRA/20150101632543/structured_1/"
        iso_cd = main_text_path.split('/')[1]
        language = main_text_path.split('/')[2]

        product_group_code = main_text_path.split('/')[4]
        product_code = main_text_path.split('/')[5]
        data_id = main_text_path.split('/')[6]

        file_name = main_text_path.split('/')[-1]
        chunk_num = file_name.split('_')[-1].split('.')[0]

        mapping_key = f"{type}_{iso_cd}_{language}_{product_group_code}_{product_code}_{data_id}"

        if len(main_text) == 0 :
            main_text = ' '
        
        main_text_vector = openai_instance.generate_embeddings(main_text)
        
        url_path = '/'.join(main_text_path.split('/')[:-2])
        url_path = url_path.replace('manual/','') + f'/html/{data_id}.html'

        response_index = {"recordId": recordId,
                            "data": {
                                "type" : type,
                                "iso_cd" : iso_cd,
                                "language" : language,
                                "product_group_code" : product_group_code,
                                "product_code" : product_code,
                                "product_model_code" : product_model_code,
                                "data_id" : data_id,
                                "mapping_key" : mapping_key,
                                "chunk_num" : chunk_num,
                                "pages" : pages,
                                "url" : url_path,
                                "title" : title,
                                "main_text_vector" : main_text_vector,
                            },  
                            "errors": None,  
                            "warnings": None}
        
        return response_index
    
    def general_inquiry_to_index(self, type, as_instance, openai_instance, value) :
        
        logger.info("###### Function Name : general_inquiry_to_index")
        url = value['data']['file_path']
        recordId = value['recordId']

        file_path = url.split('documents/')[1]

        general_inquiry = json.load(as_instance.read_file(file_path))

        INTENT_NAME = general_inquiry['INTENT_NAME']
        INTENT_CODE = general_inquiry['INTENT_CODE']
        chatbot_response = general_inquiry['chatbot_response']
        related_link_name = general_inquiry['related_link_name']
        related_link_url = general_inquiry['related_link_url']
        EVENT_CD = general_inquiry['EVENT_CD']
        symptom = general_inquiry['symptom']

        INTENT_NAME = "" if INTENT_NAME is None else INTENT_NAME
        INTENT_CODE = "" if INTENT_CODE is None else INTENT_CODE
        chatbot_response = "" if chatbot_response is None else chatbot_response
        related_link_name = "" if related_link_name is None else related_link_name
        related_link_url = "" if related_link_url is None else related_link_url
        EVENT_CD = "" if EVENT_CD is None else EVENT_CD
        symptom = "" if symptom is None else symptom

        iso_cd = file_path.split('/')[1]
        language = file_path.split('/')[2]

        product_group_code = file_path.split('/')[4]
        product_code = file_path.split('/')[5]

        mapping_key = f"{type}_{iso_cd}_{language}_{product_group_code}_{product_code}_{INTENT_CODE}"

        if len(chatbot_response) == 0 :
            chatbot_response = ' '
        
        main_text_vector = openai_instance.generate_embeddings(chatbot_response)
        
        response_index = {"recordId": recordId,
                    "data": {
                        "type" : type,
                        "iso_cd" : iso_cd,
                        "language" : language,
                        "product_group_code" : product_group_code,
                        "product_code" : product_code,
                        "product_model_code" : chatbot_response,
                        "data_id" : mapping_key,
                        "mapping_key" : mapping_key,
                        "chunk_num" : "",
                        "pages" : "",
                        "url" : related_link_url,
                        "title" : symptom,
                        "main_text_vector" : main_text_vector,
                    },  
                    "errors": None,  
                    "warnings": None}
        
        return response_index
    
    def youtube_to_index(self, type, as_instance, openai_instance, value) :
        
        logger.info("###### Function Name : youtube_to_index")
        url = value['data']['file_path']
        recordId = value['recordId']

        main_text_path = url.split('documents/')[1]

        data = json.load(as_instance.read_file(main_text_path))
        title = data['title']
        main_text = data['main_text']
        youtube_url = data['url']

        # ex) data/AT/German/youtube/ACN/PAC/
        iso_cd = main_text_path.split('/')[1]
        language = main_text_path.split('/')[2]

        product_group_code = main_text_path.split('/')[4]
        product_code = main_text_path.split('/')[5]

        data_id = main_text_path.split('/')[-1].split('.')[0]
        mapping_key = f"{type}_{iso_cd}_{language}_{product_group_code}_{product_code}_{data_id}"

        if len(main_text) == 0 :
            main_text = ' '
        
        main_text_vector = openai_instance.generate_embeddings(main_text)
        
        response_index = {"recordId": recordId,
                    "data": {
                        "type" : type,
                        "iso_cd" : iso_cd,
                        "language" : language,
                        "product_group_code" : product_group_code,
                        "product_code" : product_code,
                        "product_model_code" : "",
                        "data_id" : data_id,
                        "mapping_key" : mapping_key,
                        "chunk_num" : "0",
                        "pages" : "",
                        "url" : youtube_url,
                        "title" : title,
                        "main_text_vector" : main_text_vector,
                    },  
                    "errors": None,  
                    "warnings": None}
        
        return response_index
    
    def pdf_to_image_html(self, corporation_name, language_name) :

        logger.info("###### Function Name : pdf_to_image_html")
        as_instance_web = AzureStorage(container_name='$web', storage_type='docst')
        web_container_client = as_instance_web.container_client
        as_instance_raw = AzureStorage(container_name='raw-data', storage_type='datast')
        pdf_list = as_instance_raw.extract_blob_list(blob_path=f"manual_output_{corporation_name}_240207/{corporation_name}/{language_name}", file_type='pdf')
        product_to_check = as_instance_raw.read_file('Contents_Manual_List_Mst_Data/PRODUCT_MST.csv')
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
        LOCALE_MST = pd.merge(locale_mst_table_1,locale_mst_table_2, left_on='LANGUAGE_CD', right_on='LANGUAGE_CD')
        
        for file_path in pdf_list :
            pdf_data = as_instance_raw.read_file(file_path=file_path)
            language = file_path.split('/')[2]
            product_code = file_path.split('/')[3]
            product_group_code = product_to_check[product_to_check['PROD_CD'] == product_code]['PROD_GROUP_CD'].iloc[0].strip()
            item_id = file_path.split('/')[-1].split('.')[0]
            iso_cd_list = [item.upper() for item in LOCALE_MST[(LOCALE_MST['CORP_CD']==corporation_name)&(LOCALE_MST['CODE_NAME']==language_name)]['LOCALE_CD'].drop_duplicates().str.split('_').str[0].tolist()]
            try :
                doc = fitz.open(stream=pdf_data)
                
            except:
                logger.info(f"Broken PDF : {file_path}")
                continue
            logger.info(f"Converting pdf to image, PDF : {file_path}")
            
            for i, page in enumerate(doc):
                svg = page.get_svg_image(matrix=fitz.Identity)
                svg_bytes = svg.encode('utf-8')
                image_data = io.BytesIO(svg_bytes)
                for iso_cd in iso_cd_list :
                    save_path = f'data/{iso_cd}/{language}/{product_group_code}/{product_code}/{item_id}/images/{i+1}.svg'
                    image_data.seek(0)
                    as_instance_web.upload_file(image_data, file_path = save_path, content_type="image/svg+xml", overwrite=True)
            
            for iso_cd in iso_cd_list :
                img_url_list = []
                save_path = f'data/{iso_cd}/{language}/{product_group_code}/{product_code}/{item_id}/images/'
                image_list = as_instance_web.extract_blob_list(blob_path=save_path, file_type='svg')
                if len(image_list) == 0 :
                    logger.info(f"No Images : {save_path}")
                    continue
                else :
                    for image_path in image_list :
                        image_url = web_container_client.get_blob_client(image_path).url
                        img_url_list.append({"page" : image_path.split('/')[-1].split('.')[0],
                                            "img_url" : as_instance_web.get_sas_url(image_path,expiry_time=False)})
                    img_url_list = sorted(img_url_list, key=lambda x: int(x['page']))
            
                    html_content = "<html><body style='background-color: white;'>"
            
                    for url in img_url_list:
                        html_content += f'<div id="page{url["page"]}" style="text-align: center;"><img src="{url["img_url"]}" style="display: inline-block; margin: 10px auto;"></div>\n'
            
                    html_content += "</body></html>"
                    file_name = f"{item_id}.html"
                    blob_name = f'data/{iso_cd}/{language}/{product_group_code}/{product_code}/{item_id}/html/{file_name}'
                    logger.info(blob_name)
                    as_instance_web.upload_file(html_content, file_path=blob_name, overwrite=True, content_type=("text/html"))


        return logger.info("Successfully converted pdf to image, html")
    


    