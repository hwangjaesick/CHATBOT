import os, datetime, json, io, re
import logging
from azure.storage.blob import BlobClient
from azure.core.exceptions import ResourceExistsError
from azure.storage.fileshare import ShareFileClient
from pytz import timezone

DI_STORAGE_CONN_STRING = os.environ['DI_STORAGE_CONN_STRING']
DI_CONTAINER = os.environ['DI_CONTAINER']
DI_FILE_SHARE = os.environ['DI_FILE_SHARE']
DI_BLOB_ENDPOINT = os.environ['DI_BLOB_ENDPOINT']
DI_FILE_ENDPOINT = os.environ['DI_FILE_ENDPOINT']
KR_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# Databricks Storage
DB_STORAGE_CONN_STRING = os.environ['DB_STORAGE_CONN_STRING']
DB_CONTAINER = os.environ['DB_CONTAINER']
DB_STREAMING_PATH = os.environ['DB_STREAMING_PATH']


# Event trigger의 event 정보 보여주고
# Blob의 URL 반환
def show_event_info(event):

    # Event 정보 추출
    topic = event.topic
    subject = event.subject
    event_type = event.event_type
    event_time = event.event_time
    id = event.id
    
    # Event 내 Data 정보 추출
    data = event.get_json()
    data_version = event.data_version

    data_api = None
    data_client_req_id = None
    data_req_id = None
    data_etag = None
    data_cont_type = None
    data_cont_len = None
    data_blob_type = None
    data_url = None
    data_sequencer = None
    data_diag_batch_id = None
    event_keys = data.keys()
    
    if 'api' in event_keys:
        data_api = data['api']
    
    if 'clientRequestId' in event_keys:
        data_client_req_id = data['clientRequestId']
    
    if 'requestId' in event_keys:
        data_req_id = data['requestId']

    if 'eTag' in event_keys:
        data_etag = data['eTag']

    if 'contentType' in event_keys:
        data_cont_type = data['contentType']

    if 'contentLength' in event_keys:
        data_cont_len = data['contentLength']

    if 'blobType' in event_keys:
        data_blob_type = data['blobType']

    if 'url' in event_keys:
        data_url = data['url']

    if 'sequencer' in event_keys:
        data_sequencer = data['sequencer']

    if 'storageDiagnostics' in event_keys:
        data_diag_batch_id = data['storageDiagnostics']['batchId']

    logging.info('========= Event trigger start =========')
    logging.info('===== Trigger Info ')
    logging.info('===== Event Type : {}'.format(event_type))
    logging.info('===== Event Time : {}'.format(event_time))
    logging.info('===== API : {}'.format(data_api))
    logging.info('===== Blob Size : {}{}'.format(round(data_cont_len/1024),'kb'))
    logging.info('===== Blob URL : {}'.format(data_url))

    return data_url

# Blob URL의 상대경로 가져오기
def get_blob_rel_url(data_url):

    # Azure storage endpoint 가져오기
    st_endpoint = get_storage_blob_endpoint()
    # Azure storage container명 가져오기
    container = get_container()
    # File의 full URL에서 endpoint와 컨테이너 정보를 제외한 나머지 sub URL 추출
    blob_name = data_url.replace(st_endpoint+'/'+container+'/',"")

    return blob_name


# Event Hub의 event 정보 보여주고
# File의 URL 반환
def show_eventhub_info(event):

    event_keys = event.keys()

    # Event 정보 추출

    event_cat = None
    event_oper = None
    event_time = None
    event_status_code = None
    event_status_text = None
    file_uri = None

    if 'category' in event_keys:
        event_cat = event['category']

    if 'operationName' in event_keys:
        event_oper = event['operationName']
    
    if 'time' in event_keys:
        event_time = event['time']
    
    if 'statusCode' in event_keys:
        event_status_code = event['statusCode']

    if 'statusText' in event_keys:
        event_status_text = event['statusText']

    if 'uri' in event_keys:
        file_uri = event['uri']

    logging.info('========= Event Hub trigger start =========')
    logging.info('===== Trigger Info ')
    logging.info('===== Event Category : {}'.format(event_cat))
    logging.info('===== Event Operation : {}'.format(event_oper))
    logging.info('===== Event Time : {}'.format(event_time))
    logging.info('===== Event Status : {}, {}'.format(event_status_code, event_status_text))
    logging.info('===== Blob URL : {}'.format(file_uri))

    return file_uri

# File URL의 상대경로 가져오기
def get_file_rel_url(data_url):

    # Azure storage file endpoint 가져오기
    st_endpoint = get_storage_file_endpoint()

    # file_share 가져오기
    file_share = get_file_share()

    # File의 full URL에서 endpoint와 File Share 정보를 제외한 나머지 sub URL 추출
    file_names = data_url.split('?')
    file_name = None
    if len(file_names) > 0:
        file_name = file_names[0]
        file_name = file_name.replace(st_endpoint,"")
        file_name = file_name.replace(file_share+'/',"")
        file_name = file_name.replace(':443/',"")
        
    return file_name       

def get_refined_file_url(file_url):

    refined_url = file_url.replace('//','/')
    refined_url = re.sub('^[/]','',refined_url)

    return refined_url

# File binary return
def get_file(file_name):

    file = ShareFileClient.from_connection_string(
            conn_str=DI_STORAGE_CONN_STRING
            , share_name=DI_FILE_SHARE
            , file_path=file_name)


    down_stream = file.download_file()
    data = down_stream.readall()

    return json.loads(data.decode('utf-8-sig'))

# Blob binary return
def get_blob(blob_name):

    blob = BlobClient.from_connection_string(
            conn_str=DI_STORAGE_CONN_STRING
            , container_name=DI_CONTAINER
            , blob_name=blob_name
            )

    stream = blob.download_blob()
    data = stream.readall()

    return data

def upload_blob(stream, file_name):

    try:
        blob = BlobClient.from_connection_string(
                conn_str=DB_STORAGE_CONN_STRING
                , container_name=DB_CONTAINER
                , blob_name=os.path.join(DB_STREAMING_PATH,'edu',file_name)
                )

        blob.upload_blob(stream, blob_type="BlockBlob")
    
    except ResourceExistsError:
        logging.info('===== DB Blob file already exists : {}'.format(file_name))

# Storage blob endpoint return
def get_storage_blob_endpoint():
    return DI_BLOB_ENDPOINT

# Container명 return
def get_container():
    return DI_CONTAINER

# Storage file endpoint return
def get_storage_file_endpoint():
    return DI_FILE_ENDPOINT

# Container명 return
def get_file_share():
    return DI_FILE_SHARE
'''
# Connect cosmosDB mongo database
def conn_mongo():
    logging.info('========= Connecting to cosmosDB mongo start =========')

    client = MongoClient(DI_COSMOS_MONGO_CONN_STRING)
    db = client[DI_COSMOS_MONGO_DB]

    try: 
        db.command("serverStatus")
    except Exception as e: 
        logging.error('========= Error occured when connecting to cosmosDB mongo =========');
        logging.error(e)
    else: 
        logging.info('========= Connecting to cosmosDB mongo completed =========')
        return db

# Seperate blob name and get info
def get_info_from_blob_name(blob_name):

    dir_codes = blob_name.split('/')

    # 서비스 ID, 자원 앱 ID, File명
    dc = ['service_id', 'rsrc_id', 'blob_url']
    # 사용자 ID, 진단 ID, 활동 ID, 자원 앱 시작시간 / 종료시간, 데이터 종류, 우울점수
    fc = ['member_id', 'diagApi_id', 'atvApi_id', 'rsrc_start_time'
            , 'rsrc_end_time', 'data_type', 'score']

    blob_info = {}
    file_name = None

    i = 0
    for d in dc:

        if i == 2:
            blob_info[d] = blob_name
            file_name = dir_codes[i]
        else:
            blob_info[d] = dir_codes[i]
        
        i = i + 1
    
    # 확장자 제거한 파일명
    file_name_only = os.path.splitext(file_name)[0]
    # 언더스코어 기준으로 split
    file_codes = file_name_only.split('_')

    i = 0
    for f in fc:

        if f == 'score':
            blob_info[f] = 0
        elif f == 'rsrc_end_time':
            if len(fc)-1 == len(file_codes):
                blob_info[f] = file_codes[i]
                i = i + 1
            else:
                blob_info[f] = None
        else:
            blob_info[f] = file_codes[i]
            i = i + 1
    
    blob_info['create_time'] = get_cur_local_time()

    return blob_info
'''
def get_cur_local_time():
    
    local_time = datetime.datetime.now(timezone('Asia/Seoul'))
    local_time_str = local_time.strftime(KR_TIME_FORMAT)

    return local_time_str

def get_time_diff(start_time, end_time):

    if start_time is not None and start_time != '' and end_time is not None and end_time != '':
        start_time_dt = datetime.datetime.strptime(start_time[:-4], '%Y-%m-%d %H:%M:%S')
        end_time_dt = datetime.datetime.strptime(end_time[:-4], '%Y-%m-%d %H:%M:%S')
        difference = end_time_dt - start_time_dt
        
        return difference.total_seconds()
    
    else:
        return None

def get_local_time_from_utc(str_utc_time):

    tzinfo = timezone('Asia/Seoul')

    str_dt_format = '%Y-%m-%d'

    if 'T' in str_utc_time:
        str_dt_format = str_dt_format+'T%H:%M:%S.%f'
    else:
        str_dt_format = str_dt_format+' %H:%M:%S.%f'

    if 'Z' in str_utc_time:
        str_dt_format = str_dt_format+'Z'

    utc_time = datetime.datetime.strptime(str_utc_time, str_dt_format)
    kr_time = tzinfo.fromutc(utc_time)
    str_kr_time = kr_time.strftime(KR_TIME_FORMAT+'.%f')

    return str_kr_time[:-3]