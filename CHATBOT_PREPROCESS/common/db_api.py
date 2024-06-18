import logging
import os, pymysql

def db_conn(logger, db_host, db_port, db_user, db_pw, db_name):
    try:

        cwd = os.getcwd()
        ssl_path = os.path.join(cwd,'common/DigiCertGlobalRootCA.crt.pem')
        isExist = os.path.exists(ssl_path)
        logger.info('==== SSL file path : {}, {}'.format(ssl_path, str(isExist)))

        conn = pymysql.connect(
            user=db_user,
            password=db_pw,
            host=db_host,
            port=db_port,
            database=db_name,
            charset='utf8mb4',
            ssl={'ssl_ca': os.path.join(cwd,'common/DigiCertGlobalRootCA.crt.pem')}
        )

        return conn

    except Exception as e:
        logger.error(f'==== Error connecting to DB: {e}')
        raise e 

def db_close(logger, conn):

    conn.close()

def select(logger, conn, sql, params=None):

    cur = conn.cursor(pymysql.cursors.DictCursor)

    cur.execute(sql, params)

    res_list = cur.fetchall()

    cur.close()

    return res_list

def insert(logger, conn, sql, params):

    cur = conn.cursor(pymysql.cursors.DictCursor)

    cur.executemany(sql, params)

    row_cnt = cur.rowcount

    conn.commit()

    cur.close()

    return row_cnt

def create_table(logger, conn, query):

    with conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()

def drop_table(logger, conn, table):

    with conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS `{table}`')
        conn.commit()

# 2024-06-03 수정
def call_proc(logger, conn, proc_name, p_type, p_start_date):
 
    try:
 
        with conn:
            with conn.cursor() as cur:                              
                cur.execute(f"CALL {proc_name}('{p_type}', '{p_start_date}')")
            
            conn.commit()
    except Exception as e:
 
        logger.error(f'==== Error call proc : {e}')
        raise e
