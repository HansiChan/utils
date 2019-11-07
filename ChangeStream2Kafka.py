# -*- coding:utf-8 -*-
import random

import bson
import json
import pymongo
import pymysql
import sys
from bson.json_util import dumps
from kafka import KafkaProducer
import configparser
import time
import signal
import logging.handlers
formatter = logging.Formatter("%(levelname)s %(message)s")
handler1 = logging.StreamHandler()
handler1.setFormatter(formatter)
logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)
logger.addHandler(handler1)


conf = configparser.ConfigParser()
# conf.read(r"D:\job_script\utils\config.ini")
conf.read("/data/job_pro/utils/config.ini")
HOST = conf.get('mongo', 'host')
PORT = int(conf.get('mongo', 'port'))
USERNAME = conf.get('mongo', 'user')
PASSWORD = conf.get('mongo', 'password')
KAFKA_HOSTS = conf.get('kafka', 'hosts')

MYSQL_HOSTS= conf.get('mysqldb','host')
MYSQL_USERNAME=conf.get('mysqldb','user')
MYSQL_PASSWORD=conf.get('mysqldb','password')
MYSQL_DB=conf.get('mysqldb','dbreport')



def getTopic(database):
    conn_mysql = pymysql.connect(MYSQL_HOSTS,MYSQL_USERNAME,MYSQL_PASSWORD,MYSQL_DB)
    cur = conn_mysql.cursor()
    sql="select topic from t_change_stream_kafka where db='%s'" % (database)
    logger.info('getTpoic Sql: '+sql)
    cur.execute(sql)
    result = cur.fetchall()
    if len(result)>0:
        data = result[0][0].strip()
    else:
        data=None
    conn_mysql.close()
    return data

def setOffset(database,ts):
    conn_mysql = pymysql.connect(MYSQL_HOSTS,MYSQL_USERNAME,MYSQL_PASSWORD,MYSQL_DB)
    cur = conn_mysql.cursor()
    sql = "replace into t_change_stream_offset(db,offset) values('%s',%d)" % (database,ts)
    logger.info('update offset Sql : '+sql)
    cur.execute(sql)
    conn_mysql.commit()
    cur.close()

def getOffset(database):
    conn_mysql = pymysql.connect(MYSQL_HOSTS,MYSQL_USERNAME,MYSQL_PASSWORD,MYSQL_DB)
    cur = conn_mysql.cursor()
    sql = "select offset from t_change_stream_offset where db='%s'" % (database)
    logger.info('getOffset Sql:'+ sql)
    cur.execute(sql)
    result = cur.fetchall()
    if len(result)>0:
        data = result[0][0]
    else:
        data=None
    conn_mysql.close()
    return data

def term_sig_handler(signum, frame):
    logger.info('意外退出更新offset: %d,%s' % (signum,database))
    ts=int(time.time())-300
    setOffset(database,ts)
    sys.exit(1)

def convert_n_bytes(n, b):
    bits = b * 8
    return (n + 2 ** (bits - 1)) % 2 ** bits - 2 ** (bits - 1)

def convert_4_bytes(n):
    return convert_n_bytes(n, 4)

def getHashCode(s):
    h = 0
    n = len(s)
    for i, c in enumerate(s):
        h = h + ord(c) * 31 ** (n - 1 - i)
    return convert_4_bytes(h)

def key2lower(d):
    new = {}
    for k, v in d.items():
        if isinstance(v, dict):
            v = key2lower(v)
        new[k.lower()] = v
    return new

def getBlackList(database):
    conn_mysql = pymysql.connect(MYSQL_HOSTS,MYSQL_USERNAME,MYSQL_PASSWORD,MYSQL_DB)
    cur = conn_mysql.cursor()
    sql="select collection from blacklist_collection where source='mongo' and db='%s'" % (database)
    logger.info('getBlackList Sql: '+sql)
    cur.execute(sql)
    result = cur.fetchall()
    if len(result)>0:
        data=[]
        for coll in result:
            data.append(''.join(coll).strip())
    else:
        data=None
    conn_mysql.close()
    return data

if __name__ == '__main__':
    database = sys.argv[1].split('.')[0]
    # database = 'module'
    try:
        mongo_con = pymongo.MongoClient(host=HOST,port=PORT,username=USERNAME,password=PASSWORD)

        signal.signal(signal.SIGTERM, term_sig_handler)
        signal.signal(signal.SIGINT, term_sig_handler)
        hosts_producer_arr=[]
        if ',' in KAFKA_HOSTS:
           hostslist=KAFKA_HOSTS.split(',')
           for i in range(0,len(hostslist)):
              host=hostslist[i].strip()
              hosts_producer_arr.append(host)
        else:
            hosts_producer_arr.append(KAFKA_HOSTS)

        topic = getTopic(database)
        logger.info(database+'------------->'+str(topic))
        if topic==None:
            raise RuntimeError('Topic为空!')
        offset = getOffset(database)

        if offset==None:
            offset=int(time.time())-300
            setOffset(database,offset)

        logger.info(database+'------------->'+str(offset))
        mongo_db = mongo_con.get_database(database)
        # stream = mongo_db.watch(full_document = 'updateLookup',start_at_operation_time=bson.timestamp.Timestamp(int(offset),1))
        ##筛选collection,去除黑名单中的表
        blacklist=getBlackList(database)
        if blacklist:
            stream = mongo_db.watch([{'$match': {'ns.coll': {'$nin':blacklist}}}],
                                full_document = 'updateLookup',start_at_operation_time=bson.timestamp.Timestamp(int(offset),1))
        else:
            stream = mongo_db.watch(full_document='updateLookup',
                                    start_at_operation_time=bson.timestamp.Timestamp(int(offset), 1))

        producer = KafkaProducer(bootstrap_servers = hosts_producer_arr)
        partition = producer.partitions_for(topic)
        numPartitions = len(partition)

        logger.info('*****************开始发送数据*****************')
        for change in stream:
            msg =bytes(dumps(change,ensure_ascii=False),encoding='utf8')
            jsondata = str(msg,'utf-8')
            if len(msg)>80960:
                logger.error('长度超限:'+jsondata)
            text = json.loads(jsondata)
            tb = text['ns']['db']+'.'+text['ns']['coll']
            i = abs(getHashCode(tb)) %numPartitions
            if 'fullDocument' in text and text['fullDocument']!=None:
                msg_data = {}
                full_doc = text['fullDocument'] #将fullDocument里面的ky转小写
                doc = key2lower(full_doc)
                for k,v in text.items():
                    if k=='fullDocument':
                        msg_data['fullDocument']=doc
                    else:
                        msg_data[k]=v
                msg_data=json.dumps(msg_data)
                producer.send(topic,bytes(str(msg_data),encoding='utf8'),partition=i)
            else:
                producer.send(topic,bytes(str(json.dumps(text)),encoding='utf8'),partition=i)
    except Exception as e :
        ts=int(time.time())-300
        setOffset(database,ts)
        logger.error(e)
        producer.close()
        sys.exit(1)