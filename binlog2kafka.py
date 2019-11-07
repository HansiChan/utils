# -*- coding:utf-8 -*-

import configparser
import datetime
import json
import logging.handlers
import signal
import sys
import time

import decimal
import pymysql
from kafka import KafkaProducer
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)

formatter = logging.Formatter("%(levelname)s %(message)s")
handler1 = logging.StreamHandler()
handler1.setFormatter(formatter)
logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)
logger.addHandler(handler1)


conf = configparser.ConfigParser()
# conf.read(r"D:\job_script\utils\config.ini")
conf.read("/data/job_pro/utils/config.ini")
HOST = conf.get('promysql', 'host')
PORT = int(conf.get('promysql', 'port'))
USERNAME = conf.get('promysql', 'user')
PASSWORD = conf.get('promysql', 'password')
KAFKA_HOSTS = conf.get('kafka', 'hosts')

MYSQL_HOSTS= conf.get('mysqldb','host')
MYSQL_USERNAME=conf.get('mysqldb','user')
MYSQL_PASSWORD=conf.get('mysqldb','password')
MYSQL_DB=conf.get('mysqldb','dbreport')

mysql_settings = {
    'host': HOST,
    'port': PORT,
    'user': USERNAME,
    'passwd': PASSWORD
}

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
    sql="select collection from blacklist_collection where source='mysql' and db='%s'" % (database)
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

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        elif isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        else:
            return json.JSONEncoder.default(self, obj)


if __name__ == '__main__':
    # database = sys.argv[1].split('.')[0]
    database = 'mysql'
    try:
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
        blacklist = getBlackList(database)
        stream = BinLogStreamReader(
            connection_settings=mysql_settings,
            server_id=100,  # slave标识，唯一
            blocking=True,  # 阻塞等待后续事件
            skip_to_timestamp=offset,  # 从offset开始消费
            ignored_tables=blacklist,  #忽略表
            # 设定只监控写操作：增、删、改
            only_events=[
                DeleteRowsEvent,
                UpdateRowsEvent,
                WriteRowsEvent
            ]
        )
        producer = KafkaProducer(bootstrap_servers = hosts_producer_arr)
        partition = producer.partitions_for(topic)
        numPartitions = len(partition)

        logger.info('*****************开始发送数据*****************')
        for binlogevent in stream:
            for row in binlogevent.rows:
                if len(row) > 80960:
                    logger.error('长度超限:' + row)
                event = {"schema": binlogevent.schema, "table": binlogevent.table}
                if isinstance(binlogevent, DeleteRowsEvent):
                    event["operationType"] = "delete"
                    event["data"] = row["values"]
                elif isinstance(binlogevent, UpdateRowsEvent):
                    event["operationType"] = "update"
                    event["data"] = row["after_values"]
                elif isinstance(binlogevent, WriteRowsEvent):
                    event["operationType"] = "insert"
                    event["data"] = row["values"]

                text=json.dumps(event,cls=MyEncoder,ensure_ascii=False)
                tb = binlogevent.schema + '.' + binlogevent.table
                i = abs(getHashCode(tb)) % numPartitions
                if 'data' in event and event['data'] != None:
                    msg_data = {}
                    full_doc = event['data']  # 将fullDocument里面的ky转小写
                    doc = key2lower(full_doc)
                    for k, v in event.items():
                        if k == 'data':
                            msg_data['data'] = doc
                        else:
                            msg_data[k] = v
                    msg_data = json.dumps(msg_data,cls=MyEncoder,ensure_ascii=False)
                    producer.send(topic, bytes(str(msg_data), encoding='utf8'), partition=i)
                    # print(msg_data)
                else:
                    producer.send(topic, bytes(str(text), encoding='utf8'), partition=i)
                    # print(text)

    except Exception as e :
        ts=int(time.time())-300
        setOffset(database,ts)
        logger.error(e)
        producer.close()
        sys.exit(1)