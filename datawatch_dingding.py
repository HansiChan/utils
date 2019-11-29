import datetime
import pymongo
import sys
import pyodbc
import pymysql
import configparser
import logging.handlers
import time
from dingtalkchatbot.chatbot import DingtalkChatbot

formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
handler1 = logging.StreamHandler()
handler1.setFormatter(formatter)
logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)
logger.addHandler(handler1)

conf = configparser.ConfigParser()
# conf.read(r'D:\job_script\utils\config.ini')
conf.read('/data/job_pro/utils/config.ini')

HOST = conf.get('impaladb', 'host')
PORT = int(conf.get('impaladb', 'port'))
# cnxnstr = 'DSN=Sample Cloudera Impala DSN;HOST=%s;PORT=%s;UID=hive;AuthMech=3;PWD=hive;UseSasl=0' % (HOST, PORT)
cnxnstr = 'Driver={/opt/cloudera/impalaodbc/lib/64/libclouderaimpalaodbc64.so};HOST=%s;PORT=%s;UID=hive;AuthMech=3;PWD=hive;UseSasl=0' % (
    HOST, PORT)
conn = pyodbc.connect(cnxnstr, autocommit=True, timeout=240)
impala_cur = conn.cursor()

MYSQL_HOSTS = conf.get('mysqldb', 'host')
MYSQL_PORT = conf.get('mysqldb', 'port')
MYSQL_USERNAME = conf.get('mysqldb', 'user')
MYSQL_PASSWORD = conf.get('mysqldb', 'password')
MYSQL_DB = conf.get('mysqldb', 'dbreport')

MONGO_HOSTS = conf.get('mongo', 'host')
MONGO_PORT = conf.get('mongo', 'port')
MONGO_USERNAME = conf.get('mongo', 'user')
MONGO_PASSWORD = conf.get('mongo', 'password')

PROMYSQL_HOSTS = conf.get('promysql', 'host')
PROMYSQL_PORT = conf.get('promysql', 'port')
PROMYSQL_USERNAME = conf.get('promysql', 'user')
PROMYSQL_PASSWORD = conf.get('promysql', 'password')

# 导入mysql配置表
config_con = pymysql.connect(host=MYSQL_HOSTS, port=int(MYSQL_PORT), user=MYSQL_USERNAME, password=MYSQL_PASSWORD,
                             db=MYSQL_DB)
config_cur = config_con.cursor()

# WebHook地址
webhook = 'https://oapi.dingtalk.com/robot/send?access_token=e5cc9f86eb85e5c9caef97638c6b92c36e09123c541f77289a7840071146a463'
# 初始化机器人小丁
dingding = DingtalkChatbot(webhook)


def watch():
    try:
        d = ''
        dif_num = 0
        tb_list = []
        sql = 'select source_type,source,kudu,source_condition,kudu_condition,init_num from datawatch_config'
        config_cur.execute(sql)
        results = config_cur.fetchall()

        for row in results:
            source_type = row[0]
            source_db = row[1].split('.')[0]
            kudu_db = row[2].split('.')[0]
            source_collection = row[1].split('.')[1]
            kudu_collection = row[2].split('.')[1]

            source_condition = row[3]
            kudu_condition = row[4]
            init_num = row[5]
            if source_condition:
                logger.info(
                    'source:' + source_db + '.' + source_collection + ' ' + source_condition + '--------->>' + 'kudu_db:' + kudu_db + '.' + kudu_collection + ' ' + str(
                        kudu_condition))
            else:
                logger.info(
                    'source:' + source_db + '.' + source_collection + '--->>' + 'kudu_db:' + kudu_db + '.' + kudu_collection)

            # 统计来源数据
            if source_type == 'mongo':
                mongo_con = pymongo.MongoClient(host=MONGO_HOSTS, port=int(MONGO_PORT), username=MONGO_USERNAME,
                                                password=MONGO_PASSWORD)
                mongo_db = mongo_con.get_database(source_db)
                mongo_collection = mongo_db.get_collection(source_collection)
                if source_condition:
                    # source_count = mongo_collection.find(eval(source_condition)).count() # 方法过时
                    source_count = mongo_collection.count_documents(eval(source_condition))
                else:
                    source_count = mongo_collection.estimated_document_count()
                mongo_con.close()
            elif source_type == 'mysql':
                mysql_con = pymysql.connect(host=PROMYSQL_HOSTS, port=int(PROMYSQL_PORT), user=PROMYSQL_USERNAME,
                                            password=PROMYSQL_PASSWORD, db=source_db)
                mysql_cur = mysql_con.cursor()
                if source_condition:
                    sql = 'select count(*) from %s %s' % (source_collection, source_condition)
                else:
                    sql = 'select count(*) from %s' % (source_collection)
                mysql_cur.execute(sql)
                source_count = mysql_cur.fetchone()[0]
                mysql_con.close()
            # 统计kudu数据
            if kudu_condition:
                sql = 'select count(*) from %s.%s %s;' % (kudu_db, kudu_collection, kudu_condition)
            else:
                sql = 'select count(*) from %s.%s;' % (kudu_db, kudu_collection)
            rows = impala_cur.execute(sql)
            kudu_count = rows.fetchall()[0][0]
            date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if init_num == '' or init_num == None:
                difference = source_count - kudu_count
            else:
                difference = source_count - kudu_count - int(init_num)
            if difference != 0: dif_num += 1
            tb_list.append([date, source_type.upper(), source_db, source_collection,
                            source_count, kudu_db, kudu_collection, kudu_count, abs(difference)])

        tb_list.sort(key=lambda x: x[8], reverse=True)
        for i in tb_list:
            d = d + '<tr>' \
                    '<td><center>%s</center></td>' \
                    '<td><center>%s</center></td>' \
                    '<td><center>%s.%s</center></td>' \
                    '<td><center>%s</center></td>' \
                    '<td><center>%s.%s</center></td>' \
                    '<td><center>%s</center></td>' \
                    '<td><font color="#FF0000"><center>%s</center><font></td>' \
                    '</tr>' % (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8])
    except Exception as  e:
        logger.info(e)
        sys.exit(1)

    # 如果表不为空,发送消息
    if d != '':
        content = '<table width="1800" border="1" cellspacing="0" cellpadding="5" style="border-collapse:collapse;">' \
                  '<tr>' \
                  '<td style="background:#BEBEBE; color:#000" width="200"><center><b>统计时间</b></center></td>' \
                  '<td style="background:#BEBEBE; color:#000"><center><b>源表类型</b></center></td>' \
                  '<td style="background:#BEBEBE; color:#000" width="300"><center><b>源表</b></center></td>' \
                  '<td style="background:#BEBEBE; color:#000"><center><b>源表总数</b></center></td>' \
                  '<td style="background:#BEBEBE; color:#000" width="300"><center><b>目标表</b></center></td>' \
                  '<td style="background:#BEBEBE; color:#000"><center><b>目标表总数</b></center></td>' \
                  '<td style="background:#BEBEBE; color:#000"><center><b>差值</b></center></td>' \
                  '</tr>' + d + \
                  '</table>'
        # GEN_HTML = 'D://' + time.strftime('%Y-%m-%d', time.localtime()) +'_KuduMonitor.html'
        GEN_HTML = '/var/www/html/' + time.strftime('%Y-%m-%d', time.localtime()) + '_KuduMonitor.html'
        f = open(GEN_HTML, 'w')

        message = """
        <html>
        <head></head>
        <body>
        <p>
        <center>%s</center>
        </p>
        </body>
        </html>""" % content

        f.write(message)
        f.close()

        try:
            dingding.send_link(title=time.strftime('%Y-%m-%d', time.localtime()) + '日Kudu表数据校验',
                               text='数据不一致共 ' + str(dif_num) + ' 个表',
                               message_url='http://uat1/' + time.strftime('%Y-%m-%d',
                                                                          time.localtime()) + '_KuduMonitor.html',
                               pic_url='https://uat1/WARNING.jpg'
                               )
            logger.info('发送成功.....')
        except Exception:
            logger.info('发送失败.....')
            sys.exit(1)

    # print(results)
    config_con.close()


if __name__ == '__main__':
    watch()
