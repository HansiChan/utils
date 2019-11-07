import datetime
import pymongo
import sys
import pyodbc
import pymysql
import configparser
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import logging.handlers
import xlwt
import time

formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
handler1 = logging.StreamHandler()
handler1.setFormatter(formatter)
logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)
logger.addHandler(handler1)

conf = configparser.ConfigParser()
# conf.read(r"D:\job_script\utils\config.ini")
conf.read("/data/job_pro/utils/config.ini")

HOST=conf.get('impaladb', 'host')
PORT=int(conf.get('impaladb', 'port'))
# cnxnstr = "DSN=Sample Cloudera Impala DSN;HOST=%s;PORT=%s;UID=hive;AuthMech=3;PWD=hive;UseSasl=0" % (HOST,PORT)
cnxnstr = "Driver={/opt/cloudera/impalaodbc/lib/64/libclouderaimpalaodbc64.so};HOST=%s;PORT=%s;UID=hive;AuthMech=3;PWD=hive;UseSasl=0" % (HOST,PORT)
conn = pyodbc.connect(cnxnstr, autocommit=True,timeout=240)
impala_cur = conn.cursor()

MYSQL_HOSTS= conf.get('mysqldb','host')
MYSQL_PORT=conf.get('mysqldb','port')
MYSQL_USERNAME=conf.get('mysqldb','user')
MYSQL_PASSWORD=conf.get('mysqldb','password')
MYSQL_DB=conf.get('mysqldb','dbreport')

MONGO_HOSTS=conf.get('mongo','host')
MONGO_PORT=conf.get('mongo','port')
MONGO_USERNAME=conf.get('mongo','user')
MONGO_PASSWORD=conf.get('mongo','password')

PROMYSQL_HOSTS= conf.get('promysql','host')
PROMYSQL_PORT=conf.get('promysql','port')
PROMYSQL_USERNAME=conf.get('promysql','user')
PROMYSQL_PASSWORD=conf.get('promysql','password')

# 导入mysql配置表
config_con = pymysql.connect(host=MYSQL_HOSTS, port=int(MYSQL_PORT), user=MYSQL_USERNAME, password=MYSQL_PASSWORD, db=MYSQL_DB)
config_cur = config_con.cursor()

def watch():
    try:
        d = ""
        i=1

        book = xlwt.Workbook(encoding='utf-8')
        sheet = book.add_sheet('sheet1', cell_overwrite_ok=True)
        style = xlwt.XFStyle()  # 初始化样式
        style2 = xlwt.XFStyle()

        font = xlwt.Font()  # 为样式创建字体
        font.name = 'Times New Roman'
        font.bold = True  # 黑体

        borders = xlwt.Borders()  # Create Borders
        borders.left = xlwt.Borders.THIN
        borders.right = xlwt.Borders.THIN
        borders.top = xlwt.Borders.THIN
        borders.bottom = xlwt.Borders.THIN

        pattern = xlwt.Pattern()
        pattern.pattern = xlwt.Pattern.SOLID_PATTERN
        pattern.pattern_fore_colour = 22  # 背景颜色

        style.font = font  # 设定样式
        style.pattern = pattern
        style.borders = borders
        style2.borders = borders

        sheet.col(0).width = 5000
        sheet.col(2).width = 12000
        sheet.col(4).width = 12000
        sheet.write(0, 0, '统计时间',style)
        sheet.write(0, 1, '源表类型',style)
        sheet.write(0, 2, '源表',style)
        sheet.write(0, 3, '源表总数',style)
        sheet.write(0, 4, '目标表',style)
        sheet.write(0, 5, '目标表总数',style)
        sheet.write(0, 6, '差值',style)

        sql = "select source_type,source,kudu,source_condition,kudu_condition,init_num from datawatch_config"
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
                print('source:'+source_db+'.'+source_collection+' '+source_condition+'--------->>'+'kudu_db:'+kudu_db+'.'+kudu_collection+' '+str(kudu_condition))
            else:
                print('source:'+source_db+'.'+source_collection+'--->>'+'kudu_db:'+kudu_db+'.'+kudu_collection)

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
                    sql = "select count(*) from %s %s" % (source_collection,source_condition)
                else:
                    sql = "select count(*) from %s" % (source_collection)
                mysql_cur.execute(sql)
                source_count = mysql_cur.fetchone()[0]
                mysql_con.close()
            # 统计kudu数据
            if kudu_condition:
                sql = 'select count(*) from %s.%s %s;' % (kudu_db, kudu_collection,kudu_condition)
            else:
                sql = 'select count(*) from %s.%s;' % (kudu_db, kudu_collection)
            rows = impala_cur.execute(sql)
            kudu_count = rows.fetchall()[0][0]
            date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if init_num == '' or init_num == None:
                difference = source_count - kudu_count
            else:
                difference = source_count - kudu_count-int(init_num)
            # 如果不一致,增加一行
            sheet.write(i,0,date,style2)
            sheet.write(i,1,source_type.upper(),style2)
            sheet.write(i,2,source_db+'.'+source_collection,style2)
            sheet.write(i,3,source_count,style2)
            sheet.write(i,4,kudu_db+'.'+kudu_collection,style2)
            sheet.write(i,5,kudu_count,style2)
            sheet.write(i,6,difference,style2)
            i+=1
            d = d + '<tr>' \
                    '<td><center>%s</center></td>' \
                    '<td><center>%s</center></td>' \
                    '<td><center>%s.%s</center></td>' \
                    '<td><center>%s</center></td>' \
                    '<td><center>%s.%s</center></td>' \
                    '<td><center>%s</center></td>' \
                    '<td><font color="#FF0000"><center>%s</center><font></td>' \
                    '</tr>' % (date,source_type.upper(),source_db,source_collection,
                               source_count,kudu_db,kudu_collection,kudu_count,difference)
        book.save(time.strftime("%Y-%m-%d", time.localtime())+'kudu表数据校验.xls')
    except Exception as  e :
        print(e)
        sys.exit(1)

    # 如果表不为空,发送邮件
    if d != "":
        mailserver = "smtp.163.com"
        username_send = 'kuduconfig@163.com'
        password = 'a123456'
        username_recv = ['kuduconfig@163.com', 'yangzhiyuan0344@dingtalk.com','luolianyun6780@dingtalk.com','chenlg@dachentech.com.cn','wuzhaowen2019@dingtalk.com']
        # username_recv = ['chenlg@dachentech.com.cn']
        username_cc = ['kuduconfig@163.com']
        content = '<table width="1100" border="1" cellspacing="0" cellpadding="5" style="border-collapse:collapse;">' \
                  '<tr>' \
                  '<td style="background:#BEBEBE; color:#000"><center><b>统计时间</b></center></td>' \
                  '<td style="background:#BEBEBE; color:#000"><center><b>源表类型</b></center></td>' \
                  '<td style="background:#BEBEBE; color:#000" width="300"><center><b>源表</b></center></td>' \
                  '<td style="background:#BEBEBE; color:#000"><center><b>源表总数</b></center></td>' \
                  '<td style="background:#BEBEBE; color:#000" width="300"><center><b>目标表</b></center></td>' \
                  '<td style="background:#BEBEBE; color:#000"><center><b>目标表总数</b></center></td>' \
                  '<td style="background:#BEBEBE; color:#000"><center><b>差值</b></center></td>' \
                  '</tr>' + d + \
                  '</table>'
        mail = MIMEMultipart()
        text = MIMEText(content, "html", "utf-8")
        mail.attach(text)
        # 添加附件
        xlsFile = time.strftime("%Y-%m-%d", time.localtime())+'kudu表数据校验.xls'
        xlsApart = MIMEApplication(open(xlsFile, 'rb').read())
        xlsApart.add_header('Content-Disposition', 'attachment', filename=xlsFile)
        mail.attach(xlsApart)
        mail['Subject'] = time.strftime("%Y-%m-%d", time.localtime())+'kudu表数据校验....'
        mail['From'] = username_send
        mail['To'] = ','.join(username_recv)
        mail['Cc'] = ','.join(username_cc)
        try:
            smtp = smtplib.SMTP(mailserver, port=25)  # 连接邮箱服务器，smtp的端口号是25
            # smtp=smtplib.SMTP_SSL('smtp.qq.com',port=465) #QQ邮箱的服务器和端口号
            smtp.login(username_send, password)
            smtp.sendmail(username_send, username_recv + username_recv, mail.as_string())  # 参数分别是发送者，接收者，第三个是把上面的发送邮件的内容变成字符串
            logger.info("发送成功.....")
        except smtp.SMTPException:
            logger.info("发送失败.....")
            sys.exit(1)
        finally:
            smtp.quit()
    # print(results)
    config_con.close()

if __name__ == '__main__':
    watch()

