#!/usr/bin/python
# -*- coding: utf-8 -*-
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from collections import Counter
import logging.handlers
import redis
import time
import sys
import xlwt

class RedisMonitor(object):
    def __init__(self):
        self.rhost = ['192.168.3.191','192.168.3.191','192.168.3.191']
        self.rport = 6379
        self.max_con = 20
        self.rpassword = 'dachen$222'
        self.rdb = '1'

        self.formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
        self.handler1 = logging.StreamHandler()
        self.handler1.setFormatter(self.formatter)
        self.logger = logging.getLogger("logger")
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(self.handler1)

    #统计redis key数量
    def redis_count(self):
        count_totle = dict()
        for host in self.rhost:
            pool = redis.ConnectionPool(host=host, port=self.rport, max_connections=self.max_con,
                                        password=self.rpassword,db=self.rdb,decode_responses=True)
            ret = redis.Redis(connection_pool=pool)
            try:
                keys =ret.keys()
                count_dict = dict()

                for key in keys :
                    if str(key).split(':')[0] in ('POPC','HUMAN'):
                        count_dict[str(key).split(':')[0]] = ret.llen(key)
                    else:
                        if ':' in str(key):
                            if str(key).split(':')[0] in count_dict:
                                count_dict[str(key).split(':')[0]] += 1
                            else:
                                count_dict[str(key).split(':')[0]] = 1
                        else:
                            type=ret.type(key)
                            if type=='hash':
                                count_dict[key] = ret.hlen(key)
                            elif type=='set':
                                count_dict[key] = ret.scard(key)
                            elif type=='list':
                                count_dict[key] = ret.llen(key)
                            elif type=='zset':
                                count_dict[key] = ret.zcard(key)
                count_totle = dict(Counter(count_totle)+Counter(count_dict))
            except Exception as e:
                self.logger.info('查询失败:', e)
                sys.exit(1)
            finally:
                ret.close()
        return count_totle

    #创建EXCEL文件
    def create_excl(self,dict):
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
        sheet.write(0, 0, 'NAMESPACE', style)
        sheet.write(1, 0, 'VALUE', style)
        try:
            i=1
            for key,value in dict.items():
                sheet.write(0, i, key, style2)
                sheet.write(1, i, value, style2)
                i+=1
        except Exception as e:
            self.logger.info('建表失败:',e)
            sys.exit(1)
        book.save(time.strftime("%Y-%m-%d", time.localtime()) + ' REDIS数据校验.xls')
        filename = time.strftime("%Y-%m-%d", time.localtime()) + ' REDIS数据校验.xls'
        return filename

    #创建表格并发送邮件
    def mail(self,dic,excl):
        self.logger.info("开始发送邮件.....")
        mailserver = "smtp.163.com"
        username_send = 'data_monitor_dc@163.com'
        password = 'a123456'
        # username_recv = ['data_monitor_dc@163.com', 'yangzhiyuan0344@dingtalk.com', 'luolianyun6780@dingtalk.com',
        #                  'chenlg@dachentech.com.cn', 'wuzhaowen2019@dingtalk.com']
        username_recv = ['chenlg@dachentech.com.cn']
        username_cc = ['data_monitor_dc@163.com']
        #创建表格
        t1=''
        t2=''
        for key,value in dic.items():
            t1 += '<td><center>%s</center></td>'% key
            t2 += '<td><center>%s</center></td>'% value

        content = '<table width="1100" border="1" cellspacing="0" cellpadding="5" style="border-collapse:collapse;">' \
                  '<tr><td style="background:#BEBEBE; color:#000"><center><b>NAMESPACE</b></center></td>' + t1 +'</tr>' \
                  '<tr><td style="background:#BEBEBE; color:#000"><center><b>VALUE</b></center></td>' + t2 + '</tr>' \
                  '</table>'
        mail = MIMEMultipart()
        text = MIMEText(content, "html", "utf-8")
        mail.attach(text)
        # 添加附件
        xlsFile = excl
        xlsApart = MIMEApplication(open(xlsFile, 'rb').read())
        xlsApart.add_header('Content-Disposition', 'attachment', filename=xlsFile)
        mail.attach(xlsApart)
        mail['Subject'] = time.strftime("%Y-%m-%d", time.localtime()) + ' REDIS数据校验....'
        mail['From'] = username_send
        mail['To'] = ','.join(username_recv)
        mail['Cc'] = ','.join(username_cc)
        smtp = smtplib.SMTP_SSL(mailserver, port=465)  # 连接邮箱服务器，smtp的端口号是25
        try:
            # smtp=smtplib.SMTP_SSL('smtp.qq.com',port=465) #QQ邮箱的服务器和端口号
            smtp.login(username_send, password)
            smtp.sendmail(username_send, username_recv + username_recv,
                          mail.as_string())  # 参数分别是发送者，接收者，第三个是把上面的发送邮件的内容变成字符串
            self.logger.info("发送成功.....")
        except Exception:
            self.logger.info("发送失败.....")
            sys.exit(1)
        finally:
            smtp.quit()


if __name__ == '__main__':
    rm = RedisMonitor()
    dic = rm.redis_count()
    fn = rm.create_excl(dic)
    rm.mail(dic,fn)
