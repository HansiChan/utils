#!/usr/bin/python
# -*- coding: utf-8 -*-
from collections import Counter
import logging.handlers
import redis
import time
import sys
from dingtalkchatbot.chatbot import DingtalkChatbot


class RedisMonitor(object):
    def __init__(self):
        self.rhost = ['172.16.10.22', '172.16.10.23', '172.16.10.24']
        # self.rhost = ['192.168.3.191']
        self.rport = 6390
        # self.rport = 6379
        self.rpassword = 'dachen$1111'
        # self.rpassword = 'dachen$222'
        self.max_con = 20
        self.rdb = '1'

        self.formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
        self.handler1 = logging.StreamHandler()
        self.handler1.setFormatter(self.formatter)
        self.logger = logging.getLogger("logger")
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(self.handler1)

    # 统计redis key数量
    def redis_count(self):
        count_totle = dict()
        for host in self.rhost:
            pool = redis.ConnectionPool(host=host, port=self.rport, max_connections=self.max_con,
                                        password=self.rpassword, db=self.rdb, decode_responses=True)
            ret = redis.Redis(connection_pool=pool)
            try:
                keys = ret.keys()
                count_dict = dict()

                for key in keys:
                    if str(key).split(':')[0] in ('POPC', 'HUMAN'):
                        count_dict[str(key).split(':')[0]] = ret.llen(key)
                    else:
                        if ':' in str(key):
                            if str(key).split(':')[0] in count_dict:
                                count_dict[str(key).split(':')[0]] += 1
                            else:
                                count_dict[str(key).split(':')[0]] = 1
                        else:
                            type = ret.type(key)
                            if type == 'hash':
                                count_dict[key] = ret.hlen(key)
                            elif type == 'set':
                                count_dict[key] = ret.scard(key)
                            elif type == 'list':
                                count_dict[key] = ret.llen(key)
                            elif type == 'zset':
                                count_dict[key] = ret.zcard(key)
                count_totle = dict(Counter(count_totle) + Counter(count_dict))
                print(count_totle)
            except Exception as e:
                self.logger.info('查询失败:', e)
                sys.exit(1)
            finally:
                print('close....')
                # ret.close()
        return count_totle

    # 创建表格并发送钉钉
    def dingding(self, dic):
        # WebHook地址
        webhook = 'https://oapi.dingtalk.com/robot/send?access_token=e5cc9f86eb85e5c9caef97638c6b92c36e09123c541f77289a7840071146a463'
        # 初始化机器人小丁
        dingding = DingtalkChatbot(webhook)

        self.logger.info("开始发送钉钉.....")
        dic = dict(sorted(dic.items(), key=lambda x: x[0]))
        content = '[' + time.strftime('%Y-%m-%d', time.localtime()) + '日]Redis数据',

        data = ''
        for k,v in dic.items():
            data += "> **" + str(k)+"**  :  " +"<font color=#FF0000>" + str(v) + "</font> \n\n"

        try:
            dingding.send_markdown(title='Redis数据',
                                   text='# ' + str(content[0]) + '\n\n' +
                                        data,
                                   )
            self.logger.info("发送成功.....")
        except Exception as e:
            self.logger.info("发送失败.....")
            self.logger.info("失败原因：" + str(e))
            sys.exit(1)


if __name__ == '__main__':
    rm = RedisMonitor()
    dic = rm.redis_count()
    rm.dingding(dic)
