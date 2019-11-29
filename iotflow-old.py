import datetime
import math
import re
import time

import pyodbc
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys

class LLSpider(object):

    def __init__(self):
        self.options = Options()
        # self.options.add_argument('--headless')  # 爬取时隐藏浏览器
        self.options.add_argument('--no-sandbox')  # 禁用沙盒模式（linux下适用）
        self.driver = webdriver.Chrome(options=self.options)
        # self.driver = webdriver.Chrome()
        self.driver.implicitly_wait(30)
        self.verificationErrors = []
        self.accept_next_alert = True

        self.username = 'xuanguanjiankang'
        self.password = '666666'

        cnxnstr = "DSN=Sample Cloudera Impala DSN;HOST=192.168.3.121;PORT=21050;UID=hive;AuthMech=3;PWD=hive;UseSasl=0"
        # cnxnstr = "Driver={/opt/cloudera/impalaodbc/lib/64/libclouderaimpalaodbc64.so};HOST=192.168.3.121;PORT=21050;UID=hive;AuthMech=3;PWD=hive;UseSasl=0"
        conn = pyodbc.connect(cnxnstr, autocommit=True, timeout=240)
        self.impala_cur = conn.cursor()

    # 模拟登陆
    def fake_login(self):
        url = 'http://tew.yrt-tech.com/#/login'
        self.driver.get(url)
        self.driver.find_element_by_xpath('//input[@placeholder="账号"]').send_keys(self.username)
        self.driver.find_element_by_xpath('//input[@placeholder="密码"]').send_keys(self.password)
        self.driver.find_element_by_xpath('//button').click()

    def get_data(self):
        self.fake_login()
        time.sleep(1)
        url = 'http://tew.yrt-tech.com/#/stats/query-sim-flow-agent'
        self.driver.get(url)
        time.sleep(1)
        # dateList = self.get_dateList() # 跑历史数据
        dateList = self.get_dateList(str(datetime.date.today()+ datetime.timedelta(-1)))
        self.driver.find_element_by_xpath('//div[@aria-controls="pane-flowDay"]').click()
        time.sleep(1)
        for dt in dateList:
            self.driver.find_element_by_xpath('//input[@placeholder="选择日期时间"]').clear()
            self.driver.find_element_by_xpath('//input[@placeholder="选择日期时间"]').send_keys(dt)
            time.sleep(5)
            self.driver.find_element_by_xpath('//i[@class="el-icon-search"]/..').click()
            data = self.driver.find_element_by_xpath('//span[@class="el-pagination__total"]')
            total = ''.join(re.findall(r'\d+', data.text))

            print('===== ' + dt + ' 共 ' + total + ' 条数据 =====')
            pages = math.ceil(int(total) / 30)
            for p in range(2, pages + 2):
                time.sleep(2)
                table = self.driver.find_element_by_xpath('//table[@class="el-table__body"]')
                data = table.find_elements_by_xpath('.//tr')
                for d in data:
                    row = d.text.split('\n')
                    print(row)
                    id = "'"+str(row[1])+"'"
                    # if id == '8986031947208115248':
                    operator = row[2]
                    statu = row[3]
                    flow = self.convert(row[4])
                    date = "'" + row[5] + "'"

                    sql = 'upsert into dw.dw_esy_iotflow values(%s,%s,%s,concat(cast(unix_timestamp() as string),"000"))' \
                          % (id, date, int(flow))
                    try:
                        ''# self.impala_cur.execute(sql)
                    except Exception as e:
                            print(e)

                self.driver.find_element_by_xpath('//input[@min="1" and@class="el-input__inner"]') \
                    .send_keys(Keys.BACKSPACE + Keys.BACKSPACE + Keys.BACKSPACE)
                self.driver.find_element_by_xpath('//input[@min="1" and@class="el-input__inner"]').send_keys(str(p))
                self.driver.find_element_by_xpath('//input[@min="1" and@class="el-input__inner"]').send_keys(Keys.ENTER)
        print('==== 数据爬取完毕 ====')
        self.driver.close()

    def get_dateList(self, dt=None):
        dtList = []
        sDate = datetime.datetime.strptime('2019-11-15', '%Y-%m-%d').date()
        eDate = datetime.date.today()
        if dt is not None:
            dtList.append(dt)
        else:
            while str(sDate) <= str(eDate):
                dtList.append(str(sDate))
                date = sDate + datetime.timedelta(days=1)
                sDate = date
        return dtList

    def convert(self, arg):
        arg = arg.replace(',', '')
        if '.' in arg:
            flow = re.findall(r'\d+\.\d+', arg)[0]
        else:
            flow = re.findall(r'\d+', arg)[0]
        if 'KB' in arg:
            arg = float(flow) * 1024
        elif 'MB' in arg:
            arg = float(flow) * 1024 * 1024
        elif 'GB' in arg:
            arg = float(flow) * 1024 * 1024 * 1024
        elif 'TB' in arg:
            arg = float(flow) * 1024 * 1024 * 1024 * 1024
        elif 'B' in arg:
            arg = float(flow)
        else:
            arg = 0
        return arg


if __name__ == '__main__':
    sp = LLSpider()
    sp.get_data()
