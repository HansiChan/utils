# -*- coding: utf-8 -*-
import logging
import os
import re
import shutil
import sys
import urllib.request
import zipfile


class Crack(object):

    def __init__(self, ext):
        # self.file_dir = r"/var/www/html"
        self.file_dir = r"D:/vizlib"
        self.ext = ext
        self.ext_name = ext.replace(" ", "")
        formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
        handler1 = logging.StreamHandler()
        handler1.setFormatter(formatter)
        self.logger = logging.getLogger("logger")
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(handler1)

    @staticmethod
    def _get_js(file):
        js = ''
        with open(file, 'r', encoding='utf-8') as file:
            for line in file:
                js += line
        return js

    def _unzip(self):
        file_zip = "_" + self.ext + ".zip"
        file = r"%s/vizlib_zip/%s" % (self.file_dir, file_zip)
        if not os.path.exists(file):
            self.logger.info(file + ' 路径文件不存在!!!')
            exit(1)
        file_dir = file.replace('.zip', '')
        zip_file = zipfile.ZipFile(file)
        if os.path.isdir(file_dir):
            pass
        else:
            os.mkdir(file_dir)
        for names in zip_file.namelist():
            zip_file.extract(names, file_dir + "/")
        zip_file.close()
        # os.remove(file)
        return file_dir

    def _zip(self, package):
        zip_name = "_" + self.ext + "_new.zip"
        zip_dir = r"%s/vizlib_zip/%s" % (self.file_dir, zip_name)
        z = zipfile.ZipFile(zip_dir, "w", zipfile.ZIP_DEFLATED)
        for dir_path, dir_names, file_names in os.walk(package):
            f_path = dir_path.replace(package, '')  # 这一句很重要，不replace的话，就从根目录开始复制
            f_path = f_path and f_path + os.sep or ''  # 实现当前文件夹以及包含的所有文件的压缩
            for filename in file_names:
                z.write(os.path.join(dir_path, filename), f_path + filename)
        z.close()

    def _package(self):
        regex = r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
        ext_js = self.ext_name + ".js"
        self.logger.info('解压拓展包...')
        package = self._unzip()
        self.logger.info('解压完毕')
        filename = package + "/" + ext_js
        download_js = r"%s/vizlib/%s.obt.js" % (self.file_dir, self.ext_name)
        js_str = self._get_js(filename)
        url = re.findall(regex, js_str)[0][0:-2]
        self.logger.info('开始下载入口js文件...')
        urllib.request.urlretrieve(url, download_js)
        self.logger.info('入口js文件下载完毕')
        new_js = js_str.replace(url, 'https://xgdata.dachentech.com.cn/monitor/vizlib/%s.obt' % self.ext_name)
        with open(filename, 'w', encoding='utf-8') as file_obj:
            file_obj.write(new_js)
        self.logger.info('开始打包拓展包...')
        self._zip(package)
        shutil.rmtree(package)
        self.logger.info('拓展包打包完毕')
        self.logger.info('开始下载obt js文件...')
        self._dowload(download_js)
        self.logger.info('obt js文件下载完毕')
        self.logger.info('整合完毕')
        return download_js

    def _dowload(self, file):
        obt_js = self._get_js(file)
        ext_dir = self.file_dir + r"/vizlib-extensions"
        regex_css = r'https://bouncer.vizlib.com/get\?key=\w+\-\w+\-\w+\-\w+&type=\w+&extension=\w+&version=\d.\d.\d&end'
        url_css = re.findall(regex_css, obt_js)[0]
        obt_js = obt_js.replace(url_css, 'https://xgdata.dachentech.com.cn/monitor/vizlib_extensions/'
                                + self.ext_name + '.css')
        obt_js = obt_js.replace('https://static.vizlib.com', 'https://xgdata.dachentech.com.cn/monitor')
        with open(file, 'w', encoding='utf-8') as file_obj:
            file_obj.write(obt_js)
        regex = r'\/%s\/[a-zA-Z0-9]+.[0-9]+.[0-9]+\/\w+\/[a-zA-Z0-9.-]+' % self.ext_name
        js_list = re.findall(regex, obt_js)
        for j in js_list:
            file_dir = ext_dir + "/".join(j.split("/")[:-1])
            file_name = ext_dir + j
            if not os.path.exists(file_dir):
                os.makedirs(file_dir)
            url = 'https://static.vizlib.com/vizlib-extensions' + j + "?"
            urllib.request.urlretrieve(url, file_name)
        css_name = ext_dir + '/' + self.ext_name + '.css'
        urllib.request.urlretrieve(url_css, css_name)


if __name__ == '__main__':
    extention = sys.argv[1]
    # extention = 'Vizlib Sankey Chart'
    c = Crack(extention)
    c._package()
