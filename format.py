#!/usr/bin/env python
# _*_ coding:utf-8 _*_
import json

import pyodbc
import requests

cnxnstr = "DSN=Sample Cloudera Impala DSN;HOST=192.168.3.121;PORT=21050;UID=hive;AuthMech=3;PWD=hive;UseSasl=0"
# cnxnstr = "Driver={/opt/cloudera/impalaodbc/lib/64/libclouderaimpalaodbc64.so};HOST=192.168.3.121;PORT=21050;UID=hive;AuthMech=3;PWD=hive;UseSasl=0"
conn = pyodbc.connect(cnxnstr, autocommit=True, timeout=240)
impala_cur = conn.cursor()


def parse_data():
    with open('11pfop', 'r') as file:
        wrote = []
        for line in file:
            line = line.replace('[', '').replace(']', '').split(',')
            dt = line[0]
            flag = line[1]
            data = ','.join(line[2:-2])
            callback = line[-2]
            unknow = line[-1]
            # print(dt, flag, data, callback, unknow)
            row = dt + '\t' + flag + '\t' + data + '\t' + callback
            wrote.append(row)
    with open('11pfop_w', 'a') as file:
        for row in wrote:
            file.write(row + '\n')


def get_url_data():
    sql = 'select times,`key`,url from tmp.11pfop2 where url is not null and url_data is NULL'
    impala_cur.execute(sql)
    results = impala_cur.fetchall()
    for row in results:
        times = "'" + row[0] + "'"
        key = "'" + row[1] + "'"
        url = row[2] + '?avinfo'
        response = requests.get(url)
        sql = 'update tmp.`11pfop2` set url_data = cast(%s as string) where times=%s and `key`=%s' % \
              ("'" + response.text.replace("'", "") + "'", times, key)
        print(str(sql))
        impala_cur.execute(sql)
        # print(response.text)
        # print(row,url)


def parse_fops():
    sql = 'select times,`key`,fops from tmp.11pfop3'
    impala_cur.execute(sql)
    results = impala_cur.fetchall()
    sql_list = []
    fops_list = []
    for row in results:
        times = "'" + row[0] + "'"
        key = "'" + row[1] + "'"
        if '/' not in row[2] or len(row[2].split('/')) == 2:
            fops = row[2]
        else:
            fops = '/'.join(row[2].split('/')[0:-1])
        if fops == 'avthumb':
            print(row[0], row[1], row[2])
        # print(fops)
        fops_list.append(fops)
    from collections import Counter
    result = Counter(fops_list)
    print(result)
    # sql = 'update tmp.`11pfop2` set fops = cast(%s as string) where times=%s and `key`=%s;' % \
    #       ("'" + fops.replace("'", "") + "'", times, key)
    # sql_list.append(sql)
    # for sql in sql_list:
    #     print(sql)
    #     impala_cur.execute(sql)


def data_formar():
    sql = 'select times,`key`,url_data from tmp.11pfop3'
    impala_cur.execute(sql)
    results = impala_cur.fetchall()
    dl = []
    for i in results:
        format_name = None
        format_long_name = None
        format_duration = None
        format_size = None
        format_bit_rate = None
        audio_codec_name = None
        audio_sample_rate = None
        audio_duration = None
        audio_bit_rate = None
        video_codec_name = None
        video_width = None
        video_height = None
        video_coded_width = None
        video_coded_height = None
        video_r_frame_rate = None
        video_duration = None
        video_bit_rate = None
        if i[2] is not None:
            url_data = json.loads(i[2])
            if 'format' in url_data:
                format_name = url_data.get('format').get('format_name')
                format_long_name = url_data.get('format').get('format_long_name')
                format_duration = url_data.get('format').get('duration')
                format_size = url_data.get('format').get('size')
                format_bit_rate = url_data.get('format').get('bit_rate')
            if 'streams' in url_data:
                streams = url_data.get('streams')
                for stream in streams:
                    if stream.get('codec_type') == 'audio':
                        audio_codec_name = stream.get('codec_name')
                        audio_sample_rate = stream.get('sample_rate')
                        audio_duration = stream.get('duration')
                        audio_bit_rate = stream.get('bit_rate')
                    if stream.get('codec_type') == 'video':
                        video_codec_name = stream.get('codec_name')
                        video_width = stream.get('width')
                        video_height = stream.get('height')
                        video_coded_width = stream.get('coded_width')
                        video_coded_height = stream.get('coded_height')
                        video_r_frame_rate = stream.get('r_frame_rate')
                        video_duration = stream.get('duration')
                        video_bit_rate = stream.get('bit_rate')
        d = [i[0], i[1], format_name, format_long_name, format_duration, format_size, format_bit_rate, audio_codec_name,
             audio_sample_rate, audio_duration, audio_bit_rate, video_codec_name, video_width, video_height,
             video_coded_width, video_coded_height, video_r_frame_rate, video_duration, video_bit_rate]
        d = list(map(str, d))
        data = '\t'.join(d)
        dl.append(data)
    with open('11pfop_avinfo', 'a') as file:
        for row in dl:
            file.write(row + '\n')


if __name__ == '__main__':
    # parse_data()
    # get_url_data()
    parse_fops()
    # data_formar()
