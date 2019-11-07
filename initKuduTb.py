import pyodbc
import configparser
import sys
from impala.dbapi import connect
import pymysql

conf = configparser.ConfigParser()
# conf.read(r"D:\\job_script\\utils\\config.ini")
conf.read("/data/job_pro/utils/config.ini")
HOST=conf.get('impaladb', 'host')
PORT=int(conf.get('impaladb', 'port'))
# cnxnstr = "DSN=Sample Cloudera Impala DSN;HOST=%s;PORT=%s;UID=hive;AuthMech=3;PWD=hive;UseSasl=0" % (HOST,PORT)
cnxnstr = "Driver={/opt/cloudera/impalaodbc/lib/64/libclouderaimpalaodbc64.so};HOST=%s;PORT=%s;UID=hive;AuthMech=3;PWD=hive;UseSasl=0" % (HOST,PORT)
conn = pyodbc.connect(cnxnstr, autocommit=True,timeout=240)
impala_cur = conn.cursor()
hive_con = connect(host=HOST, port=10000, user='root' ,auth_mechanism='PLAIN')
hive_cur = hive_con.cursor()

MYSQL_HOSTS= conf.get('mysqldb','host')
MYSQL_USERNAME=conf.get('mysqldb','user')
MYSQL_PASSWORD=conf.get('mysqldb','password')
MYSQL_DB=conf.get('mysqldb','dbreport')

def getTable():
    conn_mysql = pymysql.connect(MYSQL_HOSTS,MYSQL_USERNAME,MYSQL_PASSWORD,MYSQL_DB)
    cur = conn_mysql.cursor()
    sql = "select id,source,target,source_condition from report.initKudu_config where flag='Y'"
    print('getTable Sql:'+ sql)
    cur.execute(sql)
    result = cur.fetchall()
    conn_mysql.close()
    return result

def updateFlag(id):
    conn_mysql = pymysql.connect(MYSQL_HOSTS,MYSQL_USERNAME,MYSQL_PASSWORD,MYSQL_DB)
    cur = conn_mysql.cursor()
    sql = "update report.initKudu_config set flag='N' where id=%d" % (id)
    print('update Flag:'+ sql)
    cur.execute(sql)
    conn_mysql.commit()
    conn_mysql.close()

def init(src_table,target_table,source_condition):
    try:
        sql = 'show create table %s;' % (target_table)
        impala_cur.execute(sql)
        ct = impala_cur.fetchall()[0][0]
        tmp_Table = 'tmp.' + target_table.split('.')[1] + '_kuduinit'
        arr = ct.split('\n')[1:-4]
        fieldarr=[]
        colum=''
        existsLoadtime = 0
        for i in arr:
            if 'loadtime' in i:
                existsLoadtime =1
            if 'PRIMARY' not in i and 'PARTITION' not in i and 'loadtime' not in i and 'checkusercount' not in i :
                field='`'+i.split('  ')[1].split(' ')[0]+'` ' + i.split('  ')[1].split(' ')[1].split(' ')[0] +','
                colum+='`'+i.split('  ')[1].split(' ')[0] + '`,'
                fieldarr.append(field)
        dropsql = "drop table if exists " + tmp_Table
        print('删除临时表(如果存在):'+dropsql)
        impala_cur.execute(dropsql)
        nct = 'create table if not exists ' +  tmp_Table + '(' \
              + '\n' + '\n'.join(fieldarr)[0:-1] + '\n' \
              + ')\n' \
              + 'stored as parquet'

        print('拼装建表语句,创建临时hive表:\n'+nct)
        hive_cur.execute(nct)
        if source_condition:
            load = 'insert overwrite table ' + tmp_Table + ' select '+ colum[:-1] +' from ' + src_table +' ' +source_condition
        else:
            load = 'insert overwrite table ' + tmp_Table + ' select '+ colum[:-1] +' from ' + src_table
        print('执行插入临时表语句.....:'+load)
        import os
        os.system("hive -e '"+load+"' ")
        # hive_cur.execute(load)

        refresh = 'INVALIDATE METADATA ' + tmp_Table
        impala_cur.execute(refresh)
        print('刷新临时表元数据,插入对应kudu表中.....')
        insert_sql=''
        if existsLoadtime ==1 :
            insert_sql = 'upsert into ' + target_table + ' select ' + colum[:-1] + ',concat(cast(unix_timestamp() as string),\'000\') as loadtime from ' + tmp_Table
        else:
            alter_loadtime = 'alter table '+target_table+' add columns(loadtime string);'
            impala_cur.execute(alter_loadtime)
            insert_sql = 'upsert into ' + target_table + ' select ' + colum[:-1] + ',concat(cast(unix_timestamp() as string),\'000\') as loadtime from ' + tmp_Table
        print('写入kudu的sql语句:'+insert_sql)
        impala_cur.execute(insert_sql)

        tmp_sql='select count(1) from '+tmp_Table
        tmp_exc=impala_cur.execute(tmp_sql)
        tmp_count=str(tmp_exc.fetchone()[0])

        kudu_sql='select count(1) from '+target_table
        kudu_exc=impala_cur.execute(kudu_sql)
        kudu_count=str(kudu_exc.fetchone()[0])
        print('临时表数据量:'+tmp_count+'  目标kudu表数据量:'+kudu_count)
        #删除临时hive表
    except Exception as e:
        print(e)
        refresh = 'INVALIDATE METADATA ' + tmp_Table
        impala_cur.execute(refresh)
        impala_cur.close()
        sys.exit(1)
if __name__ == '__main__':
    print('###############################此初始化程序只适合mogo2hive/mysql2hive表字段命名与kudu一致的情况!!!!!###############################')
    if len(sys.argv)==2:
        src_table = sys.argv[1]
        target_table = sys.argv[2]
        print('***************源表:'+src_table+"*********目标表:"+target_table)
        init(src_table,target_table)
    else:
        tb = getTable()
        for row in tb:
            id = row[0]
            src_table=row[1].strip()
            target_table=row[2].strip()
            if not row[3]== None: source_condition=row[3].strip()
            else:source_condition=row[3]
            print('*****************************源表:'+src_table+"--->目标表:"+target_table+'*****************************')
            init(src_table,target_table,source_condition)
            updateFlag(id)
    impala_cur.close()