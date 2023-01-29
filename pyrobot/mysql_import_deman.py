

#Run this to import the demand data of 2022 to mysql

import pymysql


#connect
config = {'host':'',
          'port':3306,
          'user':'root',
          'passwd':'Chaoxuhong888999!',
          'charset':'utf8mb4',
          'local_infile':1
          }
conn = pymysql.connect(**config)
cur = conn.cursor()


#load csv
def load_csv(file_name,csv_file_path,table_name,database='evdata'):
    #open the csv file
    file = open(csv_file_path, 'r',encoding='utf-8')
    #read first line of csv and creat table
    reader = file.readline()
    b = reader.split(',')
    colum = ''
    for a in b:
        colum = colum + a + ' varchar(255),'
    colum = colum[:-1]
    #sql command
    create_sql = 'create table if not exists ' + table_name + ' ' + '(' + colum + ')' + ' DEFAULT CHARSET=utf8'
    data_sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES" % (file_name,table_name)
 
    #set db
    cur.execute('use %s' % database)
    #set code formate
    cur.execute('SET NAMES utf8;')
    cur.execute('SET character_set_connection=utf8;')
    #creat table
    cur.execute(create_sql)
    #exercude
    cur.execute(data_sql)
    conn.commit()
    #close connect
    conn.close()
    cur.close()

load_csv('demanddata.csv','C:\pythonprogramm\pyrobot\demanddata.csv','demand_2022','market_data')