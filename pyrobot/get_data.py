from requests_html import HTMLSession
import urllib
import json
import pymysql
import pandas as pd
from datetime import datetime



#run mysqlcontrol.py to create tabel before run this file
#run this file to get data from nord pool and import to mysql

def datelist(beginDate, endDate):
    #get date list
    date_l=[datetime.strftime(x,'%d-%m-%y') for x in list(pd.date_range(start=beginDate, end=endDate))]
    return date_l

def get_now_dayahead_price(date):
    session = HTMLSession()
    url = 'https://www.nordpoolgroup.com/api/marketdata/page/189553?currency=GBP,GBP,EUR,EUR&endDate='+date
    #the price data url of the uk day ahead market from nood pool
    #dynamic website
    response = session.get(url)
    response.encoding = 'utf8'
    html = response.text
    marketdata_dict = json.loads(html)
    #transfer the json dictionary to python list

    marketdata_list=list(marketdata_dict.values())
    data = marketdata_list[0]
    data_list = list(data.values())
    data_list1=data_list[0]
    j=0
    for i in data_list1:
        data_list1[j]=list(i.values())
        j=j+1
    j=0
    k=0
    #process the data 
    #data structure: the first item in marketdata is needed call datalist
                    #the first item in datalist is need call datalist1
                    #the datalist1 have 24 lines for 24h
                    # each line include 4 dict item for time, first half hour price, 
                    # last half hour price and  DA hourly price
                    # the 'value' key is what we need


    dailydatatime=['none']*len(data_list1)#save the time
    dailydataprice1=['none']*len(data_list1)#first half hour price
    dailydataprice2=['none']*len(data_list1)#last half hour price

    for thing in data_list1:
        dailydatatime[j]=data_list1[j][0][0]['Value']
        dailydataprice1[j]=data_list1[j][0][1]['Value']
        dailydataprice2[j]=data_list1[j][0][2]['Value']
        j=j+1

    j=0
    m = '0'
    n = '0'
    for item in dailydatatime:
        m=item[0:2]
        n=item[15:]
        dailydatatime[j] = m+'-'+n
        j=j+1
    #transfer the date to "xx-yy" format
    return dailydatatime,dailydataprice1,dailydataprice2 


#These code is for insect new data
def send_data_to_dayahead_form(date,time,price1,price2):
    #connect to mysql 
    con = pymysql.connect(host='localhost',
                     user='root',
                     password='Chaoxuhong888999!',
                     database='market_data')

    cur = con.cursor()

    #use this code to create new database:
    #
    #cur.execute("create database market_data1 character set utf8;")
    #create new table
    #cur.execute("use market_data") #select database

    #use this code to generate new table:
    #
    #sql = """CREATE TABLE dayahead (
    #     data  CHAR(20) ,
     #    time  CHAR(20),
      #   first_hour CHAR(20),  
      #   last_hour CHAR(20)
       # )"""
    #cur.execute(sql)

    #These code is for insect new data
    cur = con.cursor()
    sql = "insert into dayahead(data,time,first_hour,last_hour) VALUE(%s, %s, %s, %s)"
    if date == 'none':
        today = datetime.date.today()
        date = [str(today)]*len(time)
    
    sqldata = list(zip(date,time,price1,price2))

    #run sql 
    try:
        cur.executemany(sql,sqldata)

    except:
        con.rollback()
        print("sql insect to day ahead error")
    
    #close sql
    con.commit()
    con.close()

    return 0



#run this function will get the inputdate day ahead data from nord pool
#output datetime, first hour price, last hour price
def get_now_dayahead_price(date):
    session = HTMLSession()
    url = 'https://www.nordpoolgroup.com/api/marketdata/page/189553?currency=GBP,GBP,EUR,EUR&endDate='+date
    #the price data url of the uk day ahead market from nood pool
    #dynamic website
    response = session.get(url)
    response.encoding = 'utf8'
    html = response.text
    marketdata_dict = json.loads(html)
    #transfer the json dictionary to python list

    marketdata_list=list(marketdata_dict.values())
    data = marketdata_list[0]
    data_list = list(data.values())
    data_list1=data_list[0]
    j=0
    for i in data_list1:
        data_list1[j]=list(i.values())
        j=j+1
    j=0
    k=0
    #process the data 
    #data structure: the first item in marketdata is needed call datalist
                    #the first item in datalist is need call datalist1
                    #the datalist1 have 24 lines for 24h
                    # each line include 4 dict item for time, first half hour price, 
                    # last half hour price and  DA hourly price
                    # the 'value' key is what we need


    dailydatatime=['none']*len(data_list1)#save the time
    dailydataprice1=['none']*len(data_list1)#first half hour price
    dailydataprice2=['none']*len(data_list1)#last half hour price

    for thing in data_list1:
        dailydatatime[j]=data_list1[j][0][0]['Value']
        dailydataprice1[j]=data_list1[j][0][1]['Value']
        dailydataprice2[j]=data_list1[j][0][2]['Value']
        j=j+1

    j=0
    m = '0'
    n = '0'
    for item in dailydatatime:
        m=item[0:2]
        n=item[15:]
        dailydatatime[j] = m+'-'+n
        j=j+1
    #transfer the date to "xx-yy" format
    return dailydatatime,dailydataprice1,dailydataprice2 


#These code is for insect new data
def send_data_to_dayahead_form(date,time,price1,price2):
    #connect to mysql 
    con = pymysql.connect(host='localhost',
                     user='root',
                     password='Chaoxuhong888999!',
                     database='market_data')

    cur = con.cursor()

    #use this code to create new database:
    #
    #cur.execute("create database market_data1 character set utf8;")
    #create new table
    #cur.execute("use market_data") #select database

    #use this code to generate new table:
    #
    #sql = """CREATE TABLE dayahead (
    #     data  CHAR(20) ,
     #    time  CHAR(20),
      #   first_hour CHAR(20),  
      #   last_hour CHAR(20)
       # )"""
    #cur.execute(sql)

    #These code is for insect new data
    cur = con.cursor()
    sql = "insert into dayahead(date,time,first_hour,last_hour) VALUE(%s, %s, %s, %s)"

    
    sqldata = list(zip(date,time,price1,price2))

    #run sql 
    try:
        cur.executemany(sql,sqldata)

    except:
        con.rollback()
        print("sql insect to day ahead error")
    
    #close sql
    con.commit()
    con.close()

    return 0

import time

def dateRange(bgn, end):
    fmt = '%d-%m-%Y'
    bgn = int(time.mktime(time.strptime(bgn,fmt)))
    end = int(time.mktime(time.strptime(end,fmt)))
    return [time.strftime(fmt,time.localtime(i)) for i in range(bgn,end+1,3600*24)]




datelist_2022=dateRange('01-01-2022','31-12-2022')
print(type(datelist_2022[0]))

datatime = []
dataprice1 = []
dataprice2 = []
date_2022 = []
datatime_2022 = []
dataprice1_2022 = []
dataprice2_2022 = []
date24=[]
print(datelist_2022)

for date in datelist_2022:
    print(date)
    
    datatime,dataprice1,dataprice2 = get_now_dayahead_price(str(date))
    if date == '30-10-2022':#nord pool data problem,del the third data
        datatime.pop(3)
        dataprice1.pop(3)
        dataprice2.pop(3)


    date24=len(datatime)*[date]
    #send_data_to_dayahead_form(date24,datetime,dataprice1,dataprice2)
    date_2022 = date_2022 + len(datatime)*[date]
    datatime_2022 = datatime_2022 + datatime
    dataprice1_2022 = dataprice1_2022 + dataprice1
    dataprice2_2022 = dataprice2_2022 + dataprice2
    

print(len(datelist_2022))
print(len(date_2022))
print(len(datatime_2022))
print(len(dataprice1_2022))
print(len(dataprice2_2022))

send_data_to_dayahead_form(date_2022,datatime_2022,dataprice1_2022,dataprice2_2022)
