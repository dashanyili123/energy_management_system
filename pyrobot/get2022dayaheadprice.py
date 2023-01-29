from requests_html import HTMLSession
import urllib
import json
import pymysql
import datetime

#this is a test file

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
    cur.executemany(sql,sqldata)
    #run sql 
    #try:
        #cur.executemany(sql,sqldata)

    #except:
        #con.rollback()
        #print("sql insect to day ahead error")
    
    #close sql
    con.commit()
    con.close()

    return 0



m = '01-01-2022'
a,b,c = get_now_dayahead_price(m)
print(b)


