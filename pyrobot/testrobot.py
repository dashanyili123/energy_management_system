
from requests_html import HTMLSession
import urllib
import json
import pymysql
import datetime

#test file

# model.add(layers.Dense(64, activation=opt))
#import jsonpath

session = HTMLSession()
url = 'https://www.nordpoolgroup.com/api/marketdata/page/189553?currency=GBP,GBP,EUR,EUR'
response = session.get(url)
#webdata = session.get(url,headers={'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'})
#sel = 'dashboard-column > div > div.dashboard-box > div.dashboard-tabs.chart-size.dashboard-indent > div.dashboard-tab.dashboard-tab-table.market-data-table > div.table-wrapper.uk-table-wrapper > div'
#results = webdata.html.find(sel)
#print(results)
#for i in marketdata:
   # print(i)

#print(type(marketdata))

#print(marketdata.text)

#print(type(marketdata))

response.encoding = 'utf8'
html = response.text
marketdata_dict = json.loads(html)
#marketdata_dict是原始数据



#print(type(marketdata_dict))

marketdata_list=list(marketdata_dict.values())
#第一层转为list



data = marketdata_list[0]
data_list = list(data.values())
#marketdata的第一项为需要的数据

data_list1=data_list[0]
#datalist的第一项为需要的数据
#datalist结构：
#第一层为时间段
#第二层为具体数据

#print(type(data_list1))
#print(len(data_list1))
#print(type(data_list1[0]))
#print(len(data_list1[0]))

j=0
for i in data_list1:
   
   data_list1[j]=list(i.values())
   j=j+1

#print('detail of data_list1')
#print(len(data_list1))
#print(type(data_list1))

#print('detail of each line in data_list1')
#print(len(data_list1[0]))
#print(type(data_list1[0]))

#print('detail of each item in data_list1')#EACH LINE/HOUR include half hour and time prieod 
#print(len(data_list1[0][0]))
#print(type(data_list1[0][0]))

#print('detail of each item in data_list1')#EACH HALF HOUR
#print(len(data_list1[0][0][0]))
#print(type(data_list1[0][0][0]))

#print('\n')



j=0
k=0
dailydatatime=['none']*len(data_list1)
dailydataprice1=['none']*len(data_list1)
dailydataprice2=['none']*len(data_list1)



dailydatatime[0]=data_list1[j][0][0]['Value']#test


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


#print(dailydatatime)
#print("\n")
##print(dailydataprice1)
#print('\n')
#print(dailydataprice2)

print(type(dailydatatime[0]))


import pymysql
import datetime
# 打开数据库连接
con = pymysql.connect(host='localhost',
                     user='root',
                     password='Chaoxuhong888999!',
                     database='market_data')

cur = con.cursor()
#cur.execute("create database market_data1 character set utf8;")
#create new table
#cur.execute("use market_data") #select database

#These code is for generate new table
#cur.execute("create table dayhead(date char(20),time char(20),first_half_price char(20),last_half_price char(20),)character set utf8;")
sql = """CREATE TABLE dayahead (
         data  CHAR(20) ,
         time  CHAR(20),
         first_hour CHAR(20),  
         last_hour CHAR(20)
        )"""
cur.execute(sql)


#These code is for insect new data
cur = con.cursor()
sql = "insert into dayahead(data,time,first_hour,last_hour) VALUE(%s, %s, %s, %s)"
today = datetime.date.today()
date = [str(today)]*len(data_list1)

sqldata = list(zip(date,dailydatatime,dailydataprice1,dailydataprice2))

try:
   # 执行sql语句
   cur.executemany(sql,sqldata)

except:
   # 如果发生错误则回滚
   con.rollback()
   print("error")


# 关闭数据库连接
con.commit()
con.close()
print(sqldata[0][0])