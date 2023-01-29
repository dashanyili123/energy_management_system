
import pandas as pd
import numpy as np
import pymysql

#run this to read mysql data and tranfer the data to suit LSTM model

def get_one_data_form_mysql(item_name,table_name):
    con = pymysql.connect(host='localhost',
                    user='root',
                    password='Chaoxuhong888999!',
                    database='market_data')

    cur = con.cursor()
    sqlcom = 'select '+ str(item_name) +' from '+str(table_name)
    cur.execute(sqlcom)

    alldata = cur.fetchall()
    
    data_list = []
    for item in alldata:
        data_list = data_list + [item[0]]
    print('get '+ item_name +' success')
    return data_list
    cur.close()
    con.close()

day_list = get_one_data_form_mysql('date','businesslist')
month_list = get_one_data_form_mysql('month','businesslist')
period = get_one_data_form_mysql('SETTLEMENT_PERIOD','demand_2022')
dayahead1 = get_one_data_form_mysql('first_hour','dayahead')
dayahead2 = get_one_data_form_mysql('last_hour','dayahead')
demand_low = get_one_data_form_mysql('ND','demand_2022')
is_business = get_one_data_form_mysql('isbusinessday','businesslist')

print('day_list len:')
print(len(day_list))
print('month_list len')
print(len(month_list))
print('period len')
print(len(period))
print('dayahead1 len')
print(len(dayahead1))
print('dayahead2 len')
print(len(dayahead2))
print('demand_low len')
print(len(demand_low))
print('is_business len')
print(len(is_business))

#data process

#day process 1-48
LSTM_day=[]
for day in day_list:
    LSTM_day = LSTM_day + [day]*48
#month process 1-48
LSTM_month=[]
for month in month_list:
    LSTM_month = LSTM_month + [month]*48

#dayahead process 1-2
LSTM_dayahead=[]
i=0
j=0
for i in range(0,len(period)):
    if i%2 == 0:
        LSTM_dayahead=LSTM_dayahead+[dayahead1[j]]
    else:
        LSTM_dayahead=LSTM_dayahead+[dayahead2[j]]
        j=j+1

#isbusiness process 1-48
LSTM_bus = []
for bus in is_business:
    LSTM_bus = LSTM_bus + [bus]*48

#data list:
#LSTM_day: day number 
#LSTM_month: month number
#period: period of the day range 1-48
#LSTM_dayahead: dayahead price
#demand: demand data each period
#LSTM_bus: is this day a business day, 1=yes,0=no






