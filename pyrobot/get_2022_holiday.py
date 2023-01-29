import numpy as np
import pandas as pd
import holidays 
from datetime import datetime

def datelist(beginDate, endDate):
    #get date list
    date_l=[datetime.strftime(x,'%y-%m-%d') for x in list(pd.date_range(start=beginDate, end=endDate))]
    return date_l


holiday_date_list_2022 = []
for date in holidays.UnitedKingdom(years = 22).items():
    holiday_date_list_2022 = holiday_date_list_2022 + [str(date[0])]
    
datelist_2022 = datelist('2022-01-01', '2022-12-31')
is_holiday = ['0']*len(datelist_2022)



j = 0
for item in holiday_date_list_2022:
    holiday_date_list_2022[j] = holiday_date_list_2022[j][2:]
    j=j+1

i=0
for date in datelist_2022:
    for holiday in holiday_date_list_2022:
        if date == holiday:
            is_holiday[i] = '1'
    
    i = i+1
print(holiday_date_list_2022)
print(is_holiday)






