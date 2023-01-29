from requests_html import HTMLSession
import urllib
import json
import pymysql
import pandas as pd
from datetime import datetime
#test file


def datelist(beginDate, endDate):
    #get date list
    date_l=[datetime.strftime(x,'%d-%m-%y') for x in list(pd.date_range(start=beginDate, end=endDate))]
    return date_l

list = datelist('1-1-2022','31-12-2022')
print(len(list))
print(type(list[0]))