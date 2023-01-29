import pymysql
#run this to create tabel 
#create data base before run this file
con = pymysql.connect(host='localhost',
                     user='root',
                     password='Chaoxuhong888999!',
                     database='market_data')

cur = con.cursor()

sql = """CREATE TABLE dayahead (
    date  CHAR(20) ,
    time  CHAR(20),
    first_hour CHAR(20),  
    last_hour CHAR(20)
    )"""
cur.execute(sql)