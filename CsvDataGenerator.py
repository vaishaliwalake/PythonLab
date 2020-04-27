import csv
import os
import string
import datetime
from datetime import timedelta
import pymysql
import random
import time
import threading
from sqlConn import getDBConnCursor
cursor,conn=getDBConnCursor()

def getRandomDate():
    start_date = datetime.datetime (2020,11,10)
    end_date = datetime.datetime (2021, 2, 1)
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange (days_between_dates)
    random_date = start_date + datetime.timedelta(days=random_number_of_days)
    return random_date

def getRandomtimes( n=1):
    frmt = '%d-%m-%Y %H:%M:%S'
    stime = datetime.datetime.strptime('20-01-2018 13:30:00', frmt)
    etime = datetime.datetime.strptime('20-04-2020 13:30:00', frmt)
    td = etime - stime
    return [random.random() * td + stime for _ in range(n)][0]

def getRandomString(stringLength=10):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))
    print("Random String is ", RandomString())

def GenerateRandmRecords(startLimit,thread_name):
 messgae = "Success"
 path = ""
 try:
     rows=[]
     endLimit = 5
     currtime=datetime.datetime.now()
     curtime=currtime.strftime("%d%m%Y_%H:%M:%S")
     fileName = thread_name+"_Data_"+str(curtime)+".csv"
     path = "/Users/tanvi/PycharmProjects/assignment/data/"+fileName

     for c in range(startLimit,endLimit):
        col1 =  c
        col2 = random.randint(1,1000)
        col3= getRandomDate().date()
        col4= round(random.uniform(1,2), 2)
        col5= getRandomString(10)
        col6= getRandomtimes()

        rows.append({"intdata":col1,"intdata2":col2,"datedata":col3,"decidata":col4,"stringdata":col5,"datetimedta":col6})
        writeCsvDataFile(rows, path)
 except Exception as exe:
  print (exe)
  messgae = "Failed"

 logtodb (thread_name, path,messgae)

def writeCsvDataFile(rows, path):
    fieldnames = ['intdata', 'intdata2',"datedata","decidata","stringdata","datetimedta"]
    with open (path, 'w', newline='') as csvfile:
     writer = csv.DictWriter (csvfile,fieldnames)
     writer.writerows(rows)


def logtodb(process_name, path, message):

    process_date = datetime.datetime.today().strftime('%Y-%m-%d-%H:%M:%S')

    insertto = "INSERT INTO Activity_log (process_name,file_name, message, process_date) VALUES(%s,%s,%s,%s)"
    values= (str(process_name),str(path) ,str(message),str (process_date))

    cursor.execute (insertto, values )
    conn.commit ()

def main_task():
    for stLimit in range (1, 10):
        threadName = "threadname-"+str(stLimit)
        print('starting thread - ', threadName)
        t1 = threading.Thread (target=GenerateRandmRecords(stLimit,threadName),name=threadName)
        t1.start ()
        time.sleep (2)

if __name__ == "__main__":
    main_task ()

