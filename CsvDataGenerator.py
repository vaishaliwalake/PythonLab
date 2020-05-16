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
     endLimit = 11
     currtime=datetime.datetime.now()
     curtime=currtime.strftime("%d%m%Y_%H%MM%SS")
     fileName = thread_name+"_"+str(curtime)+".csv"
     path = "/Users/tanvi/PycharmProjects/assignment/data/inputs/"+fileName

     for c in range(startLimit,endLimit):
        col1 =  c
        col2 = random.randint(1,1000)
        if thread_name.startswith('In-Valid'):
            col3 = getRandomDate ()
        else:
           col3 = getRandomDate().date()
        col4= round(random.uniform(1,2), 2)
        col5= getRandomString(10)
        col6= getRandomtimes()

        rows.append({"intdata":col1,"intdata2":col2,"datedata":col3,"decidata":col4,"stringdata":col5,"datetimedta":col6})
     writeCsvDataFile(rows, path)
 except Exception as exe:
  print (exe)
  messgae = "Failed"


def writeCsvDataFile(rows, path):
    fieldnames = ['intdata', 'intdata2',"datedata","decidata","stringdata","datetimedta"]
    with open (path, 'w', newline='') as csvfile:
     writer = csv.DictWriter (csvfile,fieldnames)
     writer.writerows(rows)

def startValidDataGeneratorThread():
    index =1
    while True:
        threadName = "valid-data_" +str(index)
        t1 = threading.Thread (target=GenerateRandmRecords (1, threadName), name=threadName)
        t1.start ()
        time.sleep (60)
        index = index +1

def startInValidDataGeneratorThread():
    index =1
    while True:
        threadName = "In-Valid-data_" +str(index)
        t1 = threading.Thread (target=GenerateRandmRecords (1, threadName), name=threadName)
        t1.start ()
        time.sleep (600)
        index = index +1


def main_task():
    t1 = threading.Thread (target=startValidDataGeneratorThread)
    print('Starting valid data generator thread')
    t1.start()
    t2 = threading.Thread (target=startInValidDataGeneratorThread)
    print ('Starting invalid data generator thread')
    t2.start()


if __name__ == "__main__":
    main_task()
