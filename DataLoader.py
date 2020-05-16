import csv
import time
import os
import shutil
import datetime
from sqlConn import getDBConnCursor
cursor,conn=getDBConnCursor()

def writeBatchData():
    path = "/Users/tanvi/PycharmProjects/assignment/data/inputs/"
    while True:
        file_list=os.listdir(path)
        print(file_list)
        for p in file_list:
            validateAndWriteFile(path, p)
        time.sleep(60)


def validateAndWriteFile(path, fileName):
    recordCount=0
    msg = 'Success'
    soureFile = path+fileName
    prcessedPath = "/Users/tanvi/PycharmProjects/assignment/data/processed/"
    with open (soureFile, 'r') as csvFile:
        reader = csv.reader (csvFile)
        for row in reader:
            recordCount = recordCount +1
            dateValue = row[2]
            if isInvaliDate(dateValue, soureFile):
                msg = "Failed"
                break
    if(recordCount < 10):
        msg = "Failed"

    logtodb('',soureFile,recordCount, msg)
    prcessedPath = prcessedPath+msg+"/"
    os.makedirs (prcessedPath, exist_ok=True)
    shutil.move(soureFile, prcessedPath)

def logtodb(process_name, path, recCount, message):
    process_date = datetime.datetime.today ().strftime ('%Y-%m-%d-%H:%M:%S')
    process_name = "DataLoad-"+str(process_date)
    insertto = "INSERT INTO Activity_log (process_name,file_name, message, process_date, count) VALUES(%s,%s,%s,%s,%s)"
    values= (str(process_name),str(path) ,str(message),str (process_date), recCount)

    cursor.execute (insertto, values )
    conn.commit ()

def isInvaliDate(date_text, path):
    invalid=False
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        print("Incorrect data format, should be YYYY-MM-DD in file " , path)
        invalid = True
    return invalid

if __name__ == '__main__':
    writeBatchData()

