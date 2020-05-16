import datetime
import os
import shutil
import time
import zipfile
import csv
#with zipfile.ZipFile('/Users/tanvi/PycharmProjects/assignment/zipfiles/Faileddata_zipfile.zip', 'r') as zip_ref:
   #zip_ref.extractall('/Users/tanvi/PycharmProjects/assignment/data/unzip')

def iterateUnzipFiles():
    path = "/Users/tanvi/PycharmProjects/assignment/data/unzip/"
    file_list=os.listdir(path)
    print(file_list)
    for filename in file_list:
        cleanData(path, filename)


def cleanData(path,filename):
    soureFile = path + filename
    rows = []
    with open (soureFile, 'r') as csvFile:
        reader = csv.reader (csvFile)
        for row in reader:
            dateValue = row[2]
            datev=datetime.datetime.strptime(dateValue,'%Y-%m-%d %H:%M:%S')
            datevaluparsed=datev.date()
            print (datevaluparsed)
            col1 = row[0]
            col2 = row[1]
            col3 = datevaluparsed
            col4 = row[3]
            col5 = row[4]
            col6 = row[5]
            rows.append ({"intdata": col1, "intdata2": col2, "datedata": col3, "decidata": col4, "stringdata": col5,
                          "datetimedta": col6})
    writeCleanDataFile (rows, filename)

def writeCleanDataFile(rows, filename):
    path = "/Users/tanvi/PycharmProjects/assignment/data/inputs/"+filename
    print('start writing cleaned file : ', path)
    fieldnames = ['intdata', 'intdata2',"datedata","decidata","stringdata","datetimedta"]
    with open (path, 'w', newline='') as csvfile:
      writer = csv.DictWriter (csvfile,fieldnames)
      writer.writerows(rows)
    print ('completed writing cleaned file : ', path)

if __name__ == '__main__':
    iterateUnzipFiles()

