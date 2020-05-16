import os
from xml.etree import ElementTree
from sqlConn import getDBConnCursor
cursor,conn=getDBConnCursor()

def loadXmlToDb():
#get xml file from a directory
 xmlOutputPath = "/Users/tanvi/PycharmProjects/assignment/data/processed/xml/"
 file_list = os.listdir (xmlOutputPath)
 os.chdir(xmlOutputPath)
 for xmlfile in file_list:
   dom = ElementTree.parse(xmlfile)
   args_list = ([t.text for t in dom.iter(tag)] for tag in ['intdata','intdata1','date','decimldata'
    ,'string','datetime'])
   query = "insert into xmldata(intdata, intdata1,datedata,decimldata,stringdata,datetimedata ) VALUES (%s, %s, %s, %s, %s, %s)"
   # create the tuples from the argument list
   sqltuples = list(zip(*args_list))
   print(sqltuples)
   try:
    cursor.executemany(query,sqltuples)
    conn.commit()
   except:
    print('failed to insert')

loadXmlToDb()