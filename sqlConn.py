import pymysql


def getDBConnCursor():
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='123456', db='loan')
    cursor = conn.cursor()
    return cursor,conn
cursor, conn=getDBConnCursor()
