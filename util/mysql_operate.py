import pymysql
from config.config import mysql_conf
import traceback

def connect():
    conf = mysql_conf
    conn = pymysql.connect(host=conf["host"],
    user=conf["user"],
    password=conf["password"],
    database=conf["database"],
    charset=conf["charset"])
    return conn

def select(SQL):
    conn = connect()
    cursor = conn.cursor()
    testlist = []
    try:
        sql = SQL
        data = cursor.execute(sql)
        for line in cursor:
            testlist.append(line)
    except Exception as e:
        print(e)
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
        return testlist

def update(SQL):
    conn = connect()
    cursor = conn.cursor()
    try:
        sql = SQL
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        traceback.print_exc()
        conn.rollback()
        print(e)
    finally:
        cursor.close()
        conn.close()

def insert(SQL):
    conn = connect()
    cursor = conn.cursor()
    sql = SQL
    try:
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        traceback.print_exc()
        conn.rollback()
        print(e)
    finally:
        cursor.close()
        conn.close()
