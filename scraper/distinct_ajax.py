#!/usr/bin/env python
# coding: utf-8



# Switch to Chinese website instead
import re
import os
import csv
import requests
import time
import sys
from urllib3 import *
from bs4 import BeautifulSoup # Used to parse the HTML content of web pages
from fake_useragent import UserAgent
import mysql.connector
from datetime import datetime




def get_ajax(url):
    ajaxport = ''.join(re.findall(r"[0-9]+",url,re.S))
    url_info = "https://www.nosetime.com/app/item.php?id={}".format(ajaxport)
    ua = UserAgent()
    headers = {'User-Agent': ua.random}
    response = requests.get(url_info,headers=headers)
    if response.status_code != 200:
        response = 1
    return response


        
if __name__ == '__main__':
    db_connection = mysql.connector.connect(
    host="urscentdb.carqdqoxxxwl.ap-northeast-1.rds.amazonaws.com",
    user="admin",
    passwd="zmlzzraa",
    port="3306",
    database="db1",
    charset='utf8mb4',
    collation = 'utf8mb4_general_ci'
    )
    mySql_select_query = ("SELECT url FROM pool1 where ajax_status = 0 limit 1") #28885
    mySql_update_query = """ UPDATE pool1 SET ajax_status = 1 WHERE url = %s """
    mySql_insert_query = """INSERT INTO perfume_ajax_distinct (nowdate, url, ajax) VALUES (%s, %s, %s) """
    count = 0
    print("Inserting perfumes ajax to mysqldb...")
    while True:
        cursor_query = db_connection.cursor()
        cursor_query.execute(query)
        record = cursor_query.fetchone()
        cursor_query.close()
        res = get_ajax(record[0])
        if res == 1:
            print("Blocked at #{} url...".format(count))
            break
        db_cursor = db_connection.cursor()
        db_cursor.execute(mySql_update_query, (record[0],))
        db_cursor.close()
        db_connection.commit()
        res = res.json()
        current_Date = datetime.now()
        formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S:%f')
        insert_tuple = (formatted_date,record[0],res)
        db_cursor = db_connection.cursor()
        db_cursor.execute(mySql_insert_query, insert_tuple)
        db_cursor.close()
        db_connection.commit()
        count += 1
        if count % 100 == 0:
            print("Scraped {} pages ajax...".format(count))
    db_connection.close()
    print("MySQL connection is closed")
    print("Woohoo, done! Congrats!")

