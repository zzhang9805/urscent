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




def get_html(url):
    ua = UserAgent()
    headers = {'User-Agent': ua.random}
    try:
        response = requests.get('https://www.nosetime.com'+url, headers=headers)
    except HTTPError as e:
        print ("Url Can not be found")
        return None
    except requests.exceptions.Timeout:
        print ("Timeout error") # Maybe set up for a retry, or continue in a retry loop
        return None
    except requests.exceptions.TooManyRedirects:
        print ("TooManyRedirects error") # Tell the user their URL was bad and try a different one
        return None
    except requests.exceptions.RequestException as e:
        print(e) # catastrophic error. bail.
        return None
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
    mySql_select_query = ("SELECT url FROM pool1 where html_status = 0 limit 1") #28885
    mySql_update_query = """ UPDATE pool1 SET html_status = 1 WHERE url = %s """
    mySql_insert_query = """INSERT INTO perfume_html_distinct (nowdate, url, html) VALUES (%s, %s, %s) """
    count = 0
    print("Inserting perfumes html to mysqldb...")
    while True:
        cursor_query = db_connection.cursor()
        cursor_query.execute(query)
        record = cursor_query.fetchone()
        cursor_query.close()
        res = get_html(record[0])
        if res == 1:
            print("Blocked at #{} url...".format(count))
            break
        html_text = res.text
        if html_text == None:
            print("Get HTML break at #{} url.".format(count))
            break
        db_cursor = db_connection.cursor()
        db_cursor.execute(mySql_update_query, (record[0],))
        db_cursor.close()
        db_connection.commit()
        current_Date = datetime.now()
        formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S:%f')
        insert_tuple = (formatted_date,record[0],html_text)
        db_cursor = db_connection.cursor()
        db_cursor.execute(mySql_insert_query, insert_tuple)
        db_cursor.close()
        db_connection.commit()
        count += 1
        if count % 100 == 0:
            print("Scraped {} pages html...".format(count))
    db_connection.close()
    print("MySQL connection is closed")
    print("Woohoo, done! Congrats!")