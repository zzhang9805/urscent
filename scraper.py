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


def read_data(filename):
    with open(filename) as f:
        lines = f.read().split(',')
    return lines


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
    return response


def get_brand_urls():
    """
    Input:
    ------
    List of brand name start letter webpage urls

    Output:
    ------
    Perfume brand name urls in a list
    A dictionary of perfume EN and CN names
    """
    lst = ['/pinpai/2-a.html','/pinpai/3-b.html','/pinpai/4-c.html',
           '/pinpai/5-d.html','/pinpai/6-e.html','/pinpai/7-f.html',
           '/pinpai/8-g.html','/pinpai/9-h.html','/pinpai/10-i.html',
           '/pinpai/11-j.html','/pinpai/12-k.html','/pinpai/13-i.html',
           '/pinpai/14-m.html','/pinpai/15-n.html','/pinpai/16-o.html',
           '/pinpai/17-p.html','/pinpai/18-q.html','/pinpai/19-r.html',
           '/pinpai/20-s.html','/pinpai/21-t.html','/pinpai/22-u.html',
           '/pinpai/23-v.html','/pinpai/24-w.html','/pinpai/25-x.html',
           '/pinpai/26-y.html','/pinpai/27-z.html']
    count = 0
    brand_urls = []
    brand_names = {}
    for url in lst:
        response = get_html(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        result = soup.find_all('a', {'class': 'imgborder'})
        for r in result:
            brand_urls.append(r.attrs['href'])
            name = r.next_sibling.text
            split = re.split(r'([a-zA-Z]+)', name)
            brand_names[split[0]] = ''.join(split[1:])
        time.sleep(5) # In case I got blocked
        count += 1
        print ("Scraped {} urls...".format(count))
    return brand_urls, brand_names


def scrape_first_page(brand_urls, range_start, range_end):
    """
    Need to go through each brand_url, scrape the first page, then return all other page_urls.
    Then go through each page_url to return the fragrance names on other pages.
    """
    count = 0
    for url in brand_urls[range_start:range_end]:
        response = get_html(url)
        if response == None:
            print ("Get HTML break at #{} url.".format(count))
            break
        soup = BeautifulSoup(response.text, 'html.parser')
        perfume = soup.find_all('a', {'class': 'imgborder'}) # scrape all 1st pages
        pages_raw = soup.find_all('div', {'class': 'next_news'})

        with open('data/perfumes_2.csv','a') as resultFile: # go through each page, fetch perfume urls and store to csv
            wr = csv.writer(resultFile)
            for p in perfume:
                wr.writerow([p.attrs['href']])

        with open('data/pages.csv','a') as resultFile:
            wr = csv.writer(resultFile)
            for page in pages_raw[0].find_all('a')[1:-2]:
                wr.writerow([page['href']])

        time.sleep(10) # In case I got blocked
        count += 1
        if count % 10 == 0:
            print("Scraped {} page urls...".format(count))
    print ("Done writing perfume urls to csv! Congrats! Save returned pages_list!")


def get_url_list(filename):
    """Convert a csv file with \r\n delimiter to a list of strings

    Input: csv file with \r\n delimeter
    Output: a list of urls
    """
    f = open(filename)
    data = []
    for line in f:
        data_line = line.rstrip().split('\r\n')
        data.append(data_line[0])
    return data


def scrape_other_pages(pages_list):
    """
    Go through each page other than the first page, scrape perfume urls

    Input: A list of page urls
    Output: Append perfume url to perfumes_2.csv
    """
    count = 0
    for url in pages_list:
        response = get_html(url)
        if response == None:
            print("Get HTML break at #{} url.".format(count))
            break
        soup = BeautifulSoup(response.text, 'html.parser')
        perfume = soup.find_all('a', {'class': 'imgborder'})
        with open('data/perfumes_2.csv','a') as resultFile: # go through each page, fetch perfume urls and store to csv
            wr = csv.writer(resultFile)
            for p in perfume:
                wr.writerow([p.attrs['href']])
        time.sleep(10) # In case I got blocked
        count += 1
        if count % 10 == 0:
            print("Scraped {} page urls...".format(count))
        if count % 90 == 0:
            print("Take a nap for 8 minutes...Please don't block me!!!")
            time.sleep(60*8)
    print("Done writing perfume urls to csv!")


def scrape_perfume_page(perfume_urls):
    """Scrape one page html and store into Mysqldb

    Input: list of perfume urls
    Output: key, url, html, stored into Mysqldb
    """
    db_connection = mysql.connector.connect(
    host="urscentdb.carqdqoxxxwl.ap-northeast-1.rds.amazonaws.com",
    user="admin",
    passwd="zmlzzraa",
    port="3306",
    database="db1",
    charset='utf8mb4',
    collation = 'utf8mb4_general_ci'
    )
    mySql_insert_query = """INSERT INTO perfume_html (nowdate, url, html) VALUES (%s, %s, %s) """
    count = 0
    for url in perfume_urls:
        html_text = get_html(url).text
        if html_text == None:
            print("Get HTML break at #{} url.".format(count))
            break
        current_Date = datetime.now()
        formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S:%f')
        insert_tuple = (formatted_date,url,html_text)
        db_cursor = db_connection.cursor()
        db_cursor.execute(mySql_insert_query, insert_tuple)
        db_cursor.close()
        db_connection.commit()
        count += 1
        if count % 100 == 0:
            print("Scraped {} pages html...".format(count))
    db_connection.close()
    print("MySQL connection is closed")


if __name__ == '__main__':
    brand_urls, brand_names = get_brand_urls()
print("Writing csv file...")
with open('data/brand_urls.csv','w') as resultFile:
    wr = csv.writer(resultFile, dialect='excel')
    wr.writerow(brand_urls)
with open('data/brand_names.csv','w') as resultFile:
    wr = csv.writer(resultFile, dialect='excel')
    for key, value in brand_names.items():
        wr.writerow([key.encode('utf-8'), value.encode('utf-8')])
brand_urls = read_data('data/brand_urls.csv')
pages_list = scrape_first_page(brand_urls, 0, 372)
print("Finished writing csv file...")
# after brands are scraped...
print("Getting pages list...")
pages = get_url_list('data/pages.csv')
for p in pages:
    pages.remove("")
print("Scraping other pages for perfume urls...")
scrape_other_pages(pages)
print("Converting perfumes csv file to a list...")
perfumes = get_url_list('data/perfumes_2.csv')
for p in perfumes:
    perfumes.remove("")
print("Inserting perfumes html to mysqldb...")
scrape_perfume_page(perfumes)
print("Woohoo, done! Congrats!")

