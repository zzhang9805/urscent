#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
import os
import sys
from collections import defaultdict
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import requests
import mysql.connector
import time
from datetime import datetime


# In[2]:


def get_attributes(url1,html1):
    """
    Input: perfume webpage html content
    Output: a dictionary of what I need for the item matrix
    I use # as delimiter for components of any attribute
    """
    html = html1
    url = url1
    soup = BeautifulSoup(html, 'html.parser')
    attributes = {'brand': '', 'theme': '', 'note': '', 'gender': '', 'perfumer': '', 'tags': '', 
                 'perfume_id': '', 'item_name': '', 'url': '', 'topnotes': '', 'heartnotes': '', 'basenotes': '', 'description': ''}
    for link in soup.find('ul', {'class': 'item_info'}):
        for sublink in link.find_all('a', href=True):
            if re.match('(/pinpai/)', sublink.attrs['href']):
                attributes['brand'] = sublink.text
            if re.match('(/xiangdiao/)', sublink.attrs['href']):
                attributes['theme'] = sublink.text
            if re.match('(/qiwei/)', sublink.attrs['href']):
                attributes['note'] = attributes['note'] + sublink.text + '#'
            if re.match('(/tiaoxiangshi/)', sublink.attrs['href']):
                attributes['perfumer'] = sublink.text
            if re.search('(field=attrib)', sublink.attrs['href']): # re.match() will match from the beginning, re.search() looks for any location where this RE matches
                attributes['gender'] = attributes['gender'] + sublink.text + '#'
            if re.search('(field=tag)', sublink.attrs['href']):
                attributes['tags'] = attributes['tags'] + sublink.text + '#'
    attributes['perfume_id'] = ''.join(re.findall(r"[0-9]+",url,re.S))[0:6]
    attributes['item_name'] = soup.find('h1').text
    attributes['url'] = url
    perfume_text = soup.find('ul', {'class': 'item_info'}).get_text()
    if "前调" in perfume_text and "中调" in perfume_text and "后调" in perfume_text: #any of the three not presented then regard as insignificant data
        if "属性" in perfume_text or "标签" in perfume_text or "调香师" in perfume_text:
            attributes['topnotes'] = perfume_text[perfume_text.index("前调")+3:perfume_text.index("中调")].split()
            attributes['heartnotes'] = perfume_text[perfume_text.index("中调")+3:perfume_text.index("后调")].split()
            attributes['basenotes'] = perfume_text[perfume_text.index("后调")+3:(perfume_text.index("：",perfume_text.index("后调：")+3)-3)].split()
        else:  
            attributes['topnotes'] = perfume_text[perfume_text.index("前调")+3:perfume_text.index("中调")].split()
            attributes['heartnotes'] = perfume_text[perfume_text.index("中调")+3:perfume_text.index("后调")].split()
            attributes['basenotes'] = perfume_text[perfume_text.index("后调")+3:].split()
    attributes['description'] = soup.find('li', {'class': 'desc'}).find('div', {'class': 'showmore'}).find('p').get_text().rstrip()
    attributes['note'] = attributes['note'].rstrip('#')
    attributes['gender'] = attributes['gender'].rstrip('#')
    attributes['tags'] = attributes['tags'].rstrip('#')
    attributes['topnotes'] = '#'.join(attributes['topnotes'])
    attributes['heartnotes'] = '#'.join(attributes['heartnotes'])
    attributes['basenotes'] = '#'.join(attributes['basenotes'])
    return attributes


# In[3]:


def get_comments(url1,html1):
    """
    Input: perfume webpage html content
    Output: a dictionary of perfume url and perfume comments
    I use # as delimiter for components of any attribute
    """
    html = html1
    url = url1
    soup = BeautifulSoup(html, 'html.parser')
    attributes1 = {'comments': '', 'perfume_id': '', 'url': '', 'commenter_lw': '', 'comment_votes': ''}
    attributes1['url'] = url
    attributes1['perfume_id'] = ''.join(re.findall(r"[0-9]+",url,re.S))[0:6]
    count = 0
    if soup.find('div', {'class': 'hfshow'}) != None:
        for discuss in soup.find_all('div', {'class': 'hfshow'}):
            attributes1['comments'] = attributes1['comments'] + discuss.text + '#'
        for discuss in soup.find_all('div', {'class': 'user'}):
            ct = 0
            for d in discuss.contents:
                ct += 1
                if ct == 3:
                    attributes1['commenter_lw'] = attributes1['commenter_lw'] + d['class'][1][2] + '#'
        for discuss in soup.find('li',{'id': 'itemdiscuss'}).find_all('span', {'class': 'fav_cnt'}):
            attributes1['comment_votes'] = attributes1['comment_votes'] + discuss.text + '#'
    attributes1['comments'] = attributes1['comments'].rstrip('#')
    attributes1['commenter_lw'] = attributes1['commenter_lw'].rstrip('#')
    attributes1['comment_votes'] = attributes1['comment_votes'].rstrip('#')
    return attributes1


# In[4]:


def get_ratings(url1,html1):
    """
    Input: perfume webpage html content
    Output: a dictionary of perfume ratings info
    I use % as delimiter for rating distribution
    """
    html = html1
    url = url1
    soup = BeautifulSoup(html, 'html.parser')
    attributes2 = {'url': '', 'perfume_id': '', 'score': '', 'votes': '', 'score_distribution': '', 'longevity': ''}
    attributes2['url'] = url
    attributes2['perfume_id'] = ''.join(re.findall(r"[0-9]+",url,re.S))[0:6]
    if '评分人数过少' not in soup.find('ul', {'class': 'item_score'}).text:
        attributes2['score'] = soup.find('ul', {'class': 'item_score'}).find('span', {'class': 'score'}).text.rstrip()
        attributes2['votes'] = ''.join(re.findall('[0-9]+', soup.find('ul', {'class': 'item_score'}).find('span', {'class': 'people'}).text))
        tmptext = ''
        for percentage in soup.find('ul', {'class': 'item_score'}).find_all('div', {'class': 'nows'}):
            tmptext = tmptext + percentage.text
        attributes2['score_distribution'] = tmptext
        attributes2['longevity'] = soup.find('ul', {'class': 'item_score'}).find('div', {'class': 'inbar'})['style'][6:].rstrip(';')
    return attributes2


# In[8]:


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
    cursor_query = db_connection.cursor()
    query = ("SELECT url, html FROM perfume_html_distinct order by url limit 4550")
    cursor_query.execute(query)
    records = cursor_query.fetchall()
    cursor_query.close()
    mySql_insert_query = """INSERT INTO attributes (nowdate, brand, theme, note, gender, perfumer, tags, perfume_id, item_name, url, topnotes, heartnotes, basenotes, description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
    print("Parsing attributes and store into mysqldb...")
    count = 0
    for record in records:
    ## Parse perfume attributes
        print(record[0])
        attributes = get_attributes(record[0],record[1])
        current_Date = datetime.now()
        formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S:%f')
        insert_list = [formatted_date]
        insert_tuple = tuple(insert_list + list(attributes.values()))
        cursor_insert = db_connection.cursor()
        cursor_insert.execute(mySql_insert_query, insert_tuple)
        cursor_insert.close()
        db_connection.commit()
        count += 1
        if count % 10 == 0:
            print("Inserted {} attributes".format(count))
    print("Done! You got all the attributes!")
    ### Parse perfume comments
    mySql_insert_query1 = """INSERT INTO comments VALUES (%s, %s, %s, %s, %s, %s) """
    print('Parsing comments and store into mysqldb...')
    count = 0
    for record in records:
        print(record[0])
        comments = get_comments(record[0],record[1])
        if comments != None:
            current_Date = datetime.now()
            formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S:%f')
            insert_list = [formatted_date]
            insert_tuple = tuple(insert_list + list(comments.values()))
            cursor_insert = db_connection.cursor()
            cursor_insert.execute(mySql_insert_query1, insert_tuple)
            cursor_insert.close()
            db_connection.commit()
            count += 1
            if count % 10 == 0:
                print("Inserted {} comments".format(count))
    print('Done! You got all the comments!')
    ### Parse perfume ratings
    mySql_insert_query2 = """INSERT INTO ratings VALUES (%s, %s, %s, %s, %s, %s, %s) """
    print('Parsing ratings and store into mysqldb...')
    count = 0
    for record in records:
        print(record[0])
        ratings = get_ratings(record[0],record[1])
        current_Date = datetime.now()
        formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S:%f')
        insert_list = [formatted_date]
        insert_tuple = tuple(insert_list + list(ratings.values()))
        cursor_insert = db_connection.cursor()
        cursor_insert.execute(mySql_insert_query2, insert_tuple)
        cursor_insert.close()
        db_connection.commit()
        count += 1
        if count % 10 == 0:
            print("Inserted {} ratings".format(count))
    print('Done! You got all the ratings!')
    print('This instance has finished its task!')
    db_connection.close()


# In[ ]:




