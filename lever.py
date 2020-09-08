#packages
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import timedelta
from fake_useragent import UserAgent
from functools import reduce
from operator import add
import numpy as np
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import re
import requests
import time
import urllib



#SQLALCHEMY
engine = create_engine("postgresql+psycopg2://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c")

#PSYCOPG
conn = psycopg2.connect(
    """
    dbname=d3dk2h0pspg85c 
    host=ec2-54-172-173-58.compute-1.amazonaws.com 
    port=5432 
    user=dtqkynygrntpco 
    password=f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938 
    sslmode=require
    """
)


conn.set_session(autocommit=True)
cursor = conn.cursor()


#Table is already made

#query = """CREATE TABLE joblist
#(
#    index int NOT NULL PRIMARY KEY,
#    ATS varchar NOT NULL,
#    Position varchar NOT NULL, 
#    Company varchar NOT NULL,
#    Location varchar NOT NULL,
#    Remote BOOLEAN NOT NULL,
#    link varchar NOT NULL
#    timestamp DATETIME NOT NULL
    
#)
#"""
#cursor.execute(query)

#ts = "ALTER TABLE joblist ADD COLUMN added timestamp with time zone NOT NULL;"
#cursor.execute(ts)
                     


delays = [7, 4, 6, 2, 10, 19]
delay = np.random.choice(delays)

number_result=100
results=0
ua = UserAgent()
newjobs= []




def link_parser(query):
    global results

    google_url = "https://www.google.com/search?q=" + query + "&num=" + str(number_result) + "&filter=0" + "&tbs=qdr:d" + "&start="+ str(results)
    time.sleep(delay)

    response = requests.get(google_url, {"User-Agent": ua.random})
    soup = BeautifulSoup(response.text, "html.parser")

    links = []
    result_div = soup.find_all('div', attrs = {'class': 'ZINbbc'})
    for r in result_div:
    # Checks if each element is present, else, raise exception
        try:
            link = r.find('a', href = True)
        
            # Check to make sure everything is present before appending
            if link != '' :
                links.append(link['href'])
        # Next loop if one element is not present
        except:
            continue
    return links


# In[7]:


def clean_links(links):
    clean_links = []
    to_remove = []

    for i, l in enumerate(links):
        clean = re.search('\/url\?q\=(.*)\&sa',l)

        # Anything that doesn't fit the above pattern will be removed
        if clean is None:
            to_remove.append(i)
            continue
        clean_links.append(clean.group(1))
    return clean_links




def jobinfo(link):
    isremote = 0
    jl_url = link
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    response = requests.get(link, {"User-Agent": ua.random} )
    soup = BeautifulSoup(response.text, "html.parser")
    if "greenhouse" not in jl_url:
        ATS = 'Lever'
        company = (soup.title.get_text()).split(' - ')[0]
        position = (soup.title.get_text()).split(' - ')[1]
        if len(company) > 2:
            position = position + " - " +((soup.title.get_text()).split(' - ')[2])
            location = soup.find("div", {"class" : "sort-by-time posting-category medium-category-label"}).get_text()
            location = location.replace('/',' ').strip()  
    else:
        ATS = 'Greenhouse'
        position =str(soup.title.get_text()).replace('Job Application for', '').strip()
        company = str(position.split(' at ')[1]).strip()
        position= str(position.split(' at ')[0]).strip()
        location = soup.find("div", {"class" : "location"})
        clean = re.compile('<.*?>')
        location = str(location)
        location = re.sub(clean, '', location)
        location = location.replace('\n',' ').strip()  
    
    if "remote" in location.lower():
        isremote=1
    else:
        isremote=0
        
    return ATS, position, company, location, jl_url, isremote, dt_string




links_cleaned = clean_links(link_parser('"apply for this job" inurl:jobs.lever.co'))
print(links_cleaned)
newjobs.append(links_cleaned)

print("first query complete. Please wait 90 seconds")
time.sleep(90)

while len(links_cleaned) != 0:   
    delay = np.random.choice(delays)   
    results += 100
    print(results)
    links_cleaned = clean_links(link_parser('"apply for this job" inurl:jobs.lever.co'))
    print(links_cleaned)
    newjobs.append(links_cleaned)
    print(delay)
    time.sleep(delay)


print("Scrape Responsibly. Lets wait 5 minutes")
time.sleep(60)
print("Scrape Responsibly. Lets wait 4 minutes")
time.sleep(60)
print("Scrape Responsibly. Lets wait 3 minutes")
time.sleep(60)
print("Scrape Responsibly. Lets wait 2 minutes")
time.sleep(60)
print("Scrape Responsibly. Lets wait 1 minute")
time.sleep(60)

results=0

links_cleaned = clean_links(link_parser('"view all jobs" inurl:greenhouse.io'))
print(links_cleaned)
newjobs.append(links_cleaned)

print("first query complete. Please wait 90 seconds")
time.sleep(90)


while len(links_cleaned) != 0:   
    delay = np.random.choice(delays)   
    results += 100
    print(results)
    links_cleaned = clean_links(link_parser('"view all jobs" inurl:greenhouse.io'))
    print(links_cleaned)
    newjobs.append(links_cleaned)
    time.sleep(3)



newjobs = reduce(add, newjobs)
len(newjobs)



testdf = pd.DataFrame({"ats" : [], "position" : [], "company" : [], "location":[], "link" : [], "remote" : [], "added" : []},
                     index =[])
x=len(newjobs)
i=0
while i<x:
    try:
        delay = np.random.choice(delays) 
        print(jobinfo(newjobs[i]))
        testdf.loc[i] = (jobinfo(newjobs[i]))
        i+=1
        #print(delay)
        time.sleep(3)

    except:
        i+=1
        continue


testdf.to_sql('joblist', con = engine, if_exists = 'append', chunksize = 1000)








