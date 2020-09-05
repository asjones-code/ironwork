#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time
import requests
from bs4 import BeautifulSoup
import urllib
from selenium import webdriver
from fake_useragent import UserAgent
import numpy as np
from operator import add
from functools import reduce
import re
import pandas as pd



# In[2]:


delays = [7, 4, 6, 2, 10, 19]
delay = np.random.choice(delays)

number_result=100
results=0
ua = UserAgent()
newjobs= []


# In[3]:


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


# In[4]:


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


# In[5]:


def jobinfo(link):
    jl_url = link
    response = requests.get(link, {"User-Agent": ua.random} )
    soup = BeautifulSoup(response.text, "html.parser")
    if "greenhouse" not in jl_url:
        location = soup.find("div", {"class" : "sort-by-time posting-category medium-category-label"}).get_text()   
    else:
        location = soup.find("div", {"class" : "location"})
        clean = re.compile('<.*?>')
        location = str(location)
        location = re.sub(clean, '', location)
        
    return soup.title.get_text(), location, jl_url


# In[6]:


links_cleaned = clean_links(link_parser('"apply for this job" inurl:"lever.co"'))
print(links_cleaned)
newjobs.append(links_cleaned)



# In[7]:


while len(links_cleaned) != 0:   
    delay = np.random.choice(delays)   
    results += 100
    print(results)
    links_cleaned = clean_links(link_parser('"apply for this job" inurl:"lever.co"'))
    print(links_cleaned)
    newjobs.append(links_cleaned)
    print(delay)
    time.sleep(delay)


 

newjobs = reduce(add, newjobs)
len(newjobs)


# In[ ]:





# In[ ]:



                                        


# In[ ]:





# In[ ]:





# In[8]:


testdf = pd.DataFrame({"job" : [], "location" : [], "link" : []},
                     index =[])
x=len(newjobs)
i=0
wtf = (jobinfo(newjobs[i]))
while i<x:
    try:
        print(jobinfo(newjobs[i]))
        i+=1
        testdf.loc[i] = (jobinfo(newjobs[i]))

    except:
        continue


# In[ ]:


testdf


# In[ ]:


testdf.to_csv('export.csv')


# In[ ]:




