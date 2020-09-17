#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import time
from selenium import webdriver
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from bs4 import BeautifulSoup
import re
from fake_useragent import UserAgent
import requests

waitarray = [3,5,7,9,30]
waitless = [3,5,7,9]
delays = [7, 9, 15, 2, 10, 13]
delay = np.random.choice(delays)

number_result=3
ua = UserAgent()
newjobs= []

scrapenice = np.random.choice (waitarray)
scrapefast = np.random.choice (waitless)

options = FirefoxOptions()
options.add_argument('--no-sandbox')
options.add_argument("--headless")

companyarray = pd.read_sql_table('companylist', 'postgres://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c')  
totalcompany = pd.read_sql_table('joblist', 'postgres://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c')  


# In[2]:


positions = totalcompany['position']


# In[3]:


positions


# In[4]:


positions = positions.drop_duplicates()


# In[5]:


positions


# In[6]:


salarydata = pd.DataFrame({"title" : [], "salary" : [], "sample_size" : []},
                     index =[])


# In[7]:


def link_parser(query):
    global results

    google_url = "https://www.google.com/search?q=" + query + "&num=" + str(number_result) 
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


# In[8]:


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


# In[9]:





# In[10]:


def glassy(url, x):
    try:
        title =positions[x]
        driver = webdriver.Firefox(options=options, executable_path=os.environ.get("GECKODRIVER_PATH"),firefox_binary=os.environ.get("FIREFOX_BIN"))      
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        salary = soup.find("p", {"class": "css-oaxin6 my-0"}).get_text()
        test = re.findall('\$\d+,\d+', salary)
        test = str(test)
        test = test.replace('[', ' ')
        test = test.replace(']', ' ')
        
        sample_size = re.findall('\d+,\d+', salary)

    except:
        print('oops')
        pass
    driver.close() 

    return str(title), test, str(sample_size[1])


# In[11]:





# In[ ]:


w=0
while w<5:
    try:
        gdlink = link_parser(positions[w] + 'glassdoor')
        todf = glassy(clean_links(gdlink)[0], w)
        print(todf)
        salarydata[w] = todf
        print('wait vvvv seconds')
        print(delay)
        time.sleep(delay)
        w+=1
    except:
        print('Index Error. No clue why...')
        w+=1
        pass


# In[ ]:





# In[ ]:




