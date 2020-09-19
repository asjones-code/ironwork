#!/usr/bin/env python
# coding: utf-8

# # Salary Data
# This is the section where we acquire as much salary data as possible

# # Step 1:
# Comparing old and new arrays

# In[1]:


import pandas as pd
old_job_title_list = pd.read_sql_table('salarylist', 'postgres://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c')  
new_job_title_list = pd.read_sql_table('joblist', 'postgres://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c')  


# In[2]:


new_job_title_list = new_job_title_list['position'].drop_duplicates()
old_job_title_list = old_job_title_list['position'].drop_duplicates()


# In[3]:


import numpy as np
new_job_title_list = np.array(new_job_title_list)
old_job_title_list = np.array(old_job_title_list)
update_job_title_list = np.setdiff1d(new_job_title_list, old_job_title_list, assume_unique=True)
print(str(len(update_job_title_list)) + " new salaries to add to the database")


# # Step 2:
# Creating functions to scrape salary data

# In[69]:


from fake_useragent import UserAgent
import requests
import time
from bs4 import BeautifulSoup
import re
from selenium import webdriver
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.proxy import Proxy, ProxyType



number_result=3
ua = UserAgent()

options = FirefoxOptions()
options.add_argument('--no-sandbox')
options.add_argument("--headless")



# In[ ]:





# In[70]:



def link_parser(query):
    global results


    google_url = "https://www.google.com/search?q=" + query + "&num=" + str(number_result) 

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

#cleans google results

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


# In[71]:


def glassdoor_salary_tool(url, x):
    try:
        position =update_job_title_list[x]
        driver = webdriver.Firefox(options=options, executable_path=os.environ.get("GECKODRIVER_PATH"),firefox_binary=os.environ.get("FIREFOX_BIN"))      
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        salary = soup.find("p", {"class": "css-oaxin6 my-0"}).get_text()
        salary = re.findall('\$\d+,\d+', salary)[0]
        salary = str(salary)
        salary = salary.replace('[', ' ')
        salary = salary.replace(']', ' ')

    except AttributeError:
        print('no Salary found')
        pass
    driver.close() 

    return position, salary


# # Step 3:
# passing array to query and getting results

# In[72]:


salarytodb = pd.DataFrame({"position" : [], "salary" : []},
                     index =[])


# In[73]:





# In[74]:



#glassdoor_salary_tool(clean_links(link_parser(update_job_title_list[1] + " inurl:glassdoor.com/Salaries/"))[0], 1)


# In[31]:


global w=0
y= len(update_job_title_list)
while w < y: 
    try:
        print(str(w) + "out of" + str(y))
        delays = [7, 4, 6, 2]
        delay = np.random.choice(delays)
        #print(update_job_title_list[w])
        gdlink = link_parser(update_job_title_list[w] + ' inurl:glassdoor.com/Salaries/')
        todf = glassdoor_salary_tool(clean_links(gdlink)[0], w)
        print(todf)
        salarytodb.loc[w] = todf
        #print('wait ' + str(delay) + ' seconds')
        #time.sleep(delay)
        w+=1
        if w%50 = 0:
            print('wait 5 minutes')
            time.sleep(300)
    except :
        print('Attribute error - empty salary')
        print('wait ' + str(delay) + ' seconds')
        time.sleep(delay)
        w+=1
        pass
        
        


# In[ ]:


#salarytodb


# In[ ]:


salarytodb = salarytodb.apply(lambda s:s.str.replace("$", ""))
salarytodb = salarytodb.replace(',','', regex=True)


# In[ ]:


from sqlalchemy import create_engine
engine = create_engine("postgresql+psycopg2://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c")
salarytodb.to_sql('salarylist', con = engine, if_exists = 'append', chunksize = 1000, index=False)


# In[ ]:


#updated_salary = pd.read_sql_table('salarylist', 'postgres://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c')  


# In[ ]:


#updated_salary=updated_salary['salary'].astype(int)


# In[ ]:





# In[ ]:



#updated_salary.plot.hist()


# In[ ]:


#tohist.plot.hist()


# In[ ]:




