#packages
import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import re
import os
import time
from sqlalchemy import create_engine

waitarray = [3,5,7,9]
scrapenice = np.random.choice (waitarray)


#SQLALCHEMY
engine = create_engine("postgresql+psycopg2://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c")


global soup

#Settings for your headless browser (Firefox/Selenium)
opts = Options()
opts.headless = True
fp = webdriver.FirefoxProfile()
driver = webdriver.Firefox(fp, options=opts)


companyinfo = pd.read_sql_table('joblist', 'postgres://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c')  
companyinfo=companyinfo['company']
companyinfo= companyinfo.drop_duplicates()


def crunchy(company):
    try:
        driver = webdriver.Firefox(fp, options=opts)
        og = company
        company=company.replace(' ', '-')
        company=company.lower()
        crunch_url="https://www.crunchbase.com/organization/" + company
        driver.get(crunch_url)
        soup = BeautifulSoup(driver.page_source, 'lxml') 
        result_div = soup.find('p').get_text()    
    except AttributeError:
        result_div="not found on crunchbase"
        
    return og, str(result_div)
    
    

companydf = pd.DataFrame({"companyname" : [], "companydesc" : []},
                     index =[])

x=0



while x<len(companyinfo):
    try:
        time.sleep(scrapenice)
        companydf.loc[x] = (crunchy(companyinfo[x]))
        print(companydf.loc[x])
        
        time.sleep(scrapenice)
        

        x+=1
        
    except KeyError:
        x+=1
        continue




companydf.to_sql('companylist', con = engine, if_exists = 'append', chunksize = 1000, index=False)






