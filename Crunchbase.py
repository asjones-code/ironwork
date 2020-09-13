#packages
import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.proxy import Proxy, ProxyType
#from selenium.webdriver.firefox.options import Options
#from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import re
import os
import time
from sqlalchemy import create_engine
import FreeProxy

waitarray = [3,5,7,9, 30]
scrapenice = np.random.choice (waitarray)
myProxy = FreeProxy(country_id=['US']).get()

proxy = Proxy({
    'proxyType': ProxyType.MANUAL,
    'httpProxy': myProxy,
    'ftpProxy': myProxy,
    'sslProxy': myProxy,
    })

#SQLALCHEMY
engine = create_engine("postgresql+psycopg2://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c")




options = FirefoxOptions()
options.add_argument('--no-sandbox')
options.add_argument("--headless")
driver = webdriver.Firefox(options=options, proxy=proxy, executable_path=os.environ.get("GECKODRIVER_PATH"),firefox_binary=os.environ.get("FIREFOX_BIN"))





global soup

#Settings for your headless browser (Firefox/Selenium)


companyinfo = pd.read_sql_table('joblist', 'postgres://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c')  
companyinfo=companyinfo['company']
companyinfo= companyinfo.drop_duplicates()


def crunchy(company):
    try:
        og = company
        company=company.replace(' ', '-')
        company=company.lower()
        crunch_url="https://www.crunchbase.com/organization/" + company
        driver.get(crunch_url)
        soup = BeautifulSoup(driver.page_source, 'lxml') 
        result_div = soup.find('p').get_text()    
    except AttributeError:
        result_div="not found on crunchbase"
        driver.quit()
        
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
        diviz = x/4
        if diviz.is_integer():
            print ('pause for 2 min')
            myProxy = FreeProxy(country_id=['US'], rand= True).get()
            print(myProxy)
            time.sleep(120)
            
        
        

        
        
    except KeyError:
        x+=1
        continue




companydf.to_sql('companylist', con = engine, if_exists = 'append', chunksize = 1000, index=False)






