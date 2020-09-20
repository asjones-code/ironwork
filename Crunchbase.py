#packages
import numpy as np
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


waitarray = [3,5,7,9, 30]
scrapenice = np.random.choice (waitarray)

import pandas as pd
old_company_title_list = pd.read_sql_table('companylist', 'postgres://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c')  
new_company_title_list = pd.read_sql_table('joblist', 'postgres://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c') 

new_company_title_list = new_company_title_list['company'].drop_duplicates()
old_company_title_list = old_company_title_list['companyname'].drop_duplicates()
update_company_title_list = np.setdiff1d(new_company_title_list, old_company_title_list, assume_unique=False)
print(str(len(update_company_title_list)) + " new companies to add to the database")



options = FirefoxOptions()
options.add_argument('--no-sandbox')
options.add_argument("--headless")





global soup

#Settings for your headless browser (Firefox/Selenium)

from fake_useragent import UserAgent
import requests

delays = [7, 4, 6, 2, 10, 13]
delay = np.random.choice(delays)

number_result=3
ua = UserAgent()


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




def crunchy(company):
    try:
        driver = webdriver.Firefox(options=options, executable_path=os.environ.get("GECKODRIVER_PATH"),firefox_binary=os.environ.get("FIREFOX_BIN"))      
        og = company
        company=company.replace(' ', '-')
        company=company.lower()
        crunch_url="https://www.crunchbase.com/organization/" + company
            .get(crunch_url)
        soup = BeautifulSoup(driver.page_source, 'lxml') 
        result_div = soup.find('p').get_text()   
        employees = soup.find("a", {"class" : "component--field-formatter field-type-enum link-accent ng-star-inserted"}).get_text()
        ipostatus = soup.find("span", {"component--field-formatter field-type-enum ng-star-inserted"}).get_text()
        #industry = soup.find("mat-chip", {"mat-chip mat-focus-indicator mat-primary mat-standard-chip ng-star-inserted"}).get_text()
        industry = soup.find("div", {"class" :"mat-chip-list-wrapper"})
        cleanr = re.compile('<.*?>')
        industry = str(industry)
        industry = re.sub(cleanr, ' ', industry)
        industry= industry.strip()
        industry = re.sub(' +', ' ', industry) 


    #industry = re.sub(r"\B([A-Z])", r" \1", industry)
    except AttributeError:
        result_div="not found on crunchbase"
        employees="not found on crunchbase"
        ipostatus="not found on crunchbase"
        industry="not found on crunchbase"
        
    driver.close() 
    return og, str(result_div), employees, ipostatus, industry
    
        
        
        
        
    
    

companydf = pd.DataFrame({"companyname" : [], "companydesc" : [], "employees" : [], "ipo_status" : [], "industry" : []},
                     index =[])


z=0
while z<len(update_company_title_list):
    try:
        cblink = link_parser(update_company_title_list[z] + 'crunchbase')
        tocb=clean_links(cblink)[0]
        print(tocb)
        companydf.loc[z]=crunchy(tocb)
        companydf.loc[z]
        z+=1
    except IndexError:
        print('Index Error. No clue why...')
        z+=1
        pass
            
        
        
    except KeyError:
        print('key error')
        z+=1
        continue
    except ConnectionRefusedError:
        print('FUCK! wait 5 minutes')
        time.sleep(300)
        continue
    except WebDriverException:
        break
        #companydf.loc[x] = (crunchy(companyinfo[x]))
        #print(companydf.loc[x])
        
       # time.sleep(scrapenice)
        #x+=1
        #diviz = x/4
        #if diviz.is_integer():
            #print ('pause for 2 min')
            #time.sleep(120)
        
        

#SQLALCHEMY
engine = create_engine("postgresql+psycopg2://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c")



companydf.to_sql('companylist', con = engine, if_exists = 'append', chunksize = 1000, index=False)






