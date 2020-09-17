import pandas as pd
import numpy as np
import time
from selenium import webdriver
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.webdriver.firefox.options import Options as FirefoxOptions


from bs4 import BeautifulSoup
import re

options = FirefoxOptions()
options.add_argument('--no-sandbox')
options.add_argument("--headless")

waitarray = [3,5,7,9, 30]
waitless = [3,5,7,9]

scrapenice = np.random.choice (waitarray)
scrapefast = np.random.choice (waitless)

opts = Options()
opts.headless = True
fp = webdriver.FirefoxProfile()
driver = webdriver.Firefox(fp, options=opts)

companyarray = pd.read_sql_table('companylist', 'postgres://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c')  
totalcompany = pd.read_sql_table('joblist', 'postgres://dtqkynygrntpco:f8b2d26aee326c186e71fcc28ffad460d698d06e4456c41b75ffa4b315750938@ec2-54-172-173-58.compute-1.amazonaws.com:5432/d3dk2h0pspg85c')  


totalcompany= totalcompany['company']
totalcompany = totalcompany.drop_duplicates()

totalcompany = np.array(totalcompany)
companyarray = np.array(companyarray)

missing = np.setdiff1d(totalcompany, companyarray, assume_unique=True)

def crunchy(company):
    try:
        driver = webdriver.Firefox(options=options, executable_path=os.environ.get("GECKODRIVER_PATH"),firefox_binary=os.environ.get("FIREFOX_BIN"))
        og = company
        company=company.replace(' ', '-')
        company=company.lower()
        crunch_url="https://www.crunchbase.com/organization/" + company
        driver.get(crunch_url)
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
    
    
missingdf = pd.DataFrame({"companyname" : [], "companydesc" : [], "employees" : [], "ipo_status" : [], "industry" : []},
                     index =[])
                     

x=0
while x<len(missing):
    try:
        time.sleep(scrapenice)
        missingdf.loc[x] = (crunchy(missing[x]))
        print(missingdf.loc[x])
        
        time.sleep(scrapenice)
        

        x+=1
        
    except KeyError:
        x+=1
        continue
        
missingdf.to_sql('companylist', con = engine, if_exists = 'append', chunksize = 1000, index=False)