import subprocess
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
#import easygui
#import openpyxl
# from bs4 import BeautifulSoup
import bs4 as bs
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os
import warnings
import configparser
import requests
import time
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions
import os
import sys
import glob
from webdriver_manager.chrome import ChromeDriverManager
import re
import sqlite3
from sqlalchemy import create_engine
import shutil
def main():
    database_path=os.path.join(os.getcwd()+"\\"+"DB")
    pickle_path=os.path.join(os.getcwd()+"\\"+"pickles")
    db_engine=create_engine('sqlite:///ratemaster.db')

    url = 'http://www.salestaxindia.com/login.aspx'

    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--incognito")
    options.add_argument('--hide-scrollbars')
    options.add_argument('--disable-gpu')
    # options.add_argument('--headless')
    # driver = webdriver.Chrome(options=options, executable_path='chromedriver.exe')
    # driver = webdriver.Chrome(options=options, executable_path=ChromeDriverPath)
    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
    driver.maximize_window()
    driver.get(url)
    driver.implicitly_wait(10)


    driver.find_element_by_xpath('//*[@id="Login1_UserName"]').send_keys('PuneInstavat')

    driver.find_element_by_xpath('//*[@id="Login1_Password"]').send_keys('pung$t9')
    driver.find_element_by_xpath('//*[@id="Login1_LoginButton"]').click()
    driver.implicitly_wait(5)

    b=driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_HyperLink1"]')
    url=b.get_property('href')
    driver.get(url)
    driver.implicitly_wait(10)
    s1 = Select(driver.find_element_by_id('ContentPlaceHolder1_DropDownList1'))
    s1.select_by_visible_text('CGST')
    time.sleep(2)
    s2 = Select(driver.find_element_by_id('ContentPlaceHolder1_DropDownList2'))
    s2.select_by_visible_text('Cgst')
    time.sleep(2)
    s3 = Select(driver.find_element_by_id('ContentPlaceHolder1_DropDownList3'))
    s3.select_by_visible_text('Notification')
    time.sleep(5)



    url_1='http://www.salestaxindia.com/TreeMenu.aspx?node=124424&Not=1'
    url_2='http://www.salestaxindia.com/TreeMenu.aspx?node=124425&Not=1'
    url_3='http://www.salestaxindia.com/TreeMenu.aspx?node=124426&Not=1'
    url_4='http://www.salestaxindia.com/TreeMenu.aspx?node=124427&Not=1'
    url_5='http://www.salestaxindia.com/TreeMenu.aspx?node=124428&Not=1'
    url_6='http://www.salestaxindia.com/TreeMenu.aspx?node=124429&Not=1'
    url_12='http://www.salestaxindia.com/TreeMenu.aspx?node=124431&Not=1'
    url_13='http://www.salestaxindia.com/TreeMenu.aspx?node=124441&Not=1'
    url_14='http://www.salestaxindia.com/TreeMenu.aspx?node=124440&Not=1'
    driver.get(url_1)
    table = driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_lbl4"]/table[1]').get_attribute('outerHTML')
    soup = bs.BeautifulSoup(table, 'html.parser')
    df_1 = pd.read_html(str(soup))[0]
    df_1.drop(index=1,inplace=True)
    #df_1.drop(index=2,inplace=True)
    df_1.columns=df_1.iloc[0]
    df_1.drop(index=0,inplace=True)
    df_1.reset_index(inplace=True,drop=True)


    driver.get(url_2)
    table = driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_lbl4"]/table').get_attribute('outerHTML')
    soup = bs.BeautifulSoup(table, 'html.parser')
    df_2 = pd.read_html(str(soup))[0]
    df_2.drop(index=1,inplace=True)
    #df_2.drop(index=2,inplace=True)
    df_2.columns=df_2.iloc[0]
    df_2.drop(index=0,inplace=True)
    df_2.reset_index(inplace=True,drop=True)


    driver.get(url_3)
    table = driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_lbl4"]/table').get_attribute('outerHTML')
    soup = bs.BeautifulSoup(table, 'html.parser')
    df_3 = pd.read_html(str(soup))[0]
    df_3.drop(index=1,inplace=True)
    #df_3.drop(index=2,inplace=True)
    df_3.columns=df_3.iloc[0]
    df_3.drop(index=0,inplace=True)
    df_3.reset_index(inplace=True,drop=True)

    driver.get(url_4)
    table = driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_lbl4"]/table').get_attribute('outerHTML')
    soup = bs.BeautifulSoup(table, 'html.parser')
    df_4 = pd.read_html(str(soup))[0]
    df_4.drop(index=1,inplace=True)
    #df_4.drop(index=2,inplace=True)
    df_4.columns=df_4.iloc[0]
    df_4.drop(index=0,inplace=True)
    df_4.reset_index(inplace=True,drop=True)

    driver.get(url_5)
    table = driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_lbl4"]/table').get_attribute('outerHTML')
    soup = bs.BeautifulSoup(table, 'html.parser')
    df_5 = pd.read_html(str(soup))[0]
    df_5.drop(index=1,inplace=True)
    #df_5.drop(index=2,inplace=True)
    df_5.columns=df_5.iloc[0]
    df_5.drop(index=0,inplace=True)
    df_5.reset_index(inplace=True,drop=True)

    driver.get(url_6)
    table = driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_lbl4"]/table').get_attribute('outerHTML')
    soup = bs.BeautifulSoup(table, 'html.parser')
    df_6 = pd.read_html(str(soup))[0]
    df_6.drop(index=1,inplace=True)
    #df_6.drop(index=2,inplace=True)
    df_6.columns=df_6.iloc[0]
    df_6.drop(index=0,inplace=True)
    df_6.reset_index(inplace=True,drop=True)

    driver.get(url_12)
    table = driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_lbl4"]/table[1]').get_attribute('outerHTML')
    soup = bs.BeautifulSoup(table, 'html.parser')
    df_7 = pd.read_html(str(soup))[0]
    df_7.drop(index=1,inplace=True)
    #df_7.drop(index=2,inplace=True)
    df_7.columns=df_7.iloc[0]
    df_7.drop(index=0,inplace=True)
    df_7.reset_index(inplace=True,drop=True)

    driver.get(url_13)
    table = driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_lbl4"]/table').get_attribute('outerHTML')
    soup = bs.BeautifulSoup(table, 'html.parser')
    df_8 = pd.read_html(str(soup))[0]
    df_8.drop(index=1,inplace=True)
    #df_8.drop(index=2,inplace=True)
    df_8.columns=df_8.iloc[0]
    df_8.drop(index=0,inplace=True)
    df_8.reset_index(inplace=True,drop=True)

    driver.get(url_14)
    table = driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_lbl4"]/table[1]').get_attribute('outerHTML')
    soup = bs.BeautifulSoup(table, 'html.parser')
    df_9 = pd.read_html(str(soup))[0]
    df_9.drop(index=1,inplace=True)
    #df_9.drop(index=2,inplace=True)
    df_9.columns=df_9.iloc[0]
    df_9.drop(index=0,inplace=True)
    df_9.reset_index(inplace=True,drop=True)

    def post_processing(df_func):
        try:
            lstB=list(df_func["S.No."])
        except:
            lstB=list(df_func["S. No."])
        lstA=list(df_func["Chapter / Heading / Sub-heading / Tariff item"])
        lstC=list(df_func["Description of Goods"])
        lst1=[]


        for i in range(len(lstA)):
            df=pd.DataFrame()
            df["S.No."]=None
            df["Chapter / Heading / Sub-heading / Tariff item"]=None
            df["Description of Goods"]=None

            if str(lstA[i]).find(",")!=-1 and (str(lstA[i]).find("other")==-1) and (str(lstA[i]).find("Except")==-1) and (str(lstA[i]).find("or")==-1):
                lst_1=str(lstA[i]).split(",",maxsplit=50)
                lst_1=[str(x).replace(" ","") for x in lst_1]
                df["Chapter / Heading / Sub-heading / Tariff item"]=pd.Series(lst_1)
                df["S.No."]=lstB[i]
                df["Description of Goods"]=lstC[i]
            elif str(lstA[i]).find(",")!=-1 and (str(lstA[i]).find("or")!=-1)  and (str(lstA[i]).find("other")==-1) and (str(lstA[i]).find("Except")==-1):
                
                lst=str(lstA[i]).split(",")
                lst_1=[]
                for x in lst:
                    if x.find("or")!=-1:
                        lst_1.append(str(x.split("or")[0]).replace(" ",""))
                        lst_1.append(str(x.split("or")[1]).replace(" ",""))
                    else:
                        lst_1.append(str(x).replace(" ",""))
                df["Chapter / Heading / Sub-heading / Tariff item"]=pd.Series(lst_1)
                df["S.No."]=lstB[i]
                df["Description of Goods"]=lstC[i]
            elif str(lstA[i]).find("or")!=-1 and str(lstA[i]).find(",")==-1  and (str(lstA[i]).find("other")==-1) and (str(lstA[i]).find("Except")==-1):
                lst_1=str(lstA[i]).split("or",maxsplit=50)
                lst_1=[str(x).replace(" ","") for x in lst_1]
                df["Chapter / Heading / Sub-heading / Tariff item"]=pd.Series(lst_1)
                df["S.No."]=lstB[i]
                df["Description of Goods"]=lstC[i]

            elif str(lstA[i]).find("to")!=-1 and str(lstA[i]).find(",")==-1  and (str(lstA[i]).find("other")==-1) and (str(lstA[i]).find("Except")==-1):
                

                start=int(str(lstA[i]).split("to")[0])
                stop=int(str(lstA[i]).split("to")[-1])
                lst_1=[str(x).replace(" ","") for x in range(start,stop+1)]
                df["Chapter / Heading / Sub-heading / Tariff item"]=pd.Series(lst_1)
                df["S.No."]=lstB[i]
                df["Description of Goods"]=lstC[i]
            elif str(lstA[i]).find("//")!=-1  and (str(lstA[i]).find("other")==-1) and (str(lstA[i]).find("Except")==-1):
                

                start=str(lstA[i]).split("//")[0]
                stop=str(lstA[i]).split("//")[-1]
                lst_1=[x for x in range(start,stop+1)]
                df["Chapter / Heading / Sub-heading / Tariff item"]=pd.Series(lst_1)
                df["S.No."]=lstB[i]
                df["Description of Goods"]=lstC[i]       

            else:
                
                df["Chapter / Heading / Sub-heading / Tariff item"]=pd.Series(lstA[i].replace(" ",""))
                df["S.No."]=pd.Series(lstB[i])
                df["Description of Goods"]=pd.Series(lstC[i]) 



            regex=r"\d{2} \d{2} \d{4}"
            df.reset_index(inplace=True,drop=True)
            for j in range(len(df)):
                if re.match(regex,str(df["Chapter / Heading / Sub-heading / Tariff item"][j])):
                    print(j,df["Chapter / Heading / Sub-heading / Tariff item"][j])
                    df["Chapter / Heading / Sub-heading / Tariff item"][j]=df["Chapter / Heading / Sub-heading / Tariff item"][j].replace(" ","")



            lst1.append(df)
        return(pd.concat(lst1,ignore_index=True))             


    df_1=post_processing(df_1)
    df_2=post_processing(df_2)
    df_3=post_processing(df_3)
    df_4=post_processing(df_4)
    df_5=post_processing(df_5)
    df_6=post_processing(df_6)
    df_7=post_processing(df_7)
    now=datetime.now()
    now=datetime.strftime(now,"%d_%m_%Y %H_%M_%S")
    now=str(now).split(maxsplit=2)
    now="_".join(now)
    excelwriter=pd.ExcelWriter("extracted_"+str(now)+".xlsx",engine='xlsxwriter')
    df_1.to_sql("ScheduleI", db_engine, if_exists="replace",index=False) 
    df_2.to_sql("ScheduleII", db_engine, if_exists="replace",index=False) 
    df_3.to_sql("ScheduleIII", db_engine, if_exists="replace",index=False) 
    df_4.to_sql("ScheduleIV", db_engine, if_exists="replace",index=False) 
    df_5.to_sql("ScheduleV", db_engine, if_exists="replace",index=False) 
    df_6.to_sql("ScheduleVI", db_engine, if_exists="replace",index=False) 
    df_7.to_sql("Notification2", db_engine, if_exists="replace",index=False)
    df_8.to_sql("Notification11", db_engine, if_exists="replace",index=False)
    df_9.to_sql("Notification12", db_engine, if_exists="replace",index=False)
    now=datetime.now()
    current_month=str(datetime.strftime(now,"%b'%y"))
    if os.path.exists(database_path+"//"+current_month):
        pass
    else:
        os.makedirs(database_path+"//"+current_month)
        shutil.copy('ratemaster.db', database_path+"//"+current_month)
        shutil.copy(pickle_path+"//"+"hsn_dict.pkl", database_path+"//"+current_month)



    df_1.to_excel(excelwriter,"Schedule I - 2.5%",index=None)
    df_2.to_excel(excelwriter,"Schedule II - 6%",index=None)
    df_3.to_excel(excelwriter,"Schedule III - 9%",index=None)
    df_4.to_excel(excelwriter,"Schedule IV - 14%",index=None)
    df_5.to_excel(excelwriter,"Schedule V - 1.5%",index=None)
    df_6.to_excel(excelwriter,"Schedule VI - 0.125%",index=None)
    df_7.to_excel(excelwriter,"Notification No.2 2017",index=None)
    df_8.to_excel(excelwriter,"Notification No.12 2017",index=None)
    df_9.to_excel(excelwriter,"Notification No.11 2017",index=None)

    excelwriter.save()
    print("Processing Completed")
#main()
