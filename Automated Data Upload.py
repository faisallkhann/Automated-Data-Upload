from typing import Counter
import snowflake.connector
import gspread
import pandas as pd
import gspread_dataframe as gd
import time
import winsound
import os
from colorama import Fore, Back, Style

#--------------Settings------------------------
hours=30 #<-----------Duration between two consecutive data upload cycle in minutes
cycle=10 #<-----------number of times data upload cycle will run
auto_shutdown = 1 #<--------------- To Turn off the Automated Shutdown Feature please use 1/0 (ON = 1 & OFF = 0)
#----------------------------------------------

if(auto_shutdown==1):
    print(Fore.RED + '''


 █████╗ ██╗   ██╗████████╗ ██████╗ ██████╗  █████╗ ████████╗ █████╗     ██╗   ██╗██████╗ ██╗      ██████╗  █████╗ ██████╗ ███████╗██████╗ 
██╔══██╗██║   ██║╚══██╔══╝██╔═══██╗██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗    ██║   ██║██╔══██╗██║     ██╔═══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗
███████║██║   ██║   ██║   ██║   ██║██║  ██║███████║   ██║   ███████║    ██║   ██║██████╔╝██║     ██║   ██║███████║██║  ██║█████╗  ██████╔╝
██╔══██║██║   ██║   ██║   ██║   ██║██║  ██║██╔══██║   ██║   ██╔══██║    ██║   ██║██╔═══╝ ██║     ██║   ██║██╔══██║██║  ██║██╔══╝  ██╔══██╗
██║  ██║╚██████╔╝   ██║   ╚██████╔╝██████╔╝██║  ██║   ██║   ██║  ██║    ╚██████╔╝██║     ███████╗╚██████╔╝██║  ██║██████╔╝███████╗██║  ██║
╚═╝  ╚═╝ ╚═════╝    ╚═╝    ╚═════╝ ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝     ╚═════╝ ╚═╝     ╚══════╝ ╚═════╝ ╚═╝  ╚═╝╚═════╝ ╚══════╝╚═╝  ╚═╝ v2.0
Automated Shutdown Feature: ON                                                                                    Developed by Faisal Khan              
To Turn off Automated Shutdown feature, please look into the source code.
''')
elif(auto_shutdown==0):
    print(Fore.RED + '''


 █████╗ ██╗   ██╗████████╗ ██████╗ ██████╗  █████╗ ████████╗ █████╗     ██╗   ██╗██████╗ ██╗      ██████╗  █████╗ ██████╗ ███████╗██████╗ 
██╔══██╗██║   ██║╚══██╔══╝██╔═══██╗██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗    ██║   ██║██╔══██╗██║     ██╔═══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗
███████║██║   ██║   ██║   ██║   ██║██║  ██║███████║   ██║   ███████║    ██║   ██║██████╔╝██║     ██║   ██║███████║██║  ██║█████╗  ██████╔╝
██╔══██║██║   ██║   ██║   ██║   ██║██║  ██║██╔══██║   ██║   ██╔══██║    ██║   ██║██╔═══╝ ██║     ██║   ██║██╔══██║██║  ██║██╔══╝  ██╔══██╗
██║  ██║╚██████╔╝   ██║   ╚██████╔╝██████╔╝██║  ██║   ██║   ██║  ██║    ╚██████╔╝██║     ███████╗╚██████╔╝██║  ██║██████╔╝███████╗██║  ██║
╚═╝  ╚═╝ ╚═════╝    ╚═╝    ╚═════╝ ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝     ╚═════╝ ╚═╝     ╚══════╝ ╚═════╝ ╚═╝  ╚═╝╚═════╝ ╚══════╝╚═╝  ╚═╝ v2.0
Automated Shutdown Feature: OFF                                                                                   Developed by Faisal Khan              
To Turn on Automated Shutdown feature, please look into the source code.
''')

userid=input("Please Enter Your Snowflake User ID: ")
warehouse_name=input("Please Enter the Warehouse Name: ")
account_ID=input("Please Enter the Account ID: ")

ctx = snowflake.connector.Connect(user=userid,warehouse=warehouse_name, account=account_ID,authenticator="externalbrowser")
cur = ctx.cursor()
api = gspread.service_account(filename="api\\api.json") #<-----------location of the Google Service Account API

t=hours*60 
a=0

def error_sound():
    c = 0
    while c<4:
        winsound.PlaySound("SystemExit", winsound.SND_ALIAS)  
        c=c+1

def countdown(t):
    
    while t:
        mins, secs = divmod(t, 60)
        timer = '{:02d}:{:02d}'.format(mins, secs)
        print(timer, end="\r")
        time.sleep(1)
        t -= 1

def data_upload_1():
    try:
        sheet = api.open("Insert_SpreadSheet_Name_Here") #<-----------Please Insert the Google Sheet Name Here.
        wks = sheet.worksheet("Insert_WorkSheet_Name_Here") #<-----------Please Enter the Worksheet Name Here.
        query = open('src\\query.sql','r') #<-----------specify the location of your SQL Query File here.
        sql = query.read()
        cur.execute(sql)
        df = cur.fetch_pandas_all()
        df0 = pd.DataFrame(df)
        wks.clear()
        time.sleep(4)
        gd.set_with_dataframe(wks, df0)        
    except:
        error_sound()        
        print("Error: Data Upload 1")

def data_upload_2():
    try:
        sheet = api.open("Insert_SpreadSheet_Name_Here") #<-----------Please Insert the Google Sheet Name Here.
        wks = sheet.worksheet("Insert_WorkSheet_Name_Here") #<-----------Please Enter the Worksheet Name Here.
        query = open('src\\query.sql','r') #<-----------specify the location of your SQL Query File here.
        sql = query.read()
        cur.execute(sql)
        df = cur.fetch_pandas_all()
        df0 = pd.DataFrame(df)
        wks.clear()
        time.sleep(4)
        gd.set_with_dataframe(wks, df0)        
    except:
        error_sound()        
        print("Error: Data Upload 2")

def data_upload_append(): #<----------------This will Append the data into the Google Sheets. This type of function will not overwrite the data in the Google Sheet. 
    try:
        sheet = api.open("Insert_SpreadSheet_Name_Here") #<-----------Please Insert the Google Sheet Name Here.
        wks = sheet.worksheet("Insert_WorkSheet_Name_Here") #<-----------Please Enter the Worksheet Name Here.
        query = open('src\\query.sql','r') #<-----------specify the location of your SQL Query File here.
        sql = query.read()
        cur.execute(sql)
        df = cur.fetch_pandas_all()
        df0 = pd.DataFrame(df)
        df1 = gd.get_as_dataframe(wks, usecols=[0])
        df2 = df1.dropna(how='all')
        l = len(df2.index)+2
        gd.set_with_dataframe(wks, df0, row=l)
        time.sleep(4)
        
    except:
        error_sound()        
        print("Error: Data Upload Append")
        time.sleep(4)

while a<=cycle:
    data_upload_1()
    data_upload_2()
    data_upload_append()
    a=a+1
    print(f"Data uploaded {a} times. Please wait for next upload cycle.")
    countdown(int(t))
    print(f"Data Upload Cycle No.{a+1} in Progress. Please Wait.")

if(auto_shutdown==1):
    os.system("shutdown /s /t 10")
