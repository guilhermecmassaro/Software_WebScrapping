# %% [markdown]
# ## Importing the libraries

# %%
# Importing the Libraries

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date, time
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import time
from io import StringIO
import re
import mysql.connector
from sqlalchemy import create_engine, text
import traceback
import pyodbc    
from urllib.parse import quote_plus
import random

# %%
# Browser   

service = Service()
options = webdriver.ChromeOptions()
options.add_experimental_option("detach", True)
options.add_argument("--mute-audio")
browse = webdriver.Chrome(service=service, options=options)

# %% [markdown]
# ## Credentials

# %%
# Time Start

start_time = datetime.now()

# All Credentials to login in

login_link = 'XXXXXX'
headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'}
username = 'XXXXXX'
password = 'XXXXXX'

# Database Credentials SQL Server (Azure DataBase)

host_db = 'XXXXXX'
name_db = 'XXXXXX'
user_db = 'XXXXXX'
password_db = 'XXXXXX'
server_db = 'XXXXXX'

# Database Credentials MySQL

host_db_mysql = 'XXXXXX'
name_db_mysql = 'XXXXXX'
user_db_mysql = 'XXXXXX'
password_db_mysql = 'XXXXXX'

# %% [markdown]
# ## Defining the functions

# %%
# Functions

def login():
    """ This function is used to open the WebPage and login to the system"""
    browse.get(login_link) # Open the Web Software
    username_input = browse.find_element(By.ID, 'mat-input-0')
    username_input.send_keys(username) # Write the Username
    password_input = WebDriverWait(browse, 10).until(EC.presence_of_element_located((By.ID, 'inputPassword')))
    password_input.send_keys(password) # Write the Password
    module_choice = browse.find_elements(By.CSS_SELECTOR, 'div.mat-select-value > span > span.mat-select-min-line')

    # It will try to find the Input with Administração selected
    for elements in module_choice:
        if elements.text.strip() == 'Administração':
            elements.click()

    time.sleep(0.5)

    # It will run through the list trying to find the word Administração -- It's a repetiticion step just to make sure
    module_list = browse.find_elements(By.CSS_SELECTOR, 'span.mat-option-text')
    for module in module_list:
        if module.text == 'Administração':
            module.click()

    # It will look for the login button
    login_attempt = browse.find_element(By.XPATH, '//*[@id="theme"]/div/div/app-login/div[2]/form/div[6]/button')
    login_attempt.click()

def get_kanban_table():
    """ This function is used to navigate in the software, after the login, to go to the service's cars table"""
    wait_headers_kanban = WebDriverWait(browse, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.header.a-center')))
    time.sleep(3.5)
    headers_kanban = browse.find_elements(By.CSS_SELECTOR,'div.header.a-center')
    for elements in headers_kanban:
        columns_names = elements.text.split('\n')[0]
        if columns_names == 'Em serviço':
            elements.click()

def get_html_page():
    """ This function gets the html from the page using selenium and after transforms this HTML inside the BeautifulSoup"""
    operation_page_html = browse.page_source
    html = BeautifulSoup(operation_page_html, 'html.parser')
    return html

def filter_kanban_table():
    """ This function is used to filter the table in the service's cars table"""

    # First of all, it will wait for the stages filter and the entire HTML page to load
    stages_filter = WebDriverWait(browse, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR,'div.mat-select-value > span > span.mat-select-min-line')))
    time.sleep(2)

    # It will open the stages filter
    stages_filter = browse.find_elements(By.CSS_SELECTOR,'div.mat-select-value > span > span.mat-select-min-line')
    for elements in stages_filter:
        if elements.text.strip() == 'Em serviço, Entregue, Espera para serviço, Falta liberação, Falta vistoria, Perda total, Pronto':
            elements.click()

    # After open the filter, it will look for the stages options and unselect all of them and then select the 'Em serviço' stage
    stages_option = browse.find_elements(By.CSS_SELECTOR, 'mat-option.mat-focus-indicator > span.mat-option-text')
    for elements in stages_option:
        if elements.text.strip() == 'Desmarcar todos':
            elements.click()
        time.sleep(1)
        if elements.text.strip() == 'Em serviço':
            elements.click()
            break

def connect_to_database_sqlserver(server, user, password, database):
    """ This function is used to connect to the SQL Server database (if we have)"""
    
    try:
        # String de conexão Windows Server.
        parametros = (
            # Driver que será utilizado na conexão
            'DRIVER={ODBC Driver 17 for SQL Server};'
            # IP ou nome do servidor\Versão do SQL.
            f'SERVER={server};'
            # Porta
            'PORT=1433;'
            # Banco que será utilizado.
            f'DATABASE={database};'
            # Nome de usuário.
            f'UID={user};'
            # Senha.
            f'PWD={password}')

        # Convertendo a string para um padrão de URI HTML.
        url_db = quote_plus(parametros)

        # Conexão.
        engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % url_db)
        # Test the connection
        conn = engine.connect()
        if conn:
            print("Connection to SQL Server DB successful")
            return conn
        
    except Exception as e:
        print(e)

def connect_to_database_mysql(host, user, password, database):
    """ This function is used to connect to the MySQL database"""

    try:
        # Connect to MySQL
        engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
        # Test the connection
        conn = engine.connect()
        if conn:
            print("Connection to MySQL DB successful")
            return conn
    except Exception as e:
        print(e)

def except_error_path(error , message):
    """ This function is used to create a default path if we have some Exception inside the code"""
    e = error
    print("Error uploading data:", e)
    con_massaro_database_mysql = connect_to_database_mysql(host=host_db_mysql, user=user_db_mysql, password=password_db_mysql, database=name_db_mysql)
    df_log = pd.DataFrame({'Data' : datetime.now().date(), 'Automacao': 'Automação Em Servico','Duracao (s)': int((datetime.now() - start_time).seconds), 'Status' : f'Falha {message} - {e}'}, index=[0])
    df_log.to_sql(name='LogRefreshs', con= con_massaro_database_mysql, if_exists='append', index=False)
    con_massaro_database_mysql.close()
    print('Database Closed!')
    browse.close()

# %% [markdown]
# ## Starting the Login and Navigation to KanBan Table

# %%
try: 
    login()
    get_kanban_table()
    filter_kanban_table()

except Exception as e:
    except_error_path(e, 'Login and Navigation')
    

# %% [markdown]
# ## WebScrapping and Data Cleanning

# %%
try: 

    # Getting the table using the HTML
    html_page = get_html_page()
    # Close the browser
    browse.quit()
    
    # Selecting the HTML from table headers
    headers = html_page.select('div.content-padding > div.content-table > div.row-header > div > span')
    
    # Creating the Dataframe with the columns names
    headers_list = []
    for elements in headers:
        headers_list.append(elements.text.strip())
    df_service = pd.DataFrame(columns=headers_list)

    # Selecting the total number of cars in service to iterate
    find_number_of_cars = html_page.select('div.content-padding > div.content-table > div.scroll-content > div.content-list > div.sum-values > div.col')[0]
    number_of_cars = int(find_number_of_cars.text.strip())

    # Iterating each line of the table scrapping the data
    for index_car in range(number_of_cars):
        car_row_list = []
        # Selecting the HTML from table rows
        car_row_table = html_page.select('div.content-padding > div.content-table > div.scroll-content > div.content-list > div.row.nowrap.ng-star-inserted')[index_car]
        # Cleaning the data from each row
        for elements in car_row_table:
            text = elements.text.strip()
            if text != '' and text != '-':
                text = text.replace('.','')
                text = text.replace(',','.')
                car_row_list.append(text)
            elif text == '-':
                car_row_list.append('')
            else:
                car_row_list.append('')

        # Inserting the data from each row in the dataframe        
        df_service.loc[index_car] = car_row_list # Inserting the data in the dataframe

except Exception as e:
    except_error_path(e, 'Scraping and Cleanning')

# %% [markdown]
# ## Changing the type of data

# %%
try: 

    # Converting all Columns from Object to String
    df_service = df_service.astype({'Processo' : 'string',
                                'Seguradora': 'string',
                                'Carro' : 'string',
                                'Placa': 'string',
                                'Prisma': 'string',
                                'Prioridade': 'string',
                                'Horas' : 'string',
                                'Peças pintadas' : 'string',
                                'Franquia' : 'string',
                                'Na oficina' : 'string',
                                'Serviço' : 'string',
                                'Pronto' : 'string',
                                'Entregue' : 'string',
                                'Cadastro': 'string',
                                'Liberação' : 'string',
                                'Previsão' : 'string',
                                'Previsão2' : 'string',
                                'Prazo' : 'string'
                                })

    # Converting the String Columns to Float Columns
    df_service['Orçado'] = pd.to_numeric(df_service['Orçado'], errors='coerce')
    df_service['M.O.'] = pd.to_numeric(df_service['M.O.'], errors='coerce')
    df_service['Agregado'] = pd.to_numeric(df_service['Agregado'], errors='coerce')
    df_service['Peças'] = pd.to_numeric(df_service['Peças'], errors='coerce')
    df_service['S.T'] = pd.to_numeric(df_service['S.T'], errors='coerce')
    df_service['Material'] = pd.to_numeric(df_service['Material'], errors='coerce')

    # Converting the String Columns to Date Columns
    df_service['Entregue'] = pd.to_datetime(df_service['Entregue'], format="%d/%m/%Y" )
    df_service['Serviço'] = pd.to_datetime(df_service['Serviço'], format="%d/%m/%Y" )
    df_service['Pronto'] = pd.to_datetime(df_service['Pronto'], format="%d/%m/%Y" )
    df_service['Cadastro'] = pd.to_datetime(df_service['Cadastro'], format="%d/%m/%Y" )
    df_service['Liberação'] = pd.to_datetime(df_service['Liberação'], format="%d/%m/%Y" )
    df_service['Previsão'] = pd.to_datetime(df_service['Previsão'], format="%d/%m/%Y" )
    df_service['Previsão2'] = pd.to_datetime(df_service['Previsão2'], format="%d/%m/%Y" )
    df_service['Prazo'] = pd.to_datetime(df_service['Prazo'], format="%d/%m/%Y" )
    
except Exception as e:
    except_error_path(e,'Formatting')

# %% [markdown]
# ## Uploading and Deleting From DataBase

# %%

# Inserting the dataframe into database
try:

    # Connect to database and validating the connection
    con_massaro_database_mysql = connect_to_database_mysql(host=host_db_mysql, user=user_db_mysql, password=password_db_mysql, database=name_db_mysql)
    if con_massaro_database_mysql.closed:
        con_massaro_database_mysql = connect_to_database_mysql(host=host_db_mysql, user=user_db_mysql, password=password_db_mysql, database=name_db_mysql)
    
    # Uploading the DataFrame and replacing the table in the database
    df_service.to_sql(name='ProcessosEmServico', con=con_massaro_database_mysql, if_exists='replace', index=False)
    print(f"Data uploaded successfully.")

except Exception as e:
    except_error_path(e,'Upload and Replacing')

finally:
    """ If everything is ok we close the connection with the database and upload the Log"""

    # Upload the log
    refresh_duration = datetime.now() - start_time
    print(f'Refresh duration: {refresh_duration}')
    df_log = pd.DataFrame({'Data' : datetime.now(), 'Automacao': 'Automação EmServico','Duracao (s)': int(refresh_duration.seconds), 'Status' : 'Sucesso'}, index=[0])
    df_log.to_sql(name='LogRefreshs', con=con_massaro_database_mysql, if_exists='append', index=False)
    print(f"Log uploaded successfully.")
    # Close the connection
    con_massaro_database_mysql.close()
    print('Database Closed!')




