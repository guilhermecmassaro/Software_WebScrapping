# %% [markdown]
# ## Importing the Libraries

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
import unicodedata


# %% [markdown]
# ## Browser and Credentials

# %%
# Browser   

service = Service()
options = webdriver.ChromeOptions()
options.add_experimental_option("detach", True)
options.add_argument("--mute-audio") # It will mute the web page (car noises)
browse = webdriver.Chrome(service=service, options=options)

# %%
# All Credentials to login in

login_link = 'XXXXXX'
headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'}
username = 'XXXXXX'
password = 'XXXXXX'
start_time = datetime.now()

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
# ## Defining the Functions

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
    """ This function is used to navigate in the software, after the login, to go to the schedule's cars table"""

    # Wait for the WebPage load and then try to find the header on KanBan named Agendamento para serviços
    wait_headers_kanban = WebDriverWait(browse, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'app-nav-virtual-body-shop > nav.of-show-bars > ul > li')))
    headers_kanban = browse.find_elements(By.CSS_SELECTOR, 'app-nav-virtual-body-shop > nav.of-show-bars > ul > li')
    for elements in headers_kanban:
        if elements.text.strip().lower() == 'agendamento para serviços':
            elements.click()

    # Wait for the WebPage load again and then try to find the header on KanBan named Sem agendamento
    wait_schedule_dates_header = WebDriverWait(browse, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR,'div.header.a-center')))
    time.sleep(3.5) # We need a time sleep here because the page doesn't load all the elements at the same time
    schedule_dates_header = browse.find_elements(By.CSS_SELECTOR,'div.header.a-center')
    for elements in schedule_dates_header:
        columns_names = elements.text.split('\n')[0]
        if columns_names.lower() == 'sem agendamento':
            elements.click()

def get_html_page():
    """ This function gets the html from the page using selenium and after transforms this HTML inside the BeautifulSoup"""
    operation_page_html = browse.page_source
    html = BeautifulSoup(operation_page_html, 'html.parser')
    return html

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

def delete_date_range(start_date, end_date, con, db_table_name, date_column):
    """ This function is used to delete the older same range of data before we upload the new one
        We use a SQL Query to delete the data
    """


    try:
        start_date_str = start_date.date().strftime("%Y-%m-%d")  # Format the start date
        end_date_str = end_date.date().strftime("%Y-%m-%d")  # Format the end date
        query_to_delete = f"DELETE FROM `{db_table_name}` WHERE `{date_column}` BETWEEN :start_date AND :end_date"
        # Execute the query with parameters as a dictionary
        params = {"start_date": start_date_str, "end_date": end_date_str}
        con.execute(text(query_to_delete), params)
        # Commit the transaction
        con.commit()
        print(f"Table '{db_table_name}' from {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')} deleted successfully!")
    except Exception as e:
        print("An error occurred:", e)
        traceback.print_exc()  # Print detailed error traceback

def except_error_path(error , message):
    """ This function is used to create a default path if we have some Exception inside the code"""
    e = error
    print("Error uploading data:", e)
    con_massaro_database_mysql = connect_to_database_mysql(host=host_db_mysql, user=user_db_mysql, password=password_db_mysql, database=name_db_mysql)
    df_log = pd.DataFrame({'Data' : datetime.now().date(), 'Automacao': 'Automação Agendados','Duracao (s)': int((datetime.now() - start_time).seconds), 'Status' : f'Falha {message} - {e}'}, index=[0])
    df_log.to_sql(name='LogRefreshs', con= con_massaro_database_mysql, if_exists='append', index=False)
    con_massaro_database_mysql.close()
    print('Database Closed!')
    browse.close()

# %% [markdown]
# ## Starting the Login and Navigation to KanBan Table

# %%
# Starting the Login and Navigation to KanBan Table

try: 
    login()
    get_kanban_table()

except Exception as e:
    except_error_path(e, 'Login and Navigation')

# %% [markdown]
# ## WebScrapping and Data Cleaning

# %%
try: 
    # Getting the table using the HTML
    html_page = get_html_page()

    # Close the browser
    browse.close()

    # Selecting the HTML from table headers
    headers_table = html_page.select('div.content-padding > div.content-table > div.row-header > div > span')

    # Creating the list with the headers
    headers_list = []
    for elements in headers_table:
        headers_list.append(elements.text.strip())
    headers_list.append('Agendamento') # Adding the 'Agendamento' column (last column)

    # Creating the Dataframe with the columns names
    df_scheduled = pd.DataFrame(columns=headers_list)

    # Getting the different tables and dates possibles
    all_date_header_table = html_page.select('div.content-table > div.scroll-content.of-show-bars > a.row.header')
    all_tables = html_page.select('div.content-table > div.scroll-content.of-show-bars > div.content-list')


    # Iterating each line of the table
    for index,table in enumerate(all_tables):
        date_header_table = all_date_header_table[index].text.strip()[:10]
        all_rows = table.select('div.row.nowrap.ng-star-inserted') # CSS Selector from the rows
        total_len_rows = len(all_rows) # Getting the number of rows for each day
        for index in range(total_len_rows):
            row_list = []
            index_row = all_rows[index]
            for elements in index_row:
                element_text = elements.text.strip()
                if element_text != '' and element_text != '-':
                    element_text = element_text.replace('.','')
                    elemenet_text = element_text.replace(',','.')
                    row_list.append(element_text)
                elif element_text == '-':
                    row_list.append('')
                else:
                    row_list.append('')

            # Inserting the date of the table in the dataframe        
            row_list.append(date_header_table)

            # Inserting the lines of the table in the dataframe
            df_temporary = pd.DataFrame([row_list], columns=headers_list)
            df_scheduled = pd.concat([df_scheduled, df_temporary], ignore_index=True)

    # Cleaning the column Agendamento from Dataframe (Convert to date type after)
    df_scheduled['Agendamento'] = df_scheduled['Agendamento'].replace('Sem agenda','')

except Exception as e:
    except_error_path(e, 'Scraping and Cleaning')

# %%

try: 
    
    # Converting all Columns from Object to String
    df_scheduled = df_scheduled.astype({'Processo' : 'string',
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
                                'Prazo' : 'string',
                                'Agendamento': 'string'
                                })

    # Converting the String Columns to Float Columns
    df_scheduled['Orçado'] = pd.to_numeric(df_scheduled['Orçado'], errors='coerce')
    df_scheduled['M.O.'] = pd.to_numeric(df_scheduled['M.O.'], errors='coerce')
    df_scheduled['Agregado'] = pd.to_numeric(df_scheduled['Agregado'], errors='coerce')
    df_scheduled['Peças'] = pd.to_numeric(df_scheduled['Peças'], errors='coerce')
    df_scheduled['S.T'] = pd.to_numeric(df_scheduled['S.T'], errors='coerce')
    df_scheduled['Material'] = pd.to_numeric(df_scheduled['Material'], errors='coerce')

    # Converting the String Columns to Date Columns
    df_scheduled['Entregue'] = pd.to_datetime(df_scheduled['Entregue'], format="%d/%m/%Y" )
    df_scheduled['Serviço'] = pd.to_datetime(df_scheduled['Serviço'], format="%d/%m/%Y" )
    df_scheduled['Pronto'] = pd.to_datetime(df_scheduled['Pronto'], format="%d/%m/%Y" )
    df_scheduled['Cadastro'] = pd.to_datetime(df_scheduled['Cadastro'], format="%d/%m/%Y" )
    df_scheduled['Liberação'] = pd.to_datetime(df_scheduled['Liberação'], format="%d/%m/%Y" )
    df_scheduled['Previsão'] = pd.to_datetime(df_scheduled['Previsão'], format="%d/%m/%Y" )
    df_scheduled['Previsão2'] = pd.to_datetime(df_scheduled['Previsão2'], format="%d/%m/%Y" )
    df_scheduled['Prazo'] = pd.to_datetime(df_scheduled['Prazo'], format="%d/%m/%Y" )
    df_scheduled['Agendamento'] = pd.to_datetime(df_scheduled['Agendamento'], format="%d/%m/%Y" )
    
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

    # Getting the start date and end date from Agendamento Column
    start_date = df_scheduled['Agendamento'].min()
    end_date = df_scheduled['Agendamento'].max()

    # Deleting data from start_date to end_date
    delete_date_range(start_date = start_date, end_date = end_date, con = con_massaro_database_mysql, db_table_name = 'ProcessosAgendados', date_column='Agendamento')
    
    # Uploading the DataFrame to the database
    df_scheduled.to_sql(name='ProcessosAgendados', con= con_massaro_database_mysql, if_exists='append', index=False)
    print(f"Data from {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')} uploaded successfully.")

except Exception as e:
    except_error_path(e,'Uploading and Deleting')

finally:
    """ If everything is ok we close the connection with the database and upload the Log"""

    # Upload the log
    refresh_duration = datetime.now() - start_time # Get the finish time from the code minus the start time of it
    print(f'Refresh duration: {refresh_duration}')
    df_log = pd.DataFrame({'Data' : datetime.now(), 'Automacao': 'Automação Agendados','Duracao (s)': int(refresh_duration.seconds), 'Status' : 'Sucesso'}, index=[0])
    df_log.to_sql(name='LogRefreshs', con=con_massaro_database_mysql, if_exists='append', index=False)
    print(f"Log uploaded successfully.")
    # Close the connection
    con_massaro_database_mysql.close()
    print('Database Closed!')




