# %% [markdown]
#  ## Importing the libraries

# %%
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
# %%
# Browser   

service = Service()
options = webdriver.ChromeOptions()
options.add_experimental_option("detach", True)
options.add_argument("--mute-audio")
browse = webdriver.Chrome(service=service, options=options)


# %% [markdown]
#  ## Credentials

# %%
# %%
# Time Start

start_time = datetime.now()

# All Credentials to login in

login_link = 'xxxxxxx'
headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'}
username = 'xxxxxxx'
password = 'xxxxxxx'

# Database Credentials SQL Server (Azure DataBase)

host_db = 'xxxxxxxx'
name_db = 'xxxxxxx'
user_db = 'xxxxxx'
password_db = 'xxxxxx'
server_db = 'xxxxxx'

# Database Credentials MySQL

host_db_mysql = 'xxxxxxx'
name_db_mysql = 'xxxxxxx'
user_db_mysql = 'xxxxx'
password_db_mysql = 'xxxxxxxx'
server_db_mysql = ''



# %% [markdown]
#  ## Defining the functions

# %%
# All needed lists and dataframe to use with the functions
consulting_name = []
budgetist_name =[]
customer_name = []

result_cost_list = []
result_budget_list = []
result_earned_list = []

service_budget_list = []
service_cost_list = []

insume_budget_list = []
insume_cost_list = []

outsource_budget_list = []
outsource_cost_list = []

parts_budget_list = []
parts_cost_list = []

# Global Variable
df_sectors_final = pd.DataFrame()

# %%
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

def get_process_all_table_values(process_column, service_column , html, index):
    """ This function gets the sectors values from the html and save it in a list """

    global df_sectors_final

    try:
        # Get all tables from the HTML Scrapped
        all_tables = html.select('div.content-tables.ng-star-inserted > div.flex-table > div.content-table > table')

        # Get the Detailed Process's Sectors Values table (each area)
        sector_table = all_tables[1]
        df_sector_table = pd.read_html(StringIO(sector_table.prettify()),decimal=',',thousands='.')[0]
        df_sector_table.columns = df_sector_table.columns.str.replace(r'\b\d+\b|[,.:]', '', regex=True).str.strip()
        df_sector_table['Processo'] = process_column[index]
        df_sector_table['Serviço'] = service_column[index]

        if df_sector_table is not None:
            df_sectors_final = pd.concat([df_sectors_final, df_sector_table], ignore_index=True)
        
        # Get the Process's Budgets, Costs and Earns Values and append to the properly list
        table_cost_value = all_tables[0]
        df_result_value = pd.read_html(StringIO(table_cost_value.prettify()),decimal=',',thousands='.')[0]
        df_result_value.columns = df_result_value.columns.str.replace(r'\b\d+\b|[,.:]', '', regex=True).str.strip()
        unique_budget_value = df_result_value['Orçado/Faturado'][0]
        unique_earned_value = df_result_value['Orçado/Faturado'][1]
        unique_cost_value = df_result_value['Custo'][0]

        result_budget_list.append(unique_budget_value)
        result_earned_list.append(unique_earned_value)
        result_cost_list.append(unique_cost_value)
        
        # Get the Overall Process's Sectors Values table (final numbers) and append to the properly list
        service_table = all_tables[1]
        service_budget_value = service_table.select(' th > div.txt-bold')[1].text.strip()
        service_cost_value = service_table.select(' th > div.txt-bold')[5].text.strip()

        service_budget_list.append(service_budget_value)
        service_cost_list.append(service_cost_value)

        # Get the Process's Insumes Values table and append to the properly list
        insumes_table = all_tables[2]
        insumes_budget_value = insumes_table.select(' th > div.txt-bold')[0].text.strip()
        insumes_cost_value = insumes_table.select(' th > div.txt-bold')[1].text.strip()

        insume_budget_list.append(insumes_budget_value)
        insume_cost_list.append(insumes_cost_value)

        # Get the Process's Outsourcing Values table and append to the properly list
        outsource_table = all_tables[3]
        outsource_budget_value = outsource_table.select(' th > div.txt-bold')[0].text.strip()
        outsource_cost_value = outsource_table.select(' th > div.txt-bold')[1].text.strip()

        outsource_budget_list.append(outsource_budget_value)
        outsource_cost_list.append(outsource_cost_value)

        # Get the Process's Parts Values table and append to the properly list
        parts_table = all_tables[4]
        parts_budget_value = parts_table.select(' th > div.txt-bold')[0].text.strip()
        parts_cost_value = parts_table.select(' th > div.txt-bold')[1].text.strip()

        parts_budget_list.append(parts_budget_value)
        parts_cost_list.append(parts_cost_value)

    except:

        """Sometimes we can have process missing values and it will throw an error, in that case, we just append an empty string to the list"""
        result_budget_list.append('')
        result_earned_list.append('')
        result_cost_list.append('')
        service_budget_list.append('')
        service_cost_list.append('')
        insume_budget_list.append('')
        insume_cost_list.append('')
        outsource_budget_list.append('')
        outsource_cost_list.append('')
        parts_budget_list.append('')
        parts_cost_list.append('')

def get_employees_name(process_column,html,index):
    """ This function gets the employee name from the html and save it in a list """

    # Get the HTML Elements where are the names
    html_titles_names = html.select('div.col-info.pl-5.col-12 > div > div > span.key-name')

    # Boolean way to make sure the name from each var was found
    found_budgetist = False
    found_customer = False
    found_consultist = False

    # Iterate through the list to get each name correctly
    for titles_name in html_titles_names:

        # Customer Name
        if titles_name.text.strip() == 'Cliente':
            name = titles_name.next_sibling
            if name is None:
                customer_name.append('')
            else:
                customer_name.append(name.text.strip())

            found_customer = True
    

        # Consultor Name
        if titles_name.text.strip() == 'Consultor':
            name = titles_name.next_sibling
            if name is None:
                consulting_name.append('')
            else:
                consulting_name.append(name.text.strip())

            found_consultist = True
        
        #Budgetist Name
        if titles_name.text.strip() == 'Orçamentista':
            name = titles_name.next_sibling
            if name is None:
                budgetist_name.append('')
            else:
                budgetist_name.append(name.text.strip())

            found_budgetist = True

    # If the boolean var still False, it means that the name was not found
    if not found_budgetist:
        budgetist_name.append('')
    if not found_consultist:
        consulting_name.append('')
    if not found_customer:
        customer_name.append('')


    print(f'The line {index + 1} from the process {process_column[index]} was captured successfully!')

    time.sleep(1)

    return consulting_name,budgetist_name, customer_name

def iterate_all_processes_page(main_table):
    """ The function get the list of process from the main table to iterate each one getting the detailed numbers"""
    
    # Looping for each Process's Car 
    process_list_to_iterate = list(main_table['Processo'].replace('-','',regex=True))

    for index,process in enumerate(process_list_to_iterate):   
        iterating_link = f'https://reparo.sistemasigma.com/localizar-processo?processCode={process}&companyCode=2&processSection=processClosing'
        browse.get(iterating_link)
        
        wait_sector_table = WebDriverWait(browse, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.content-tables.ng-star-inserted > div.flex-table > div.content-table > table')))
        
        page_values_process_html = get_html_page()
        """
        temporary_sectors_table = get_process_all_table_values(process_column = main_table['Processo'], delivery_column= main_table['Entregue'] ,html = temporary_html, index = index)

        if temporary_sectors_table is not None:
            sector_table = pd.concat([sector_table, temporary_sectors_table], ignore_index=True)
            """
        get_process_all_table_values(process_column = main_table['Processo'], service_column= main_table['Serviço'],html = page_values_process_html, index = index)
        get_employees_name(process_column = main_table['Processo'], html = page_values_process_html, index = index)

        time.sleep(0.5)

# %% [markdown]
#  ## Starting the Login and Navigation to KanBan Table

# %%
# %%
try: 
    login()
    get_kanban_table()
    filter_kanban_table()

except Exception as e:
    except_error_path(e, 'Login and Navigation')
    


# %% [markdown]
#  ## WebScrapping and Data Cleanning

# %%
# %%
try: 

    # Getting the table using the HTML
    html_page = get_html_page()
    
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
        process_row_list = []
        # Selecting the HTML from table rows
        process_row_table = html_page.select('div.content-padding > div.content-table > div.scroll-content > div.content-list > div.row.nowrap.ng-star-inserted')[index_car]
        # Cleaning the data from each row
        for elements in process_row_table:
            text = elements.text.strip()
            if text != '' and text != '-':
                text = text.replace('.','')
                text = text.replace(',','.')
                process_row_list.append(text)
            elif text == '-':
                process_row_list.append('')
            else:
                process_row_list.append('')

        # Inserting the data from each row in the dataframe        
        df_service.loc[index_car] = process_row_list # Inserting the data in the dataframe

    # Cleaning the dataframe
    columns_to_drop = ['M.O.','Agregado','Peças','S.T','Material','Horas','Peças pintadas','Franquia']
    df_service = df_service.drop(columns=columns_to_drop)

except Exception as e:
    except_error_path(e, 'Scraping and Cleanning')


# %%
try: 
    iterate_all_processes_page(main_table = df_service)

except Exception as e:
    except_error_path(e,'Detailed Scraping - Iterate all processes')
finally:           
    browse.close()
    

# %% [markdown]
#  ## Changing the type of data

# %%
# %%
try: 
    # Inserting the detailed data scrapped in the main dataframe
    df_service['Consultor'] = consulting_name
    df_service['Orcamentista'] = budgetist_name
    df_service['Cliente'] = customer_name
    df_service['Custo'] = result_cost_list
    df_service['Orçado'] = result_budget_list
    df_service['Faturado'] = result_earned_list
    df_service['Orçado MO'] = service_budget_list
    df_service['Custo MO'] = service_cost_list
    df_service['Orçado materiais'] = insume_budget_list
    df_service['Custo materiais'] = insume_cost_list
    df_service['Orçado ST'] = outsource_budget_list
    df_service['Custo ST'] = outsource_cost_list
    df_service['Orçado peças'] = parts_budget_list
    df_service['Custo peças'] = parts_cost_list

    # Replacing and cleaning the dataframe

    df_service = df_service.replace('\.', '', regex=True)
    df_service = df_service.replace(',', '.', regex=True)
    df_service = df_service.replace('', np.nan, regex=False)

    # Converting all Columns from Object to String
    df_service = df_service.astype({'Processo' : 'string',
                                'Seguradora': 'string',
                                'Carro' : 'string',
                                'Placa': 'string',
                                'Prisma': 'string',
                                'Prioridade': 'string',
                                'Na oficina' : 'string',
                                'Serviço' : 'string',
                                'Pronto' : 'string',
                                'Entregue' : 'string',
                                'Cadastro': 'string',
                                'Liberação' : 'string',
                                'Previsão' : 'string',
                                'Previsão2' : 'string',
                                'Prazo' : 'string',
                                'Orçado MO': 'float',
                                'Custo MO': 'float',
                                'Orçado peças': 'float',
                                'Custo peças' : 'float',
                                'Orçado materiais' : 'float',
                                'Custo materiais' : 'float',
                                'Orçado ST' : 'float',
                                'Custo ST': 'float',
                                'Orçado' : 'float',
                                'Custo' : 'float',
                                'Faturado': 'float',
                                'Consultor': 'string',
                                'Orcamentista': 'string',
                                'Cliente' : 'string'

                                })

    # Converting the String Columns to Date Columns
    df_service['Entregue'] = pd.to_datetime(df_service['Entregue'], format="%d/%m/%Y" )
    df_service['Serviço'] = pd.to_datetime(df_service['Serviço'], format="%d/%m/%Y" )
    df_service['Pronto'] = pd.to_datetime(df_service['Pronto'], format="%d/%m/%Y" )
    df_service['Cadastro'] = pd.to_datetime(df_service['Cadastro'], format="%d/%m/%Y" )
    df_service['Liberação'] = pd.to_datetime(df_service['Liberação'], format="%d/%m/%Y" )
    df_service['Previsão'] = pd.to_datetime(df_service['Previsão'], format="%d/%m/%Y" )
    df_service['Previsão2'] = pd.to_datetime(df_service['Previsão2'], format="%d/%m/%Y" )
    # df_service['Prazo'] = pd.to_datetime(df_service['Prazo'], format="%d/%m/%Y" )
    
except Exception as e:
    except_error_path(e,'Formatting')


# %% [markdown]
# ### Detailed Process's Sectors Table

# %%
# Replacing from dataframe Sectors
try: 
       df_sectors_final = df_sectors_final.replace('-','')
       df_sectors_final = df_sectors_final.replace('','NaN')

       # Converting data types
       df_sectors_final = df_sectors_final.astype({'Setor' : 'string', 'Tempo orçado' : 'string', 'Valor orçado' : 'float', 'Tempo distribuído' : 'string',
              'Valor distribuído' : 'float', 'Tempo trabalhado' : 'string', 'Valor trabalhado': 'float',
              'Resultado tempo' : 'string', 'Resultado valor' : 'float', 'Lucro  %' : 'string', 'Processo' : 'string'})

       # Converting the String Columns to Date Columns       
       df_sectors_final['Serviço'] = pd.to_datetime(df_sectors_final['Serviço'], format="%d/%m/%Y" )

except Exception as e:
    except_error_path(e,'Formatting Sector Table')

# %% [markdown]
#  ## Uploading and Deleting From DataBase

# %%
# %%

# Inserting the dataframe into database
try:

    # Connect to database and validating the connection
    con_massaro_database_mysql = connect_to_database_mysql(host=host_db_mysql, user=user_db_mysql, password=password_db_mysql, database=name_db_mysql)
    if con_massaro_database_mysql.closed:
        con_massaro_database_mysql = connect_to_database_mysql(host=host_db_mysql, user=user_db_mysql, password=password_db_mysql, database=name_db_mysql)
    
    # Uploading the DataFrame and replacing the table in the database
    df_service.to_sql(name='ProcessosEmServico', con=con_massaro_database_mysql, if_exists='replace', index=False)
    df_sectors_final.to_sql(name='ProcessosEmServicoSetores', con= con_massaro_database_mysql, if_exists='replace', index=False)
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







