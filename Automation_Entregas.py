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
from selenium.webdriver.common.action_chains import ActionChains
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
options.add_argument("--mute-audio")
browse = webdriver.Chrome(service=service, options=options)

# %%
# Writting the credentials

login_link = 'xxxxxxx'
headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'}
username = 'xxxxx'
password = 'xxxxx'

# First day of next month, we rerun all days from last month

if datetime.now().day == 1 and datetime.now().hour <= 10:

    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date.replace(day=1)

else:

    start_date =  datetime.now()-timedelta(3) # %Y-%m-%d
    end_date =  datetime.now() # %Y-%m-%d

tipo_de_data = 'Entregue'
start_time = datetime.now()

# Database Credentials SQL Server (Azure DataBase)

host_db = 'xxxxxxx'
name_db = 'xxxxxxx'
user_db = 'xxxxxxx'
password_db = 'xxxxxxx'
server_db = 'xxxxx'

# Database Credentials MySQL

host_db_mysql = 'xxxxr'
name_db_mysql = 'xxxxx'
user_db_mysql = 'xxxxxxx'
password_db_mysql = 'xxxxxxx'

# Changing the type from "start_date" and "end_date" if needed

if type(start_date) == str:
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
if type(end_date) == str:
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    

# %% [markdown]
# ## Functions

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
# Functions

def login():
    """ This function is used to open the WebPage and login to the system"""
    browse.get(login_link) # Open the Web Software
    username_input = browse.find_element(By.CSS_SELECTOR, 'div.form-login > form > div > mat-form-field > div > div > div > input.mat-input-element')
    username_input.send_keys(username) # Write the Username
    time.sleep(3)
    password_input = WebDriverWait(browse, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.mat-form-field-infix > input#inputPassword.mat-input-element')))
    password_input.send_keys(password) # Write the Password
    time.sleep(2)

    # It will look for the login button
    login_attempt = browse.find_element(By.CSS_SELECTOR, 'div.form-login > form > div > button')
    login_attempt.click()

def gestor_page():
    operational_button = WebDriverWait(browse, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.sigma-icon.operational-result')))
    operational_button.click()

    # Changing the Date Type for "Entregue"
    input_date_type = WebDriverWait(browse, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'mat-select > div.mat-select-trigger.ng-tns-c75-5')))
    input_date_type.click()
    wait_list_date_type = WebDriverWait(browse, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'mat-option > span.mat-option-text'))) # Make sure that everything is fine
    list_date_type = browse.find_elements(By.CSS_SELECTOR, 'mat-option > span.mat-option-text')
    for type in list_date_type:
        if type.text.strip().lower() == 'entregue':
            type.click()

    header_input_load = WebDriverWait(browse, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.mat-form-field-infix > span.mat-form-field-label-wrapper > label > mat-label')))

    # 2 Possible inputs for dates
    date_input_headers = browse.find_elements(By.CSS_SELECTOR, 'div.mat-form-field-infix > input.mat-input-element.mat-datepicker-input')


    # Input Date "Periodo De"   
    date_input_headers[0].send_keys(Keys.CONTROL + "a")
    time.sleep(0.5)
    date_input_headers[0].send_keys(Keys.DELETE)
    time.sleep(0.5)
    date_input_headers[0].send_keys(start_date.strftime('%d/%m/%Y'))
    time.sleep(0.5)

    # Input Date "Até"
    date_input_headers[1].send_keys(Keys.CONTROL + "a")
    time.sleep(0.5)
    date_input_headers[1].send_keys(Keys.DELETE)
    time.sleep(0.5)
    date_input_headers[1].send_keys(end_date.strftime('%d/%m/%Y'))
    time.sleep(0.5)

    # Looking for the search button    
    buttons = browse.find_elements(By.CSS_SELECTOR, 'div.filters > div > button.icon')
    for element in buttons:
        if element.text.strip().lower() == 'buscar':
            element.click()
            
    summary_button = WebDriverWait(browse, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div > mat-checkbox > label.mat-checkbox-layout > span.mat-checkbox-inner-container')))
    summary_button.click()

    time.sleep(0.5)
    filter_button = browse.find_element(By.CSS_SELECTOR, 'div.filters-box-actions > div > div.icon > div.sigma-icon.funnel ')
    filter_button.click()

    table_check = WebDriverWait(browse, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.content-table > table')))
    time.sleep(1)

def get_html_page():
    """ This function gets the html from the page using selenium and after transforms this HTML inside the BeautifulSoup"""
    operation_page_html = browse.page_source
    html = BeautifulSoup(operation_page_html, 'html.parser')
    return html

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
        if titles_name.text.strip().lower() == 'cliente':
            name = titles_name.next_sibling
            if name is None:
                customer_name.append('')
            else:
                customer_name.append(name.text.strip())

            found_customer = True
    

        # Consultor Name
        if titles_name.text.strip().lower() == 'consultor':
            name = titles_name.next_sibling
            if name is None:
                consulting_name.append('')
            else:
                consulting_name.append(name.text.strip())

            found_consultist = True
        
        #Budgetist Name
        if titles_name.text.strip().lower() == 'orçamentista':
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

def get_process_all_table_values(process_column, delivery_column, html, index):
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
        df_sector_table['Entregue'] = delivery_column[index]

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
    df_log = pd.DataFrame({'Data' : datetime.now().date(), 'Automacao': 'Automação Entregues','Duracao (s)': int((datetime.now() - start_time).seconds), 'Status' : f'Falha {message} - {e}'}, index=[0])
    df_log.to_sql(name='LogRefreshs', con= con_massaro_database_mysql, if_exists='append', index=False)
    con_massaro_database_mysql.close()
    print('Database Closed!')
    browse.close()

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
        get_process_all_table_values(process_column = main_table['Processo'], delivery_column= main_table['Entregue'] ,html = page_values_process_html, index = index)
        get_employees_name(process_column = main_table['Processo'], html = page_values_process_html, index = index)

        time.sleep(0.5)

# %% [markdown]
# ## Starting the Login and Navigation to the Gestor Page

# %%
# Starting the Login and Navigation to Gestor Page

try: 
    login()
    gestor_page()

except Exception as e:
    except_error_path(e,'Login and Navigation')

# %% [markdown]
# ## WebScrapping and Data Cleaning

# %%
try: 
    # Get the html page
    html_scrapped = get_html_page()

    # Get the mainly used table and convert it to a dataframe
    html_table = html_scrapped.select('div.content-table > table')[0]
    html_table = html_table.prettify()
    df_result = pd.read_html(StringIO(html_table),decimal=',',thousands='.')[0].dropna(axis=1, how='all')

    # Use regex to extract only alphabetic characters from column names
    df_result.columns = df_result.columns.str.replace(r'\b\d+\b|[,.:-]', '', regex=True).str.strip()

    # Dropping column 'Resultado' (problematic)
    df_result = df_result.drop(columns=['Resultado'])
    df_result = df_result.replace('-',np.nan)

    # Replacing '.' with ',' from column 'Fornecimento' (problematic)
    df_result['Fornecimento'] = df_result['Fornecimento'].replace('\.',',',regex=True)

except Exception as e:
    except_error_path(e,'General Scraping and Cleaning')

# %%
try: 
    iterate_all_processes_page(main_table = df_result)

except Exception as e:
    except_error_path(e,'Detailed Scraping - Iterate all processes')
    
finally:           
    browse.close()
    
    

# %% [markdown]
# ## Formatting the Data

# %% [markdown]
# ### General Process's Table

# %%
try: 

    # Inserting the detailed data scrapped in the main dataframe
    df_result['Consultor'] = consulting_name
    df_result['Orcamentista'] = budgetist_name
    df_result['Cliente'] = customer_name
    df_result['Custo'] = result_cost_list
    df_result['Orçado'] = result_budget_list
    df_result['Faturado'] = result_earned_list
    df_result['Orçado MO'] = service_budget_list
    df_result['Custo MO'] = service_cost_list
    df_result['Orçado materiais'] = insume_budget_list
    df_result['Custo materiais'] = insume_cost_list
    df_result['Orçado ST'] = outsource_budget_list
    df_result['Custo ST'] = outsource_cost_list
    df_result['Orçado peças'] = parts_budget_list
    df_result['Custo peças'] = parts_cost_list

    # Replacing and cleaning the dataframe

    df_result = df_result.replace('\.', '', regex=True)
    df_result = df_result.replace(',', '.', regex=True)
    df_result = df_result.replace('', np.nan, regex=False)

    # Converting data types
    df_result = df_result.astype({'Orçado' : 'float',
                                '% MO': 'float',
                                'Custo' : 'float',
                                'Faturado': 'float',
                                'Peças pintadas': 'float',
                                'Peças recuperadas': 'float',
                                'Peças trocadas' : 'float',
                                'Orçado MO': 'float',
                                'Custo MO': 'float',
                                'Orçado peças': 'float',
                                'Custo peças' : 'float',
                                'Orçado materiais' : 'float',
                                'Custo materiais' : 'float',
                                'Orçado ST' : 'float',
                                'Custo ST': 'float',
                                'Fornecimento' : 'float',
                                'Cadastro': 'string',
                                'Liberação': 'string',
                                'Entregue' : 'string',
                                'Serviço' : 'string',
                                'Pronto' : 'string',
                                'Entregue' : 'string',
                                'Competência': 'string',
                                'Fechamento' : 'string'
                                })

    # Converting the String Columns to Date Columns
    df_result['Cadastro'] = pd.to_datetime(df_result['Cadastro'], format="%d/%m/%Y" )
    df_result['Liberação'] = pd.to_datetime(df_result['Liberação'], format="%d/%m/%Y" )
    df_result['Entregue'] = pd.to_datetime(df_result['Entregue'], format="%d/%m/%Y" )
    df_result['Serviço'] = pd.to_datetime(df_result['Serviço'], format="%d/%m/%Y" )
    df_result['Pronto'] = pd.to_datetime(df_result['Pronto'], format="%d/%m/%Y" )
    df_result['Competência'] = pd.to_datetime(df_result['Competência'], format="%d/%m/%Y" )
    df_result['Fechamento'] = pd.to_datetime(df_result['Fechamento'], format="%d/%m/%Y" )

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
              'Resultado tempo' : 'string', 'Resultado valor' : 'float', 'Lucro  %' : 'string', 'Processo' : 'string',
              'Entregue': 'string'})

       # Converting the String Columns to Date Columns
       df_sectors_final['Entregue'] = pd.to_datetime(df_sectors_final['Entregue'], format ='%d/%m/%Y')

except Exception as e:
       except_error_path(e,'Formatting Sector Table')

# %% [markdown]
# ## Uploading and Deleting From DataBase

# %%
# Inserting the dataframe into database
try:
    # Connect to database and validating the connection
    con_massaro_database_mysql = connect_to_database_mysql(host=host_db_mysql, user=user_db_mysql, password=password_db_mysql, database=name_db_mysql)
    if con_massaro_database_mysql.closed:
        con_massaro_database_mysql = connect_to_database_mysql(host=host_db_mysql, user=user_db_mysql, password=password_db_mysql, database=name_db_mysql)

    # Deleting data from start_date to end_date
    delete_date_range(start_date = start_date, end_date = end_date, con = con_massaro_database_mysql, db_table_name = 'ProcessosEntregues', date_column='Entregue')
    delete_date_range(start_date = start_date, end_date = end_date, con = con_massaro_database_mysql, db_table_name = 'ProcessosEntreguesSetores', date_column='Entregue')
    
    # Uploading the DataFrame to the database
    df_result.to_sql(name='ProcessosEntregues', con= con_massaro_database_mysql, if_exists='append', index=False)
    df_sectors_final.to_sql(name='ProcessosEntreguesSetores', con= con_massaro_database_mysql, if_exists='append', index=False)
    print(f"Data from {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')} uploaded successfully.")

except Exception as e:
    except_error_path(e,'Uploading and Deleting')

finally:
    """ If everything is ok we close the connection with the database and upload the Log"""

    # Upload the log
    refresh_duration = datetime.now() - start_time
    print(f'Refresh duration: {refresh_duration}')
    df_log = pd.DataFrame({'Data' : datetime.now(), 'Automacao': 'Automação Entregas','Duracao (s)': int(refresh_duration.seconds), 'Status' : 'Sucesso'}, index=[0])
    df_log.to_sql(name='LogRefreshs', con=con_massaro_database_mysql, if_exists='append', index=False)
    print(f"Log uploaded successfully.")
    # Close the connection
    con_massaro_database_mysql.close()
    print('Database Closed!')




