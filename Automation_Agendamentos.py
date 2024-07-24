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

login_link = 'xxxxx'
headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'}
username = 'xxxxxx'
password = 'xxxxxx'
start_time = datetime.now()

# Database Credentials SQL Server (Azure DataBase)

host_db = 'xxxxxxx'
name_db = 'xxxxxx'
user_db = 'xxxxxxx'
password_db = 'xxxxxxx'
server_db = 'xxxxxxx'

# Database Credentials MySQL

host_db_mysql = 'xxxxxx'
name_db_mysql = 'xxxxxx'
user_db_mysql = 'xxxxx'
password_db_mysql = 'xxxxxx'
server_db_mysql = ''
    

# %% [markdown]
# ## Defining the Functions

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

    # Filtering the processes without scheduled to improve the code's duration

    stage_input = browse.find_element(By.CSS_SELECTOR, 'global-mat-select > mat-form-field > div > div > div.mat-form-field-infix > mat-select#mat-select-16')
    stage_input.click()
    stage_options_list = browse.find_elements(By.CSS_SELECTOR, 'div#mat-select-16-panel > mat-option > span.mat-option-text')
    for elements in stage_options_list:
        if elements.text.strip().lower() == 'sem agendamento':
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

def get_process_all_table_values(process_column, scheduled_column,  html, index):
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
        df_sector_table['Agendamento'] = scheduled_column[index]

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
        get_process_all_table_values(process_column = main_table['Processo'],scheduled_column= main_table['Agendamento'] ,html = page_values_process_html, index = index)
        get_employees_name(process_column = main_table['Processo'], html = page_values_process_html, index = index)

        time.sleep(0.5)

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
        day_total_rows = len(all_rows) # Getting the number of rows for each day
        for index in range(day_total_rows):
            process_row_list = []
            index_row = all_rows[index]
            for elements in index_row:
                element_text = elements.text.strip()
                if element_text != '' and element_text != '-':
                    element_text = element_text.replace('.','')
                    elemenet_text = element_text.replace(',','.')
                    process_row_list.append(element_text)
                elif element_text == '-':
                    process_row_list.append('')
                else:
                    process_row_list.append('')

            # Inserting the date of the table in the dataframe        
            process_row_list.append(date_header_table)

            # Inserting the lines of the table in the dataframe
            df_temporary = pd.DataFrame([process_row_list], columns=headers_list)
            df_scheduled = pd.concat([df_scheduled, df_temporary], ignore_index=True)

    # Cleaning the column Agendamento from Dataframe (Convert to date type after)
    df_scheduled['Agendamento'] = df_scheduled['Agendamento'].replace('Sem agenda','')

    # Cleaning the dataframe
    columns_to_drop = ['M.O.','Agregado','Peças','S.T','Material','Horas','Peças pintadas','Franquia']
    df_scheduled = df_scheduled.drop(columns=columns_to_drop)

except Exception as e:
    except_error_path(e, 'Scraping and Cleaning')

# %%
try: 
    iterate_all_processes_page(main_table = df_scheduled)

except Exception as e:
    except_error_path(e,'Detailed Scraping - Iterate all processes')
    
finally:           
    browse.close()


# %%
# %%
try: 
    # Inserting the detailed data scrapped in the main dataframe
    df_scheduled['Consultor'] = consulting_name
    df_scheduled['Orcamentista'] = budgetist_name
    df_scheduled['Cliente'] = customer_name
    df_scheduled['Custo'] = result_cost_list
    df_scheduled['Orçado'] = result_budget_list
    df_scheduled['Faturado'] = result_earned_list
    df_scheduled['Orçado MO'] = service_budget_list
    df_scheduled['Custo MO'] = service_cost_list
    df_scheduled['Orçado materiais'] = insume_budget_list
    df_scheduled['Custo materiais'] = insume_cost_list
    df_scheduled['Orçado ST'] = outsource_budget_list
    df_scheduled['Custo ST'] = outsource_cost_list
    df_scheduled['Orçado peças'] = parts_budget_list
    df_scheduled['Custo peças'] = parts_cost_list

    # Replacing and cleaning the dataframe

    df_scheduled = df_scheduled.replace('\.', '', regex=True)
    df_scheduled = df_scheduled.replace(',', '.', regex=True)
    df_scheduled = df_scheduled.replace('', np.nan, regex=False)

    # Converting all Columns from Object to String
    df_scheduled = df_scheduled.astype({'Processo' : 'string',
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
    df_scheduled['Entregue'] = pd.to_datetime(df_scheduled['Entregue'], format="%d/%m/%Y" )
    df_scheduled['Serviço'] = pd.to_datetime(df_scheduled['Serviço'], format="%d/%m/%Y" )
    df_scheduled['Pronto'] = pd.to_datetime(df_scheduled['Pronto'], format="%d/%m/%Y" )
    df_scheduled['Cadastro'] = pd.to_datetime(df_scheduled['Cadastro'], format="%d/%m/%Y" )
    df_scheduled['Liberação'] = pd.to_datetime(df_scheduled['Liberação'], format="%d/%m/%Y" )
    df_scheduled['Previsão'] = pd.to_datetime(df_scheduled['Previsão'], format="%d/%m/%Y" )
    df_scheduled['Previsão2'] = pd.to_datetime(df_scheduled['Previsão2'], format="%d/%m/%Y" )
    df_scheduled['Agendamento'] = pd.to_datetime(df_scheduled['Agendamento'], format="%d/%m/%Y" )
    # df_scheduled['Prazo'] = pd.to_datetime(df_scheduled['Prazo'], format="%d/%m/%Y" )
    
except Exception as e:
    except_error_path(e,'Formatting')
    

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
       df_sectors_final['Agendamento'] = pd.to_datetime(df_sectors_final['Agendamento'], format="%d/%m/%Y" )

except Exception as e:
    # except_error_path(e,'Formatting Sector Table')
    print(e)

# %% [markdown]
# ## Uploading and Deleting From DataBase

# %%
# Inserting the dataframe into database
try:
    # Connect to database and validating the connection
    con_massaro_database_mysql = connect_to_database_mysql(host=host_db_mysql, user=user_db_mysql, password=password_db_mysql, database=name_db_mysql)
    if con_massaro_database_mysql.closed:
        con_massaro_database_mysql = connect_to_database_mysql(host=host_db_mysql, user=user_db_mysql, password=password_db_mysql, database=name_db_mysql)

    start_date = df_scheduled['Agendamento'].min()
    end_date = df_scheduled['Agendamento'].max()

    # Deleting data from start_date to end_date
    delete_date_range(start_date = start_date, end_date = end_date, con = con_massaro_database_mysql, db_table_name = 'ProcessosAgendados', date_column='Agendamento')
    delete_date_range(start_date = start_date, end_date = end_date, con = con_massaro_database_mysql, db_table_name = 'ProcessosAgendadosSetores', date_column='Agendamento')
    
    # Uploading the DataFrame to the database
    df_scheduled.to_sql(name='ProcessosAgendados', con= con_massaro_database_mysql, if_exists='append', index=False)
    df_sectors_final.to_sql(name='ProcessosAgendadosSetores', con= con_massaro_database_mysql, if_exists='append', index=False)
    print(f"Data from {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')} uploaded successfully.")

except Exception as e:
    except_error_path(e,'Uploading and Deleting')

finally:
    """ If everything is ok we close the connection with the database and upload the Log"""

    # Upload the log
    refresh_duration = datetime.now() - start_time
    print(f'Refresh duration: {refresh_duration}')
    df_log = pd.DataFrame({'Data' : datetime.now(), 'Automacao': 'Automação Agendados','Duracao (s)': int(refresh_duration.seconds), 'Status' : 'Sucesso'}, index=[0])
    df_log.to_sql(name='LogRefreshs', con=con_massaro_database_mysql, if_exists='append', index=False)
    print(f"Log uploaded successfully.")
    # Close the connection
    con_massaro_database_mysql.close()
    print('Database Closed!')



