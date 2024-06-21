"""
Author:
Guilherme Crivellenti Massaro

Create Date:
21/06/2023

Refresh Date:
-

Python Version:
Python 3.11.9

Notes:
Below we have some tips to make you understand well the code

--------------------------------------------------------------------------------------------------------

Documentacion: Webscraping Web Software

# Introduction

These codes were created to webscrap a web software of cars repairs. It was a personal job to help my parents company with data analysis and business intelligence. They will be useful with you want to study more about selenium and beautifulsoup, also with you want to see examples of data cleaning (ETL) using Python. I've used MySQL to upload the data and connect with Softwares as Power BI and i've created a Log to upload if the code was successfully or not to help me make improves

# Mainly libraries required:

1. Pandas: Main library for data manipulation
2. Selenium: Wonderful choice to navigate and/or scrap websites
3. BeautifulSoup: Good tool to get the html from the page and make it better to work
4. SQLAlchemy: One of the best choices to connect in databases
5. Re: Perfect to data cleaning elements
6. Numpy: Advanced tool to work with numbers in general (Real summarized)

# Requirements:

Make sure of all libraries are correctly installed in your computer or venv to execute the script.

# Step by Step:

1.	Install all libraries:
Make sure to install correctly all libraries located in 'requirements.txt'.

2.	Define the Credentials to log in the Software's Account and Database:
Inside the code, we have some strings as 'XXXXXX'. There, you have to use the correct credentials needed.

3.	Run the Script:
Execute the script, there i've written some 'print' to help understand which line is running and it is being successfully or not.

4.	Data Cleaning (Opcional):
Feel free to improve or change the data cleaning from the script. I've done what it was the best for our need.

5.	Database Storage:
Inside the code, we have the function connect_to_database_mysql that helps you connect in the database and upload the dataframe. Make sure of write all de credentials and the name of dataframe correctly
