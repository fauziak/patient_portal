# -*- coding: utf-8 -*-
"""sql_table_creation.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1JgoQIbYkhWuZjfIuiFKaIjG0c52hnprp
"""
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

MYSQL_HOSTNAME = os.getenv('MYSQL_HOSTNAME')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')

connection_string = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}/{MYSQL_DATABASE}'

db = create_engine(connection_string)
print(db)

query1 = 'create table medications (NDC_Code varchar(255), Name varchar(255), Packager varchar(255), ID int NOT NULL AUTO_INCREMENT PRIMARY KEY);'
query2 = 'create table treatments_procedures (CPT_Code varchar(255), Diagnosis varchar(255), Description varchar(255), ID int NOT NULL AUTO_INCREMENT PRIMARY KEY);'
query3 = 'create table conditions (Name varchar(255), ICD_10 varchar(255), Description varchar(255), Billable boolean, treatment_procedure_ID int, foreign key (treatment_procedure_ID) references treatments_procedures(ID), ID int NOT NULL AUTO_INCREMENT PRIMARY KEY);'
query4 =  'create table social_determinants (Name varchar(255), Description varchar(255), Ionic_code varchar(255), Status varchar(255), ID int NOT NULL AUTO_INCREMENT PRIMARY KEY);'
query5 = 'create table patients (Name varchar(255), ID int NOT NULL AUTO_INCREMENT PRIMARY KEY, Age int, Gender varchar(255), social_determinants_ID int, foreign key (social_determinants_ID) references social_determinants(ID));'

db.execute(query1)

db.execute(query2)

db.execute(query3)

db.execute(query4)

db.execute(query5)

