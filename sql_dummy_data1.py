import dbm
from matplotlib.pyplot import table
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

MYSQL_HOSTNAME = os.getenv('MYSQL_HOSTNAME')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')

MYSQL2_HOSTNAME = os.getenv("MYSQL2_HOSTNAME")
MYSQL2_USER = os.getenv("MYSQL2_USER")
MYSQL2_PASSWORD = os.getenv("MYSQL2_PASSWORD")
MYSQL2_DATABASE = os.getenv("MYSQL2_DATABASE")

########

connection_string_gcp = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}:3306/{MYSQL_DATABASE}'
db = create_engine(connection_string_gcp)
print(db)


connection_string_gcp_2 = f'mysql+pymysql://{MYSQL2_USER}:{MYSQL2_PASSWORD}@{MYSQL2_HOSTNAME}:3306/{MYSQL2_DATABASE}'
db_gcp_2 = create_engine(connection_string_gcp_2)
print(db_gcp_2)


### show tables from databases
tableNames_gcp = db.table_names()
tableNames_gcp_2 = db_gcp_2.table_names()

# reoder tables: production_patient_conditions, production_patient_medications, production_medications, production_patients, production_conditionstableNames_azure = ['production_patient_conditions', 'production_patient_medications', 'production_medications', 'production_patients', 'production_conditions']
tableNames_gcp = ['production_patient_conditions', 'production_patient_medications', 'production_medications', 'production_patients', 'production_conditions']



#### first step below is just creating a basic version of each of the tables,
#### along with the primary keys and default values 

## 
table_prod_patients = """
create table if not exists production_patients (
    id int auto_increment,
    mrn varchar(255) default null unique,
    first_name varchar(255) default null,
    last_name varchar(255) default null,
    zip_code varchar(255) default null,
    dob varchar(255) default null,
    gender varchar(255) default null,
    contact_mobile varchar(255) default null,
    contact_home varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""


table_prod_medications = """
create table if not exists production_medications (
    id int auto_increment,
    med_ndc varchar(255) default null unique,
    med_human_name varchar(255) default null,
    med_is_dangerous varchar(255) default null,
    PRIMARY KEY (id)
); 
"""

table_prod_conditions = """
create table if not exists production_conditions (
    id int auto_increment,
    icd10_code varchar(255) default null unique,
    icd10_description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""


table_prod_patients_medications = """
create table if not exists production_patient_medications (
    id int auto_increment,
    mrn varchar(255) default null,
    med_ndc varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES production_patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (med_ndc) REFERENCES production_medications(med_ndc) ON DELETE CASCADE
); 
"""


table_prod_patient_conditions = """
create table if not exists production_patient_conditions (
    id int auto_increment,
    mrn varchar(255) default null,
    icd10_code varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES production_patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (icd10_code) REFERENCES production_conditions(icd10_code) ON DELETE CASCADE
); 
"""
table_prod_treatments_procedures = """
create table if not exists production_treatment_procedures (
    id int auto_increment,
    mrn varchar(255) default null,
    label varchar(255) default null,
    cpt10_code varchar(255) default null,
    PRIMARY KEY (id)
);
"""
table_prod_social_determinants = """
create table if not exists production_social_determinants (
    id int auto_increment,
    mrn varchar(255) default null,
    lonic_code varchar (255) default null,
    component varchar (255) default null,
    PRIMARY KEY (id)
);
"""
    
db.execute(table_prod_patients)
db.execute(table_prod_medications)
db.execute(table_prod_conditions)
db.execute(table_prod_treatments_procedures)
db.execute(table_prod_social_determinants)
db.execute(table_prod_patients_medications)
db.execute(table_prod_patient_conditions)

db_gcp_2.execute(table_prod_patients)
db_gcp_2.execute(table_prod_medications)
db_gcp_2.execute(table_prod_conditions)
db_gcp_2.execute(table_prod_treatments_procedures)
db_gcp_2.execute(table_prod_social_determinants)
db_gcp_2.execute(table_prod_patients_medications)
db_gcp_2.execute(table_prod_patient_conditions)


# get tables from db_gcp
gcp_tables = db.table_names()

# get tables from db_gcp
gcp_tables = db.table_names()


