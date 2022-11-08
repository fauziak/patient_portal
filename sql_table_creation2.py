import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from faker import Faker # https://faker.readthedocs.io/en/master/
import uuid
import random

load_dotenv()

MYSQL_HOSTNAME = os.getenv('MYSQL_HOSTNAME')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')

MYSQL2_HOSTNAME = os.getenv("MYSQL2_HOSTNAME")
MYSQL2_USER = os.getenv("MYSQL2_USER")
MYSQL2_PASSWORD = os.getenv("MYSQL2_PASSWORD")
MYSQL2_DATABASE = os.getenv("MYSQL2_DATABASE")

connection_string_gcp = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}:3306/{MYSQL_DATABASE}'
db = create_engine(connection_string_gcp)
print(db)

connection_string_gcp_2 = f'mysql+pymysql://{MYSQL2_USER}:{MYSQL2_PASSWORD}@{MYSQL2_HOSTNAME}:3306/{MYSQL2_DATABASE}'
db_gcp_2 = create_engine(connection_string_gcp_2)
print(db_gcp_2)

##show databases
print(db.table_names())
print(db_gcp_2.table_names())

##importing fake data 
fake = Faker()

fake_patients = [
    {
        #keep just the first 8 characters of the uuid
        'mrn': str(uuid.uuid4())[:8], 
        'first_name':fake.first_name(), 
        'last_name':fake.last_name(),
        'zip_code':fake.zipcode(),
        'dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
        'gender': fake.random_element(elements=('M', 'F')),
        'contact_mobile':fake.phone_number(),
        'contact_home':fake.phone_number()
    } for x in range(75)]

df_fake_patients = pd.DataFrame(fake_patients)
# drop duplicate mrn
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])


#### real icd10 codes
icd10codes = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10codes.columns)
icd10codesShort = icd10codes[['CodeWithSeparator', 'ShortDescription']]
icd10codesShort_1k = icd10codesShort.sample(n=1000)
# drop duplicates
icd10codesShort_1k = icd10codesShort_1k.drop_duplicates(subset=['CodeWithSeparator'], keep='first')


#### real ndc codes
ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1)
# drop duplicates from ndc_codes_1k
ndc_codes_1k = ndc_codes_1k.drop_duplicates(subset=['PRODUCTNDC'], keep='first')

df_fake_patients.to_sql('production_patients', con=db, if_exists='append', index=False)

df = pd.read_sql_query("SELECT * FROM production_patients", db)

insertQuery = "INSERT INTO production_patients (mrn, first_name, last_name, zip_code, dob, gender, contact_mobile, contact_home) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

for index, row in df_fake_patients.iterrows():
    db_gcp_2.execute(insertQuery, (row['mrn'], row['first_name'], row['last_name'], row['zip_code'], row['dob'], row['gender'], row['contact_mobile'], row['contact_home']))
    print("inserted row: ", index)
    
insertQuery = "INSERT INTO production_treatment_procedures (mrn, label, cpt10_code) VALUES (%s, %s, %s)"
#### real cpt codes 
cpt_codes = pd.read_csv('https://gist.githubusercontent.com/lieldulev/439793dc3c5a6613b661c33d71fdd185/raw/25c3abcc5c24e640a0a5da1ee04198a824bf58fa/cpt4.csv')
cpt_codes_1k = cpt_codes.sample(n=1000, random_state=1)
# drop duplicates from cpt_codes_1k
cpt_codes_1k = cpt_codes_1k.drop_duplicates(subset=['com.medigy.persist.reference.type.clincial.CPT.code'], keep='first')

startingRow = 0
for index, row in cpt_codes_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    print("inserted row db: ", index)
    mrn = str(uuid.uuid4())[:8]
    db_gcp_2.execute(insertQuery, (mrn, row['label'], row['com.medigy.persist.reference.type.clincial.CPT.code']))
    print("inserted row db_gcp: ", index)
    ## stop once we have 100 rows
    if startingRow == 100:
        break

### real LONIC codes ###
insertQuery = "INSERT INTO production_social_determinants (mrn, lonic_code, component) VALUES (%s, %s, %s)"
lonic_codes = pd.read_csv('data/Loinc.csv')
lonic_codes_1k = lonic_codes.sample(n=1000, random_state=1)
# drop duplicates from lonic_codes_1k
lonic_codes_1k = lonic_codes_1k.drop_duplicates(subset=['LOINC_NUM'], keep='first')

startingRow = 0
for index, row in lonic_codes_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    print("inserted row db: ", index)
    mrn = str(uuid.uuid4())[:8]
    db_gcp_2.execute(insertQuery, (mrn, row['LOINC_NUM'], row['COMPONENT']))
    print("inserted row db_gcp: ", index)
    ## stop once we have 100 rows
    if startingRow == 100:
        break


# # query dbs to see if data is there
df_gcp = pd.read_sql_query("SELECT * FROM production_patients", db_gcp_2)

########## INSERTING IN FAKE CONDITIONS ##########
insertQuery = "INSERT INTO production_medications (med_ndc, med_human_name, med_is_dangerous) VALUES (%s, %s,%s)"

startingRow = 0
for index, row in ndc_codes_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    # db.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db: ", index)
    db_gcp_2.execute(insertQuery, (row['PRODUCTNDC'], row['PROPRIETARYNAME'], row['NONPROPRIETARYNAME'] ))
    print("inserted row db_gcp: ", index)
    ## stop once we have 100 rows
    if startingRow == 100:
        break

# query dbs to see if data is there
df_gcp = pd.read_sql_query("SELECT * FROM production_medications", db_gcp_2)

insertQuery = "INSERT INTO production_conditions (icd10_code, icd10_description) VALUES (%s, %s)"

startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    # db.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db: ", index)
    db_gcp_2.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_gcp: ", index)
    ## stop once we have 100 rows
    if startingRow == 100:
        break

# query dbs to see if data is there
df_gcp = pd.read_sql_query("SELECT * FROM production_conditions", db_gcp_2)

# query dbs for id
df_medications = pd.read_sql_query("SELECT med_ndc FROM production_medications", db_gcp_2) 
df_patients = pd.read_sql_query("SELECT mrn FROM production_patients", db_gcp_2)

# create stacked df and assign patients random number of meds between 1-5
df_patient_medications = pd.DataFrame(columns=['mrn', 'med_ndc '])

# for each patient in df_patient_medications, take a random number of medications between 1 and 10 from df_medications and palce it in df_patient_medications
for index, row in df_patients.iterrows():
    # get a random number of medications between 1 and 5
    numMedications = random.randint(1, 5)
    # get a random sample of medications from df_medications
    df_patients_medication_sample = df_medications.sample(n=numMedications)
    # add the mrn to the df_medications_sample
    df_patients_medication_sample['mrn'] = row['mrn']
    # append the df_medications_sample to df_patient_medications
    df_patient_medications = df_patient_medications.append(df_patients_medication_sample)

# now lets add a random procedure to each patient
insertQuery = "INSERT INTO production_patient_medications (mrn, med_ndc) VALUES (%s, %s)"

for index, row in df_patient_medications.iterrows():
    db_gcp_2.execute(insertQuery, (row['mrn'], row['med_ndc']))
    print("inserted row: ", index)
    
 # query dbs for id2
df_conditions = pd.read_sql_query("SELECT icd10_code FROM production_conditions", db_gcp_2) 
df_patients = pd.read_sql_query("SELECT mrn FROM production_patients", db_gcp_2)

# create stacked df and assign patients random number of meds between 1-5
df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd10_code'])

# for each patient in df_patient_conditions, take a random number of conditions between 1 and 10 from df_conditions and palce it in df_patient_conditions
for index, row in df_patients.iterrows():
    # get a random number of conditions between 1 and 5
    numConditions = random.randint(1, 5)
    # get a random sample of conditions from df_conditions
    df_patients_conditions_sample = df_conditions.sample(n=numConditions)
    # add the mrn to the df_conditions_sample
    df_patients_conditions_sample['mrn'] = row['mrn']
    # append the df_conditions_sample to df_patient_conditions
    df_patient_conditions = df_patient_conditions.append(df_patients_conditions_sample)

# now lets add a random conditions to each patient
insertQuery = "INSERT INTO production_patient_conditions (mrn, icd10_code) VALUES (%s, %s)"

for index, row in df_patient_conditions.iterrows():
    db_gcp_2.execute(insertQuery, (row['mrn'], row['icd10_code']))
    print("inserted row: ", index)  