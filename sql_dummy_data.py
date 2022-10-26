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


#### MEDICATION
query1 = 'insert into medications (NDC_Code, Name , Packager) values ("0024-5837-01", "FLOMAX-tamsulosin hydrochloride capsule", "sanofi-aventis U.S. LLC");'
#db.execute(query1) 

query2 = 'insert into medications (NDC_Code, Name , Packager) values ("0002-4462-30", "CIALIS- tadalafil", "Eli Lilly and Company");'
#db.execute(query2) 

query3 = 'insert into medications (NDC_Code, Name , Packager) values ("49708-145-01", "BACTRIM DS- sulfamethoxazole and trimethoprim tablet", "Sun Pharmaceutical Industries Inc");'
#db.execute(query3) 

query4 = 'insert into medications (NDC_Code, Name , Packager) values ("66887-003-01", "XIAFLEX- collagenase clostridium histolyticum kit", "Endo Pharmaceuticals Inc");'
#db.execute(query4) 

query5 = 'insert into medications (NDC_Code, Name , Packager) values ("0469-2601-07", "MYRBETRIQ- mirabegron tablet, film coated, extended releaset", "Astellas Pharma US Inc");'
#db.execute(query5) 

#### treatments_procedures
query6 = 'insert into treatments_procedures (CPT_Code, Diagnosis, Description) values ("N40" , "Benign prostatic hyperplasia" , "adenofibromatous hypertrophy of prostate, benign hypertrophy of the prostate, benign prostatic hypertrophy, BPH, enlarged prostate");'
# db.execute(query6) 

query7= 'insert into treatments_procedures (CPT_Code, Diagnosis, Description) values ("N52" , "Male Erectile Dysfunction", "Male erectile dysfunction, Excludes1: psychogenic impotence (F52.21)");'
# db.execute(query7) 

query8= 'insert into treatments_procedures (CPT_Code, Diagnosis, Description) values ("Q62", "Vesicoureteral Reflux", "Congenital obstructive defects of renal pelvis and congenital malformations of ureter");'
# db.execute(query8) 

query9= 'insert into treatments_procedures (CPT_Code, Diagnosis, Description) values ("54110" , "Peyronie\'s Disease","The provider excises plaque, or abnormal fibrous tissues, in the penis. The provider performs this procedure to correct Peyronie’s disease in which a patient may experience severe penile curves and pain during erection, or a lump");'
# db.execute(query9) 

query10= 'insert into treatments_procedures (CPT_Code, Diagnosis, Description) values ("N32", "Overactive Bladder" , "Other disorders of bladder is a medical classification as listed by WHO under the range - Diseases of the genitourinary system.");'
# db.execute(query10) 


#### conditions
query11= 'insert into conditions (Name, ICD_10, Description, Billable, treatment_procedure_ID) values ("Benign prostatic hyperplasia","N40","enlarged prostate, Specifically for this code :enlarged prostate",false, 1);'
# db.execute(query11)

query12= 'insert into conditions (Name, ICD_10, Description, Billable,treatment_procedure_ID) values ("Male erectile dysfunction", "N52","psychogenic impotence",false,2);'
# db.execute(query12) 

query13= 'insert into conditions (Name, ICD_10, Description, Billable,treatment_procedure_ID) values ("Vesicoureteral-reflux","N13.7","pyelonephritis",false,3);'
# db.execute(query13) 

query14= 'insert into conditions (Name, ICD_10, Description, Billable,treatment_procedure_ID) values ("Peyronie\'s Disease", "N48.6","A condition characterized by hardening of the penis due to the formation of fibrous plaques on the dorsolateral aspect of the penis, usually involving the membrane (tunica albuginea) surrounding the erectile tissue (corpus cavernosum penis)", true, 4);'
#db.execute(query14) 

query15= 'insert into conditions (Name, ICD_10, Description, Billable,treatment_procedure_ID) values ("Overactive Bladder", "N32.81", "frequent urination due to specified bladder condition- code to condition", true,5);'
#db.execute(query15) 

### social_determinants

query16=  'insert into social_determinants (Name, Description ,Ionic_code,Status) values ("Number of visits with usual provider 12 months","# visits usual provider 12Mo","92257-5","ACTIVE");'
#db.execute(query16) 

query17= 'insert into social_determinants (Name, Description ,Ionic_code,Status) values ("Health insurance funding was provided","Heath insurance provided","74186-8","ACTIVE");'
#db.execute(query17) 

query18= 'insert into social_determinants (Name, Description ,Ionic_code,Status) values ("How often is the following kind of support available to you if you need it - someone to take you to the doctor if you needed it [MOS Social Support Survey]","Is healthcare easily accesible","91653-6", "ACTIVE");'
#db.execute(query18) 

query19= 'insert into social_determinants (Name, Description ,Ionic_code,Status) values ("Preferred language","What is the preferred language for the pt","54899-0","ACTIVE");'
#db.execute(query19) 

query20= 'insert into social_determinants (Name, Description ,Ionic_code,Status) values ("Challenges to maintaining treatments or health behaviors","Factors that are limiting access to coninuous treatment","	87535-1", "ACTIVE");'
#db.execute(query20) 

query21 = 'insert into patients (Name, Age, Gender,social_determinants_ID) values ("Anthony Robinson", "42","M",1);'
db.execute(query21) 

query22 = 'insert into patients (Name, Age, Gender,social_determinants_ID) values ("Clayton Duncan","48","M",2);'
db.execute(query22) 

query23 = 'insert into patients (Name, Age, Gender,social_determinants_ID) values ("Petrina Norwood","15","F",3);'
db.execute(query23) 

query24 = 'insert into patients (Name, Age, Gender,social_determinants_ID) values ("Harvey Morris","60","M",4);'
db.execute(query24) 

query25 = 'insert into patients (Name, Age, Gender,social_determinants_ID) values ("Hazel Mims","45","F",5);'
db.execute(query25) 