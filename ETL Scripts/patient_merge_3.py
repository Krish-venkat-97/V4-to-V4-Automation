import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
import numpy as np
import pandas as pd
from src.utils import get_src_myconnection, get_tgt_myconnection,get_patient_records,getPractice

import warnings
warnings.filterwarnings("ignore")

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

ref_patient = """
SELECT old_patient.id AS orginal_id,new_patient.id AS new_id,old_patient.display_name,old_patient.dob
FROM (
SELECT p.id,p.display_name,p.dob FROM patients p
WHERE p.id NOT IN (SELECT target_id FROM mapping_table m WHERE m.table_name = 'patients')
)old_patient
INNER JOIN 
( 
SELECT p.id,CONCAT(p.surname,' ',p.first_name) AS display_name,p.dob FROM patients p
WHERE p.id IN (SELECT target_id FROM mapping_table m WHERE m.table_name = 'patients')
)new_patient
ON UPPER(LTRIM(RTRIM(old_patient.display_name))) = UPPER(LTRIM(RTRIM(new_patient.display_name)))
AND old_patient.dob = new_patient.dob
"""
ref_patient_df = pd.read_sql(ref_patient,myconnection)

#Delete patients 
for index,row in ref_patient_df.iterrows():
    delete_patients = """
    UPDATE patients p
    SET p.deleted_at = CURRENT_TIMESTAMP(),p.updated_at=CURRENT_TIMESTAMP()
    WHERE p.id = {};
    """.format(row['new_id'])
    target_cursor.execute(delete_patients)

myconnection.commit()
print('patient update-delete finished')
myconnection.close()

#with open(r'D:\Medserv Migration\Jim_Merge.txt','a') as f:
#    f.writelines(statement)•