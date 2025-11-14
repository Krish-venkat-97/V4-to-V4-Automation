import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
import numpy as np
import pandas as pd
from src.utils import get_src_myconnection, get_tgt_myconnection,get_patient_records,getPractice,getSourceFilePath,getTargetFilePath,getMergeFilePath

import warnings
warnings.filterwarnings("ignore")

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()


ref_patient = """
SELECT old_patient.id AS orginal_id,new_patient.id AS new_id,old_patient.display_name,old_patient.dob
FROM (
SELECT p.id,p.display_name,p.dob,p.address1 FROM patients p
WHERE p.id NOT IN (SELECT target_id FROM mapping_table m WHERE m.table_name = 'patients')
)old_patient
INNER JOIN 
( 
SELECT p.id,p.display_name,p.dob,p.address1 FROM patients p
WHERE p.id IN (SELECT target_id FROM mapping_table m WHERE m.table_name = 'patients')
)new_patient
ON UPPER(LTRIM(RTRIM(old_patient.display_name))) = UPPER(LTRIM(RTRIM(new_patient.display_name)))
AND old_patient.dob = new_patient.dob AND 
old_patient.address1 = new_patient.address1
"""
ref_patient_df = pd.read_sql(ref_patient,myconnection)

ref_patient_df1= ref_patient_df[['orginal_id', 'new_id']]

source_file_path = getTargetFilePath()
merge_file_path = getMergeFilePath()

source_file_location = source_file_path
target_file_location = merge_file_path

def getSourcePatientFileLocation(row):
    a = os.path.join(source_file_location,str(row['new_id']))
    return a

ref_patient_df1['source_file_location'] = ref_patient_df1.apply(lambda row:getSourcePatientFileLocation(row),axis=1)

def getTargetPatientFileLocation(row):
    a = os.path.join(target_file_location,str(row['orginal_id']))
    return a

ref_patient_df1['target_file_location'] = ref_patient_df1.apply(lambda row:getTargetPatientFileLocation(row),axis=1)

bar = tqdm(total=len(ref_patient_df1),desc='uploading files')

for index,row in ref_patient_df1.iterrows():
    bar.update(1)
    if os.path.exists(row['source_file_location']):
        os.makedirs(row['target_file_location'],exist_ok=True)
        shutil.copytree(row['source_file_location'], row['target_file_location'], dirs_exist_ok=True)
    else:
        continue
    
bar.close()
myconnection.close()    
print('fnished moving the files')
    