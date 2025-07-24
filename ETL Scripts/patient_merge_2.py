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

#
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
    
#Update invoices
for index,row in ref_patient_df.iterrows():
    update_invoices = """
    UPDATE invoices 
    SET patient_id = {},updated_at=CURRENT_TIMESTAMP()
    WHERE patient_id = {};
    """.format(row['orginal_id'],row['new_id'])
    target_cursor.execute(update_invoices)

myconnection.commit()
print('invoices update finished')

#Update letters
for index,row in ref_patient_df.iterrows():
    update_letters = """
    UPDATE letters 
    SET patient_id = {},updated_at=CURRENT_TIMESTAMP()
    WHERE patient_id = {};
    """.format(row['orginal_id'],row['new_id'])
    target_cursor.execute(update_letters)

myconnection.commit()
print('letters update finished')

#Update medical_histories
for index,row in ref_patient_df.iterrows():
    update_medical_histories = """
    UPDATE medical_histories 
    SET patient_id = {},updated_at=CURRENT_TIMESTAMP()
    WHERE patient_id = {};
    """.format(row['orginal_id'],row['new_id'])
    target_cursor.execute(update_medical_histories)

myconnection.commit()
print('medical_histories update finished')

#Update patient_contact_details
for index,row in ref_patient_df.iterrows():
    update_pat_con_details = """
    UPDATE patient_contact_details 
    SET patient_id = {},updated_at=CURRENT_TIMESTAMP()
    WHERE patient_id = {};
    """.format(row['orginal_id'],row['new_id'])
    target_cursor.execute(update_pat_con_details)

myconnection.commit()    
print('patient_contact_details update finished')

#Update personal_histories
for index,row in ref_patient_df.iterrows():
    update_per_hist = """
    UPDATE personal_histories
    SET patient_id = {},updated_at=CURRENT_TIMESTAMP()
    WHERE patient_id = {};
    """.format(row['orginal_id'],row['new_id'])
    target_cursor.execute(update_per_hist)
    
myconnection.commit()
print('personal_histories update finished')
    
#Update prescriptions
for index,row in ref_patient_df.iterrows():
    update_prescription = """
    UPDATE prescriptions
    SET patient_id = {},updated_at=CURRENT_TIMESTAMP()
    WHERE patient_id = {};
    """.format(row['orginal_id'],row['new_id'])
    target_cursor.execute(update_prescription)

myconnection.commit()
print('prescriptions update finished')

#Update receipts
for index,row in ref_patient_df.iterrows():
    update_receipts = """
    UPDATE receipts 
    SET patient_id = {},updated_at=CURRENT_TIMESTAMP()
    WHERE patient_id = {};
    """.format(row['orginal_id'],row['new_id'])
    target_cursor.execute(update_receipts)

myconnection.commit()
print('receipts update finished')

#Update scan_documents
for index,row in ref_patient_df.iterrows():
    update_scan = """
    UPDATE scan_documents 
    SET patient_id = {},updated_at=CURRENT_TIMESTAMP()
    WHERE patient_id = {};
    """.format(row['orginal_id'],row['new_id'])
    target_cursor.execute(update_scan)

myconnection.commit()
print('scan_documents update finished')
    
#Update waiting_lists
for index,row in ref_patient_df.iterrows():
    update_waiting_list = """
    UPDATE waiting_lists 
    SET patient_id = {},updated_at=CURRENT_TIMESTAMP()
    WHERE patient_id = {};
    """.format(row['orginal_id'],row['new_id'])
    target_cursor.execute(update_waiting_list)

myconnection.commit()
print('waiting_lists update finished')

#Update notes
for index,row in ref_patient_df.iterrows():
    updat_notes = """
    UPDATE notes 
    SET patient_id = {},updated_at=CURRENT_TIMESTAMP()
    WHERE patient_id = {};
    """.format(row['orginal_id'],row['new_id'])
    target_cursor.execute(updat_notes)

myconnection.commit()
print('notes update finished')

myconnection.close()

    
#with open(r'D:\Medserv Migration\Jim_Merge.txt','a') as f:
    #f.writelines(statement)

    
