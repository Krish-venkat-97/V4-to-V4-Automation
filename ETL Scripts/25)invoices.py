import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
import numpy as np
import pandas as pd
from src.utils import get_src_myconnection, get_tgt_myconnection,get_patient_records,getPractice

#setup_logging(os.path.splitext(os.path.basename(__file__))[0])

import warnings
warnings.filterwarnings("ignore")

src_connection = get_src_myconnection()
tgt_connection = get_tgt_myconnection()
tgt_cursor = tgt_connection.cursor()
patient_ids = get_patient_records()
practice_name = getPractice()
table_name = 'invoices'

src_invoices  = f"SELECT * FROM invoices WHERE patient_id IN ({','.join(map(str, patient_ids))})"
src_invoices_df = pd.read_sql(src_invoices, src_connection)

tgt_mapping_patient_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'patients'"
tgt_mapping_patient_table_df = pd.read_sql(tgt_mapping_patient_table, tgt_connection)

tgt_mapping_contacts_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'contacts'"
tgt_mapping_contacts_table_df = pd.read_sql(tgt_mapping_contacts_table, tgt_connection)

tgt_mapping_insurance_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'insurance_companies'"
tgt_mapping_insurance_table_df = pd.read_sql(tgt_mapping_insurance_table, tgt_connection)

tgt_mapping_tax_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'taxes'"
tgt_mapping_tax_table_df = pd.read_sql(tgt_mapping_tax_table, tgt_connection)

#---------------------------patient_id----------------------------------
# Merging source invoices with target patients mapping to get target patient_id
src_invoices_df['patient_id'] = src_invoices_df['patient_id'].astype(int)
tgt_mapping_patient_table_df['source_id'] = tgt_mapping_patient_table_df['source_id'].astype(int)
src_invoices_df = pd.merge(src_invoices_df, tgt_mapping_patient_table_df, left_on='patient_id', right_on='source_id', how='left')
src_invoices_df = src_invoices_df.drop(columns=['patient_id', 'source_id'])
src_invoices_df = src_invoices_df.rename(columns={'target_id': 'patient_id'})

#----------------------------contact_id-----------------------------------
# Merging source invoices with target contacts mapping to get target contact_id
if src_invoices_df['contact_id'].isnull().all():
    src_invoices_df['contact_id'] = None
else:
    src_invoices_df['contact_id'] = src_invoices_df['contact_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    tgt_mapping_contacts_table_df['source_id'] = tgt_mapping_contacts_table_df['source_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    src_invoices_df = pd.merge(src_invoices_df, tgt_mapping_contacts_table_df, left_on='contact_id', right_on='source_id', how='left')
    src_invoices_df = src_invoices_df.drop(columns=['contact_id', 'source_id'])
    src_invoices_df = src_invoices_df.rename(columns={'target_id': 'contact_id'})

#----------------------------insurance_company_id-----------------------------------
# Merging source invoices with target insurance companies mapping to get target insurance_company_id
if src_invoices_df['insurance_company_id'].isnull().all():
    src_invoices_df['insurance_company_id'] = None
else:
    src_invoices_df['insurance_company_id'] = src_invoices_df['insurance_company_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    tgt_mapping_insurance_table_df['source_id'] = tgt_mapping_insurance_table_df['source_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    src_invoices_df = pd.merge(src_invoices_df, tgt_mapping_insurance_table_df, left_on='insurance_company_id', right_on='source_id', how='left')
    src_invoices_df = src_invoices_df.drop(columns=['insurance_company_id', 'source_id'])
    src_invoices_df = src_invoices_df.rename(columns={'target_id': 'insurance_company_id'})

#----------------------------tax_id-----------------------------------
# Merging source invoices with target taxes mapping to get target tax_id
if src_invoices_df['tax_id'].isnull().all():
    src_invoices_df['tax_id'] = None
else:
    src_invoices_df['tax_id'] = src_invoices_df['tax_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    tgt_mapping_tax_table_df['source_id'] = tgt_mapping_tax_table_df['source_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    src_invoices_df = pd.merge(src_invoices_df, tgt_mapping_tax_table_df, left_on='tax_id', right_on='source_id', how='left')
    src_invoices_df = src_invoices_df.drop(columns=['tax_id', 'source_id'])
    src_invoices_df = src_invoices_df.rename(columns={'target_id': 'tax_id'})

#----------------------------wrtting default values--------------------
src_invoices_df['appointment_id'] = None
src_invoices_df['surgery_id'] = None
src_invoices_df['income_category_id'] = src_invoices_df['income_category_id'].fillna(1)

#----------------------------new data insertion--------------------------------
src_invoices_df1 = src_invoices_df
#id genration for new data
tgt_invoices_max = f'SELECT CASE WHEN MAX(id) is NULL THEN 1 ELSE MAX(id) + 1 END as max_id FROM {table_name}'
tgt_invoices_max_df = pd.read_sql(tgt_invoices_max, tgt_connection)
max_id = int(tgt_invoices_max_df['max_id'].iloc[0])
src_invoices_df1.insert(0, 'target_id', range(max_id, max_id + len(src_invoices_df1)))

#id generation for invoie no
src_invoices_df1.drop(columns=['invoice_no'], inplace=True, errors='ignore')
tgt_invoiceno_max = f'SELECT CASE WHEN MAX(invoice_no) is NULL THEN 1 ELSE MAX(invoice_no) + 1 END as max_id FROM {table_name}'
tgt_invoiceno_max_df = pd.read_sql(tgt_invoiceno_max, tgt_connection)
max_id = int(tgt_invoiceno_max_df['max_id'].iloc[0])
src_invoices_df1.insert(0, 'invoice_no', range(max_id, max_id + len(src_invoices_df1)))

# Before inserting new records, check mapping_table for existing source_ids to avoid duplicates
def insert_new_records_and_mapping(df, tgt_connection, practice_name, table_name):
    # Fetch existing source_ids from mapping_table
    existing_source_ids_query = f'''
        SELECT source_id FROM mapping_table
        WHERE source = "{practice_name}" AND table_name = "{table_name}"
    '''
    existing_source_ids_df = pd.read_sql(existing_source_ids_query, tgt_connection)
    existing_source_ids_df = existing_source_ids_df.astype(int)
    #existing_source_ids = set(existing_source_ids_df['source_id'].tolist())
    # Filter out rows where id is in existing_source_ids
    new_records_df = dd.merge(df, existing_source_ids_df, left_on='id', right_on='source_id', how='left', indicator=True)
    new_records_df = new_records_df[new_records_df['_merge'] == 'left_only'].drop(columns=['_merge', 'source_id'])
    mapping_records_df = dd.merge(df, existing_source_ids_df, left_on='id', right_on='source_id', how='left', indicator=True)
    mapping_records_df = mapping_records_df[mapping_records_df['_merge'] == 'left_only'][['id', 'target_id']].rename(columns={'id': 'source_id'})
    mapping_records_df = mapping_records_df.drop_duplicates(subset=['source_id'], keep='first')
    new_records_df = new_records_df.drop(columns=['id'])
    new_records_df = new_records_df.rename(columns={'target_id': 'id'})
    new_records_df = new_records_df.replace({pd.NaT: None}).replace({np.nan: None})
    if new_records_df.empty:
        print("No new records to insert.")
        return

    # Insert new records into the target table
    column_names = [col for col in new_records_df.columns]
    columns = ", ".join(column_names)
    placeholders = ", ".join(["%s"] * len(column_names))
    rows = new_records_df.values.tolist()

    column_names1 = ['source', 'table_name', 'target_id', 'source_id','new_or_exist']
    columns1 = ", ".join(column_names1)
    placeholders1 = ", ".join(["%s"] * len(column_names1))
    mapping_rows0 = mapping_records_df.values.tolist()
    mapping_rows = [(practice_name, table_name, row[1], row[0],'new') for row in mapping_rows0]
    try:
        with tgt_connection.cursor() as cursor:
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.executemany(insert_query, rows)
            insert_query1 = f"INSERT INTO mapping_table ({columns1}) VALUES ({placeholders1})"
            cursor.executemany(insert_query1, mapping_rows)
            tgt_connection.commit()
        print('Data and mapping table insert successful for invoices - New insert!')
    except Exception as e:
        logging.error(f"Data and mapping table insert failed for invoices - New insert! Error occurred: {e} \n Query: {insert_query} \n Mapping Query: {insert_query1} \n sample row: {rows[0] if rows else 'No rows to insert'} \n sample mapping row: {mapping_rows[0] if mapping_rows else 'No mapping rows to insert'}")
        #print("Query:", insert_query)
        #print("Sample Row:", rows[0] if rows else "No rows to insert")
        print('Data and mapping table insert failed for invoices - New insert!')

# Insert new records and mapping entries simultaneously
insert_new_records_and_mapping(src_invoices_df1, tgt_connection, practice_name, table_name)




