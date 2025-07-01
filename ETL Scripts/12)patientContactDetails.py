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
table_name = 'patient_contact_details'

src_patient_contact_details  = f"SELECT * FROM patient_contact_details WHERE patient_id IN ({','.join(map(str, patient_ids))})"
src_patient_contact_details_df = pd.read_sql(src_patient_contact_details, src_connection)

tgt_mapping_patient_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'patients'"
tgt_mapping_patient_table_df = pd.read_sql(tgt_mapping_patient_table, tgt_connection)

tgt_mapping_contact_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'contacts'"
tgt_mapping_contact_table_df = pd.read_sql(tgt_mapping_contact_table, tgt_connection)

tgt_contact_types_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'contact_types'"
tgt_contact_types_table_df = pd.read_sql(tgt_contact_types_table, tgt_connection)

#---------------------------patient_id----------------------------------
# Merging source patient_contact_details with target patients mapping to get target patient_id
src_patient_contact_details_df['patient_id'] = src_patient_contact_details_df['patient_id'].astype(int)
tgt_mapping_patient_table_df['source_id'] = tgt_mapping_patient_table_df['source_id'].astype(int)
src_patient_contact_details_df = pd.merge(src_patient_contact_details_df, tgt_mapping_patient_table_df, left_on='patient_id', right_on='source_id', how='left')
src_patient_contact_details_df = src_patient_contact_details_df.drop(columns=['patient_id', 'source_id'])
src_patient_contact_details_df = src_patient_contact_details_df.rename(columns={'target_id': 'patient_id'})

#----------------------------contact_id-----------------------------------
# Merging source patient_contact_details with target contacts mapping to get target contact_id
src_patient_contact_details_df['contact_id'] = src_patient_contact_details_df['contact_id'].astype(int)
tgt_mapping_contact_table_df['source_id'] = tgt_mapping_contact_table_df['source_id'].astype(int)
src_patient_contact_details_df = pd.merge(src_patient_contact_details_df, tgt_mapping_contact_table_df, left_on='contact_id', right_on='source_id', how='left')
src_patient_contact_details_df = src_patient_contact_details_df.drop(columns=['contact_id', 'source_id'])
src_patient_contact_details_df = src_patient_contact_details_df.rename(columns={'target_id': 'contact_id'})

#----------------------------contact_type_id-----------------------------------
# Merging source patient_contact_details with target contact_types mapping to get target contact_type_id
src_patient_contact_details_df['contact_type_id'] = src_patient_contact_details_df['contact_type_id'].astype(int)
tgt_contact_types_table_df['source_id'] = tgt_contact_types_table_df['source_id'].astype(int)
src_patient_contact_details_df = pd.merge(src_patient_contact_details_df, tgt_contact_types_table_df, left_on='contact_type_id', right_on='source_id', how='left')
src_patient_contact_details_df = src_patient_contact_details_df.drop(columns=['contact_type_id', 'source_id'])
src_patient_contact_details_df = src_patient_contact_details_df.rename(columns={'target_id': 'contact_type_id'})

#----------------------------new data insertion--------------------------------
src_patient_contact_details_df1 = src_patient_contact_details_df
#id genration for new data
tgt_patient_contact_details_max = f'SELECT MAX(id) as max_id FROM {table_name}'
tgt_patient_contact_details_max_df = pd.read_sql(tgt_patient_contact_details_max, tgt_connection)
max_id = tgt_patient_contact_details_max_df['max_id'][0] + 1 if not tgt_patient_contact_details_max_df.empty else 1
src_patient_contact_details_df1.insert(0, 'target_id', range(max_id, max_id + len(src_patient_contact_details_df1)))

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
    column_names = [col if col != 'primary' else '`primary`' for col in new_records_df.columns]
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
        print('Data and mapping table insert successful for patient_contact_details - New insert!')
    except Exception as e:
        logging.error(f"Data and mapping table insert failed for patient_contact_details - New insert! Error occurred: {e} \n Query: {insert_query} \n Mapping Query: {insert_query1} \n sample row: {rows[0] if rows else 'No rows to insert'} \n sample mapping row: {mapping_rows[0] if mapping_rows else 'No mapping rows to insert'}")
        #print("Query:", insert_query)
        #print("Sample Row:", rows[0] if rows else "No rows to insert")
        print('Data and mapping table insert failed for patient_contact_details - New insert!')

# Insert new records and mapping entries simultaneously
insert_new_records_and_mapping(src_patient_contact_details_df1, tgt_connection, practice_name, table_name)
