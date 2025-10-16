import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
import numpy as np
import pandas as pd
from src.utils import get_src_myconnection, get_tgt_myconnection,get_patient_records,getPractice

import warnings
warnings.filterwarnings("ignore")

src_connection = get_src_myconnection()
tgt_connection = get_tgt_myconnection()
tgt_cursor = tgt_connection.cursor()
patient_ids = get_patient_records()
practice_name = getPractice()
table_name = 'appointment_description_procedures'

src_appointment_description_procedures  = f"SELECT * FROM appointment_description_procedures"
src_appointment_description_procedures_df = pd.read_sql(src_appointment_description_procedures, src_connection)

tgt_mapping_procedure_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'procedures'AND new_or_exist = 'new'"
tgt_mapping_procedure_table_df = pd.read_sql(tgt_mapping_procedure_table, tgt_connection)

tgt_appointment_descriptions_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'appointment_descriptions' AND new_or_exist = 'new'"
tgt_appointment_descriptions_table_df = pd.read_sql(tgt_appointment_descriptions_table, tgt_connection)

#---------------------------appointment_description_id----------------------------------
# Merging source appointment_description_procedures with target appointment descriptions mapping to get target appointment_description_id
src_appointment_description_procedures_df['appointment_description_id'] = src_appointment_description_procedures_df['appointment_description_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
tgt_appointment_descriptions_table_df['source_id'] = tgt_appointment_descriptions_table_df['source_id'].astype(int)
src_appointment_description_procedures_df = pd.merge(src_appointment_description_procedures_df, tgt_appointment_descriptions_table_df, left_on='appointment_description_id', right_on='source_id', how='inner')
src_appointment_description_procedures_df = src_appointment_description_procedures_df.drop(columns=['appointment_description_id', 'source_id'])
src_appointment_description_procedures_df = src_appointment_description_procedures_df.rename(columns={'target_id': 'appointment_description_id'})

#----------------------------procedure_id-----------------------------------
# Merging source appointment_description_procedures with target procedures mapping to get target procedure_id
src_appointment_description_procedures_df['procedure_id'] = src_appointment_description_procedures_df['procedure_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
tgt_mapping_procedure_table_df['source_id'] = tgt_mapping_procedure_table_df['source_id'].astype(int)
src_appointment_description_procedures_df = pd.merge(src_appointment_description_procedures_df, tgt_mapping_procedure_table_df, left_on='procedure_id', right_on='source_id', how='inner')
src_appointment_description_procedures_df = src_appointment_description_procedures_df.drop(columns=['procedure_id', 'source_id'])
src_appointment_description_procedures_df = src_appointment_description_procedures_df.rename(columns={'target_id': 'procedure_id'})

#----------------------------new data insertion--------------------------------
src_appointment_description_procedures_df1 = src_appointment_description_procedures_df
#id generation for new data
tgt_appointment_description_procedures_max = f'SELECT CASE WHEN MAX(id) is NULL THEN 1 ELSE MAX(id) + 1 END as max_id FROM {table_name}'
tgt_appointment_description_procedures_max_df = pd.read_sql(tgt_appointment_description_procedures_max, tgt_connection)
max_id = int(tgt_appointment_description_procedures_max_df['max_id'].iloc[0])
src_appointment_description_procedures_df1.insert(0, 'target_id', range(max_id, max_id + len(src_appointment_description_procedures_df1)))

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
        print(f'Data and mapping table insert successful for {table_name} - New insert!')
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        print("Query:", insert_query)
        print("Sample Row:", rows[0] if rows else "No rows to insert")
        print(f'Data and mapping table insert failed for {table_name} - New insert!')

# Insert new records and mapping entries simultaneously
insert_new_records_and_mapping(src_appointment_description_procedures_df1, tgt_connection, practice_name, table_name)





