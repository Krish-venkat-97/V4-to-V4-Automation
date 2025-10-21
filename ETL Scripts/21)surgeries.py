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
table_name = 'surgeries'

src_surgeries  = f"SELECT * FROM surgeries WHERE patient_id IN ({','.join(map(str, patient_ids))})"
src_surgeries_df = pd.read_sql(src_surgeries, src_connection)

tgt_mapping_patient_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'patients'"
tgt_mapping_patient_table_df = pd.read_sql(tgt_mapping_patient_table, tgt_connection)

tgt_mapping_episode_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'episodes'"
tgt_mapping_episode_table_df = pd.read_sql(tgt_mapping_episode_table, tgt_connection)

tgt_mapping_procedure_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'procedures'"
tgt_mapping_procedure_table_df = pd.read_sql(tgt_mapping_procedure_table, tgt_connection)

tgt_mapping_hospital_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'hospitals'"
tgt_mapping_hospital_table_df = pd.read_sql(tgt_mapping_hospital_table, tgt_connection)

#---------------------------patient_id----------------------------------
# Merging source surgeries with target patients mapping to get target patient_id
src_surgeries_df['patient_id'] = src_surgeries_df['patient_id'].astype(int)
tgt_mapping_patient_table_df['source_id'] = tgt_mapping_patient_table_df['source_id'].astype(int)
src_surgeries_df = pd.merge(src_surgeries_df, tgt_mapping_patient_table_df, left_on='patient_id', right_on='source_id', how='left')
src_surgeries_df = src_surgeries_df.drop(columns=['patient_id', 'source_id'])
src_surgeries_df = src_surgeries_df.rename(columns={'target_id': 'patient_id'})

#----------------------------procedure_id---------------------------
# Merging source surgeries description with target prcoedures mapping to get target procedure_id
src_surgeries_df['procedure_id'] = src_surgeries_df['procedure_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
tgt_mapping_procedure_table_df['source_id'] = tgt_mapping_procedure_table_df['source_id'].astype(int)
src_surgeries_df = pd.merge(src_surgeries_df, tgt_mapping_procedure_table_df, left_on='procedure_id', right_on='source_id', how='left')
src_surgeries_df = src_surgeries_df.drop(columns=['procedure_id', 'source_id'])
src_surgeries_df = src_surgeries_df.rename(columns={'target_id': 'procedure_id'})

#----------------------------procedure2_id---------------------------
# Merging source surgeries description with target prcoedures mapping to get target procedure_id
if src_surgeries_df['procedure2_id'].isnull().all():
    src_surgeries_df['procedure2_id'] = None
else:
    src_surgeries_df['procedure2_id'] = src_surgeries_df['procedure2_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    tgt_mapping_procedure_table_df['source_id'] = tgt_mapping_procedure_table_df['source_id'].astype(int)
    src_surgeries_df = pd.merge(src_surgeries_df, tgt_mapping_procedure_table_df, left_on='procedure2_id', right_on='source_id', how='left')
    src_surgeries_df = src_surgeries_df.drop(columns=['procedure2_id', 'source_id'])
    src_surgeries_df = src_surgeries_df.rename(columns={'target_id': 'procedure2_id'})

#----------------------------procedure3_id---------------------------
# Merging source surgeries description with target prcoedures mapping to get target procedure_id
if src_surgeries_df['procedure3_id'].isnull().all():
    src_surgeries_df['procedure3_id'] = None
else:
    src_surgeries_df['procedure3_id'] = src_surgeries_df['procedure3_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    tgt_mapping_procedure_table_df['source_id'] = tgt_mapping_procedure_table_df['source_id'].astype(int)
    src_surgeries_df = pd.merge(src_surgeries_df, tgt_mapping_procedure_table_df, left_on='procedure3_id', right_on='source_id', how='left')
    src_surgeries_df = src_surgeries_df.drop(columns=['procedure3_id', 'source_id'])
    src_surgeries_df = src_surgeries_df.rename(columns={'target_id': 'procedure3_id'})

#----------------------------procedure4_id---------------------------
# Merging source surgeries description with target prcoedures mapping to get target procedure_id
if src_surgeries_df['procedure4_id'].isnull().all():
    src_surgeries_df['procedure4_id'] = None
else:
    src_surgeries_df['procedure4_id'] = src_surgeries_df['procedure4_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    tgt_mapping_procedure_table_df['source_id'] = tgt_mapping_procedure_table_df['source_id'].astype(int)
    src_surgeries_df = pd.merge(src_surgeries_df, tgt_mapping_procedure_table_df, left_on='procedure4_id', right_on='source_id', how='left')
    src_surgeries_df = src_surgeries_df.drop(columns=['procedure4_id', 'source_id'])
    src_surgeries_df = src_surgeries_df.rename(columns={'target_id': 'procedure4_id'})

#----------------------------episode_id-----------------------------------
# Merging source surgeries with target episodes mapping to get target episode_id
if src_surgeries_df['episode_id'].isnull().all():
    src_surgeries_df['episode_id'] = None
else:
    src_surgeries_df['episode_id'] = src_surgeries_df['episode_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    tgt_mapping_episode_table_df['source_id'] = tgt_mapping_episode_table_df['source_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    src_surgeries_df = pd.merge(src_surgeries_df, tgt_mapping_episode_table_df, left_on='episode_id', right_on='source_id', how='left')
    src_surgeries_df = src_surgeries_df.drop(columns=['episode_id', 'source_id'])
    src_surgeries_df = src_surgeries_df.rename(columns={'target_id': 'episode_id'})

#----------------------------hospital_id-----------------------------------
# Merging source surgeries with target hospitals mapping to get target hospital_id
if src_surgeries_df['service_hospital_id'].isnull().all():
    src_surgeries_df['service_hospital_id'] = None
else:
    src_surgeries_df['service_hospital_id'] = src_surgeries_df['service_hospital_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    tgt_mapping_hospital_table_df['source_id'] = tgt_mapping_hospital_table_df['source_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    src_surgeries_df = pd.merge(src_surgeries_df, tgt_mapping_hospital_table_df, left_on='service_hospital_id', right_on='source_id', how='left')
    src_surgeries_df = src_surgeries_df.drop(columns=['service_hospital_id', 'source_id'])
    src_surgeries_df = src_surgeries_df.rename(columns={'target_id': 'service_hospital_id'})

#----------------------------wrtting default values--------------------
src_surgeries_df['invoice_id'] = 0
src_surgeries_df['appointment_id'] = 0
#----------------------------new data insertion--------------------------------
src_surgeries_df1 = src_surgeries_df
#id genration for new data
tgt_surgeries_max = f'SELECT CASE WHEN MAX(id) is NULL THEN 1 ELSE MAX(id) + 1 END as max_id FROM {table_name}'
tgt_surgeries_max_df = pd.read_sql(tgt_surgeries_max, tgt_connection)
max_id = int(tgt_surgeries_max_df['max_id'].iloc[0])
src_surgeries_df1.insert(0, 'target_id', range(max_id, max_id + len(src_surgeries_df1)))

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
        print('Data and mapping table insert successful for surgeries - New insert!')
    except Exception as e:
        logging.error(f"Data and mapping table insert failed for surgeries - New insert! Error occurred: {e} \n Query: {insert_query} \n Mapping Query: {insert_query1} \n sample row: {rows[0] if rows else 'No rows to insert'} \n sample mapping row: {mapping_rows[0] if mapping_rows else 'No mapping rows to insert'}", flush=True)
        #print("Query:", insert_query)
        #print("Sample Row:", rows[0] if rows else "No rows to insert")
        print('Data and mapping table insert failed for surgeries - New insert!')

# Insert new records and mapping entries simultaneously
insert_new_records_and_mapping(src_surgeries_df1, tgt_connection, practice_name, table_name)




