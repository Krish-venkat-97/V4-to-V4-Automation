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
table_name = 'scan_documents'

src_scan_documents  = f"SELECT * FROM scan_documents WHERE patient_id IN ({','.join(map(str, patient_ids))})"
src_scan_documents_df = pd.read_sql(src_scan_documents, src_connection)

tgt_mapping_patient_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'patients'"
tgt_mapping_patient_table_df = pd.read_sql(tgt_mapping_patient_table, tgt_connection)

tgt_scan_categories_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'scan_categories'"
tgt_scan_categories_table_df = pd.read_sql(tgt_scan_categories_table, tgt_connection)

tgt_mapping_episode_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'episodes'"
tgt_mapping_episode_table_df = pd.read_sql(tgt_mapping_episode_table, tgt_connection)

#---------------------------patient_id----------------------------------
# Merging source scan_documents with target patients mapping to get target patient_id
src_scan_documents_df['patient_id'] = src_scan_documents_df['patient_id'].astype(int)
tgt_mapping_patient_table_df['source_id'] = tgt_mapping_patient_table_df['source_id'].astype(int)
src_scan_documents_df = pd.merge(src_scan_documents_df, tgt_mapping_patient_table_df, left_on='patient_id', right_on='source_id', how='left')
src_scan_documents_df = src_scan_documents_df.drop(columns=['patient_id', 'source_id'])
src_scan_documents_df = src_scan_documents_df.rename(columns={'target_id': 'patient_id'})

#----------------------------scan_category_id-----------------------------------
# Merging source scan_documents with target scan_categories mapping to get target scan_category_id
if src_scan_documents_df['scan_category_id'].isnull().all():
    src_scan_documents_df['scan_category_id'] = 1
else:
    src_scan_documents_df['scan_category_id'] = src_scan_documents_df['scan_category_id'].apply(lambda x: int(x) if pd.notnull(x) else 1)
    tgt_scan_categories_table_df['source_id'] = tgt_scan_categories_table_df['source_id'].apply(lambda x: int(x) if pd.notnull(x) else 1)
    src_scan_documents_df = pd.merge(src_scan_documents_df, tgt_scan_categories_table_df, left_on='scan_category_id', right_on='source_id', how='left')
    src_scan_documents_df = src_scan_documents_df.drop(columns=['scan_category_id', 'source_id'])
    src_scan_documents_df = src_scan_documents_df.rename(columns={'target_id': 'scan_category_id'})

#----------------------------episode_id-----------------------------------
# Merging source scan_documents with target episodes mapping to get target episode_id
if src_scan_documents_df['episode_id'].isnull().all():
    src_scan_documents_df['episode_id'] = None
else:
    src_scan_documents_df['episode_id'] = src_scan_documents_df['episode_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    tgt_mapping_episode_table_df['source_id'] = tgt_mapping_episode_table_df['source_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    src_scan_documents_df = pd.merge(src_scan_documents_df, tgt_mapping_episode_table_df, left_on='episode_id', right_on='source_id', how='left')
    src_scan_documents_df = src_scan_documents_df.drop(columns=['episode_id', 'source_id'])
    src_scan_documents_df = src_scan_documents_df.rename(columns={'target_id': 'episode_id'})

#-----------------------------default values--------------------------------
src_scan_documents_df['file_path'] = ''

#----------------------------new data insertion--------------------------------
src_scan_documents_df1 = src_scan_documents_df
#id generation for new data
tgt_scan_documents_max = f'SELECT MAX(id) as max_id FROM {table_name}'
tgt_scan_documents_max_df = pd.read_sql(tgt_scan_documents_max, tgt_connection)
max_id = tgt_scan_documents_max_df['max_id'][0] + 1 if not tgt_scan_documents_max_df.empty else 1
src_scan_documents_df1.insert(0, 'target_id', range(max_id, max_id + len(src_scan_documents_df1)))

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
        print('Data and mapping table insert successful for contacts - New insert!')
    except Exception as e:
        logging.error(f"Data and mapping table insert failed for contacts - New insert! Error occurred: {e} \n Query: {insert_query} \n Mapping Query: {insert_query1} \n sample row: {rows[0] if rows else 'No rows to insert'} \n sample mapping row: {mapping_rows[0] if mapping_rows else 'No mapping rows to insert'}", flush=True)
        print("Query:", insert_query)
        print("Sample Row:", rows[0] if rows else "No rows to insert")
        print('Data and mapping table insert failed for contacts - New insert!')

# Insert new records and mapping entries simultaneously
insert_new_records_and_mapping(src_scan_documents_df1, tgt_connection, practice_name, table_name)





