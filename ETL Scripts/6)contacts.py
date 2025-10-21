import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
import numpy as np
import pandas as pd
from src.utils import get_src_myconnection, get_tgt_myconnection,get_patient_records,getPractice

import warnings
warnings.filterwarnings("ignore")

#setup_logging(os.path.splitext(os.path.basename(__file__))[0])

src_connection = get_src_myconnection()
tgt_connection = get_tgt_myconnection()
tgt_cursor = tgt_connection.cursor()
patient_ids = get_patient_records()
practice_name = getPractice()
table_name = 'contacts'

src_contacts  = f"SELECT * FROM contacts"
src_contacts_df = pd.read_sql(src_contacts, src_connection)

tgt_mapping_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'contact_types'"
tgt_mapping_table_df = pd.read_sql(tgt_mapping_table, tgt_connection)

tgt_title_mapping = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'titles'"
tgt_title_mapping_df = pd.read_sql(tgt_title_mapping, tgt_connection)

#--------------------------contact type----------------------------------
#Merging source contacts with target contact types mapping to get target contact_type_id
src_contacts_df['contact_type_id'] = src_contacts_df['contact_type_id'].astype(int)
tgt_mapping_table_df['source_id'] = tgt_mapping_table_df['source_id'].astype(int)
src_contacts_df = pd.merge(src_contacts_df, tgt_mapping_table_df, left_on='contact_type_id', right_on='source_id', how='left')
src_contacts_df = src_contacts_df.drop(columns=['contact_type_id', 'source_id'])
src_contacts_df = src_contacts_df.rename(columns={'target_id': 'contact_type_id'})

#--------------------------title----------------------------------
#Merging source contacts with target titles mapping to get target title_id
src_contacts_df['title_id'] = src_contacts_df['title_id'].astype(int)
tgt_title_mapping_df['source_id'] = tgt_title_mapping_df['source_id'].astype(int)
src_contacts_df = pd.merge(src_contacts_df, tgt_title_mapping_df, left_on='title_id', right_on='source_id', how='left')
src_contacts_df = src_contacts_df.drop(columns=['title_id', 'source_id'])
src_contacts_df = src_contacts_df.rename(columns={'target_id': 'title_id'})
src_contacts_df['title_id'] = src_contacts_df['title_id'].fillna(0).astype(int)  # Fill NaN with 0 and convert to int
#----------------------------new data insertion--------------------------------
src_contacts_df1 = src_contacts_df
#id genration for new data
tgt_contacts_max = f'SELECT CASE WHEN MAX(id) is NULL THEN 1 ELSE MAX(id) + 1 END as max_id FROM {table_name}'
tgt_contacts_max_df = pd.read_sql(tgt_contacts_max, tgt_connection)
max_id = int(tgt_contacts_max_df['max_id'].iloc[0])
src_contacts_df1.insert(0, 'target_id', range(max_id, max_id + len(src_contacts_df1)))

# Before inserting new records, check mapping_table for existing source_ids to avoid duplicates
def insert_new_records_and_mapping(df, tgt_connection, practice_name, table_name):
    # Fetch existing source_ids from mapping_table
    existing_source_ids_query = f'''
        SELECT source_id FROM mapping_table
        WHERE source = "{practice_name}" AND table_name = "{table_name}" AND source = "{practice_name}"
    '''
    existing_source_ids_df = pd.read_sql(existing_source_ids_query, tgt_connection)

    # Filter out rows where id is in existing_source_ids
    df['id'] = df['id'].astype(str)
    existing_source_ids_df['source_id'] = existing_source_ids_df['source_id'].astype(str)

    new_records_df = pd.merge(df, existing_source_ids_df, left_on='id', right_on='source_id', how='left', indicator=True).query('_merge == "left_only"').drop(columns=['_merge'])
    mapping_records_df = pd.merge(df, existing_source_ids_df, left_on='id', right_on='source_id', how='left', indicator=True).query('_merge == "left_only"').drop(columns=['_merge'])[['id', 'target_id']].rename(columns={'id': 'source_id'})
    mapping_records_df = mapping_records_df.drop_duplicates(subset=['source_id'], keep='first')
    new_records_df = new_records_df.drop(columns=['id','source_id'])
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
        logging.error(f"Data and mapping table insert failed for contacts - New insert! Error occurred: {e} \n Query: {insert_query} \n Mapping Query: {insert_query1} \n sample row: {rows[0] if rows else 'No rows to insert'} \n sample mapping row: {mapping_rows[0] if mapping_rows else 'No mapping rows to insert'}")
        #print("Query:", insert_query)
        #print("Sample Row:", rows[0] if rows else "No rows to insert")
        print('Data and mapping table insert failed for contacts - New insert!')

# Insert new records and mapping entries simultaneously
insert_new_records_and_mapping(src_contacts_df1, tgt_connection, practice_name, table_name)
