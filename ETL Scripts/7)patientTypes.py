import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_src_myconnection, get_tgt_myconnection,get_patient_records,getPractice

#setup_logging(os.path.splitext(os.path.basename(__file__))[0])

import warnings
warnings.filterwarnings("ignore")

src_connection = get_src_myconnection()
tgt_connection = get_tgt_myconnection()
tgt_cursor = tgt_connection.cursor()
patient_ids = get_patient_records()
practice_name = getPractice()
table_name = 'patient_types'

src_patient_types  = f"SELECT * FROM patient_types"
src_patient_types_df = pd.read_sql(src_patient_types, src_connection)
#drop the duplicate names (case-insensitive)
src_patient_types_df['name_upper'] = src_patient_types_df['name'].str.upper()
src_patient_types_df = src_patient_types_df.drop_duplicates(subset=['name_upper'], keep='first')

tgt_patient_types = 'SELECT id as target_id,UPPER(name) as patient_type_name FROM patient_types'
tgt_patient_types_df = pd.read_sql(tgt_patient_types, tgt_connection)

#----------------------------new data insertion--------------------------------
src_patient_types_df1 = src_patient_types_df[~src_patient_types_df['name'].str.upper().isin(tgt_patient_types_df['patient_type_name'])]
#id genration for new data
tgt_patient_types_max = f'SELECT CASE WHEN MAX(id) is NULL THEN 1 ELSE MAX(id) + 1 END as max_id FROM {table_name}'
tgt_patient_types_max_df = pd.read_sql(tgt_patient_types_max, tgt_connection)
max_id = int(tgt_patient_types_max_df['max_id'].iloc[0])
src_patient_types_df1.insert(0, 'target_id', range(max_id, max_id + len(src_patient_types_df1)))
src_patient_types_df1 = src_patient_types_df1.drop(columns=['name_upper'])

#-------------------------------existing data mapping table update--------------------------------
# Merge the source and target dataframes to find existing records
src_patient_types_df['name_upper'] = src_patient_types_df['name'].str.upper()
src_patient_types_df2 = pd.merge(src_patient_types_df, tgt_patient_types_df, left_on='name_upper', right_on='patient_type_name', how='inner')
src_patient_types_df2 = src_patient_types_df2.drop(columns=['name_upper', 'patient_type_name'])
src_patient_types_df2 = src_patient_types_df2[['target_id','id']].rename(columns={'id': 'source_id'})

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
        print('Data and mapping table insert successful for patient_types - New insert!')
    except Exception as e:
        logging.error(f"Data and mapping table insert failed for patient_types - New insert! Error occurred: {e} \n Query: {insert_query} \n Mapping Query: {insert_query1} \n sample row: {rows[0] if rows else 'No rows to insert'}")
        #print("Query:", insert_query)
        #print("Sample Row:", rows[0] if rows else "No rows to insert")
        print('Data and mapping table insert failed for patient_types - New insert!')

# Insert new records and mapping entries simultaneously
insert_new_records_and_mapping(src_patient_types_df1, tgt_connection, practice_name, table_name)

#----------------------------existing data mapping table update--------------------------------
def update_existing_records_and_mapping(df, tgt_connection, practice_name, table_name):
    # Fetch existing source_ids from mapping_table
    existing_source_ids_query = f'''
        SELECT source_id FROM mapping_table
        WHERE source = "{practice_name}" AND table_name = "{table_name}" AND source = "{practice_name}"
    '''
    existing_source_ids_df = pd.read_sql(existing_source_ids_query, tgt_connection)
    existing_source_ids_df = existing_source_ids_df.drop_duplicates()
    
    # Filter out rows where id not in existing_source_ids
    df['source_id'] = df['source_id'].astype(str)
    existing_source_ids_df['source_id'] = existing_source_ids_df['source_id'].astype(str)

    non_existing_records_df = pd.merge(df, existing_source_ids_df, on='source_id', how='left', indicator=True).query('_merge == "left_only"').drop(columns=['_merge'])
    non_existing_records_df = non_existing_records_df.drop_duplicates()
    if non_existing_records_df.empty:
        print("No existing records to update.")
        return

    # Insert existing records into the target table
    column_names = [col for col in non_existing_records_df.columns]
    columns = ", ".join(column_names)
    placeholders = ", ".join(["%s"] * len(column_names))
    rows = non_existing_records_df.values.tolist()

    column_names1 = ['source', 'table_name', 'target_id', 'source_id','new_or_exist']
    columns1 = ", ".join(column_names1)
    placeholders1 = ", ".join(["%s"] * len(column_names1))
    mapping_rows = [(practice_name, table_name, row[0], row[1],'existing') for row in rows]

    try:
        with tgt_connection.cursor() as cursor:
            insert_query1 = f"INSERT INTO mapping_table ({columns1}) VALUES ({placeholders1})"
            cursor.executemany(insert_query1, mapping_rows)
            tgt_connection.commit()
        print('Data and mapping table insert successful for patient_types - Existing update!')
    except Exception as e:
        logging.error(f"Data and mapping table insert failed for patient_types - Existing update! Error occurred: {e} \n Mapping Query: {insert_query1} \n sample Row: {rows[0] if rows else 'No rows to insert'}")
        #print("Query:", insert_query1)
        #print("Sample Row:", rows[0] if rows else "No rows to insert")
        print('Data and mapping table insert failed for patient_types - Existing update!')

# Update existing records and mapping entries simultaneously
update_existing_records_and_mapping(src_patient_types_df2, tgt_connection, practice_name, table_name)

