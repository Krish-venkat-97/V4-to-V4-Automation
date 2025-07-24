import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_src_myconnection, get_tgt_myconnection,get_patient_records,getPractice

import warnings
warnings.filterwarnings("ignore")

#setup_logging(os.path.splitext(os.path.basename(__file__))[0])

src_connection = get_src_myconnection()
tgt_connection = get_tgt_myconnection()
tgt_cursor = tgt_connection.cursor()
patient_ids = get_patient_records()
practice_name = getPractice()
table_name = 'insurance_plans'

src_insurance_plans  = f"SELECT * FROM insurance_plans"
src_insurance_plans_df = pd.read_sql(src_insurance_plans, src_connection)
#drop the duplicate names (case-insensitive)
src_insurance_plans_df['name_upper'] = src_insurance_plans_df['name'].str.upper()
src_insurance_plans_df = src_insurance_plans_df.drop_duplicates(subset=['name_upper','insurance_company_id'], keep='first')

tgt_insurance_plans = 'SELECT id as target_id,UPPER(name) as tgt_insurance_plan_name,insurance_company_id as tgt_insurance_company_id FROM insurance_plans'
tgt_insurance_plans_df = pd.read_sql(tgt_insurance_plans, tgt_connection)

tgt_mapping_table = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'insurance_companies'"
tgt_mapping_table_df = pd.read_sql(tgt_mapping_table, tgt_connection)

# Merge the mapping table to make target_id from tgt_mapping_table as insurance_company_id
src_insurance_plans_df['insurance_company_id'] = src_insurance_plans_df['insurance_company_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
tgt_mapping_table_df['source_id'] = tgt_mapping_table_df['source_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
src_insurance_plans_df = pd.merge(src_insurance_plans_df, tgt_mapping_table_df, left_on='insurance_company_id', right_on='source_id', how='left')
src_insurance_plans_df = src_insurance_plans_df.drop(columns=['insurance_company_id', 'source_id'])
src_insurance_plans_df = src_insurance_plans_df.rename(columns={'target_id': 'insurance_company_id'})

#----------------------------new data insertion--------------------------------
# Create tuple columns for comparison
src_insurance_plans_df['name_upper'] = src_insurance_plans_df['name'].str.upper()
src_insurance_plans_df['key'] = list(zip(src_insurance_plans_df['name_upper'], src_insurance_plans_df['insurance_company_id']))
tgt_insurance_plans_df['key'] = list(zip(tgt_insurance_plans_df['tgt_insurance_plan_name'], tgt_insurance_plans_df['tgt_insurance_company_id']))

# Filter rows where the (name, insurance_company_id) pair is NOT in the target
src_insurance_plans_df1 = src_insurance_plans_df[~src_insurance_plans_df['key'].isin(tgt_insurance_plans_df['key'])]
src_insurance_plans_df1 = src_insurance_plans_df1.drop(columns=['key'])

#id genration for new data
tgt_insurance_plans_max = f'SELECT MAX(id) as max_id FROM {table_name}'
tgt_insurance_plans_max_df = pd.read_sql(tgt_insurance_plans_max, tgt_connection)
max_id = tgt_insurance_plans_max_df['max_id'][0] + 1 if not tgt_insurance_plans_max_df.empty else 1
src_insurance_plans_df1.insert(0, 'target_id', range(max_id, max_id + len(src_insurance_plans_df1)))
src_insurance_plans_df1 = src_insurance_plans_df1.drop(columns=['name_upper'])

#-------------------------------existing data mapping table update--------------------------------
# Merge the source and target dataframes to find existing records
src_insurance_plans_df['name_upper'] = src_insurance_plans_df['name'].str.upper()
src_insurance_plans_df['key'] = list(zip(src_insurance_plans_df['name_upper'], src_insurance_plans_df['insurance_company_id']))
tgt_insurance_plans_df['key'] = list(zip(tgt_insurance_plans_df['tgt_insurance_plan_name'], tgt_insurance_plans_df['tgt_insurance_company_id']))

src_insurance_plans_df2 = pd.merge(src_insurance_plans_df, tgt_insurance_plans_df, left_on=['key'], right_on=['key'], how='inner')
src_insurance_plans_df2 = src_insurance_plans_df2.drop(columns=['key', 'tgt_insurance_plan_name'])
src_insurance_plans_df2 = src_insurance_plans_df2[['target_id','id']].rename(columns={'id': 'source_id'})

# Before inserting new records, check mapping_table for existing source_ids to avoid duplicates
def insert_new_records_and_mapping(df, tgt_connection, practice_name, table_name):
    # Fetch existing source_ids from mapping_table
    existing_source_ids_query = f'''
        SELECT source_id FROM mapping_table
        WHERE source = "{practice_name}" AND table_name = "{table_name}"
    '''
    existing_source_ids_df = pd.read_sql(existing_source_ids_query, tgt_connection)
    existing_source_ids = set(existing_source_ids_df['source_id'].tolist())
    # Filter out rows where id is in existing_source_ids
    new_records_df = df[~df['id'].isin(existing_source_ids)]
    mapping_records_df = df[~df['id'].isin(existing_source_ids)][['id', 'target_id']].rename(columns={'id': 'source_id'})
    mapping_records_df = mapping_records_df.drop_duplicates(subset=['source_id'], keep='first')
    new_records_df =new_records_df.drop(columns=['id'])
    new_records_df = new_records_df.rename(columns={'target_id': 'id'})
    new_records_df = new_records_df.replace({pd.NaT: None}).replace({np.nan: None})
    if new_records_df.empty:
        print("No new records to insert.", flush=True)
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
        print('Data and mapping table insert successful for insurance_plans - New insert!')
    except Exception as e:
        logging.error(f"Data and mapping table insert failed for insurance_plans - New insert! Error occurred: {e} \n Query: {insert_query} \n Mapping Query: {insert_query1} \n sample row: {rows[0] if rows else 'No rows to insert'}",flush=True)
        #print("Query:", insert_query)
        #print("Sample Row:", rows[0] if rows else "No rows to insert")
        print('Data and mapping table insert failed for insurance_plans - New insert!', flush=True)

# Insert new records and mapping entries simultaneously
insert_new_records_and_mapping(src_insurance_plans_df1, tgt_connection, practice_name, table_name)

#----------------------------existing data mapping table update--------------------------------
def update_existing_records_and_mapping(df, tgt_connection, practice_name, table_name):
    # Fetch existing source_ids from mapping_table
    existing_source_ids_query = f'''
        SELECT source_id FROM mapping_table
        WHERE source = "{practice_name}" AND table_name = "{table_name}"
    '''
    existing_source_ids_df = pd.read_sql(existing_source_ids_query, tgt_connection)
    existing_source_ids = set(existing_source_ids_df['source_id'].tolist())
    
    # Filter out rows where id is in existing_source_ids
    existing_records_df = df[~df['source_id'].isin(existing_source_ids)]
    existing_records_df = existing_records_df.drop_duplicates(subset=['source_id'], keep='first')
    if existing_records_df.empty:
        print("No existing records to update.")
        return

    # Insert existing records into the target table
    column_names = [col for col in existing_records_df.columns]
    columns = ", ".join(column_names)
    placeholders = ", ".join(["%s"] * len(column_names))
    rows = existing_records_df.values.tolist()

    column_names1 = ['source', 'table_name', 'target_id', 'source_id','new_or_exist']
    columns1 = ", ".join(column_names1)
    placeholders1 = ", ".join(["%s"] * len(column_names1))
    mapping_rows = [(practice_name, table_name, row[0], row[1],'existing') for row in rows]

    try:
        with tgt_connection.cursor() as cursor:
            insert_query1 = f"INSERT INTO mapping_table ({columns1}) VALUES ({placeholders1})"
            cursor.executemany(insert_query1, mapping_rows)
            tgt_connection.commit()
        print('Data and mapping table insert successful for insurance_plans - Existing update!', flush=True)
    except Exception as e:
        logging.error(f"Data and mapping table insert failed for insurance_plans - Existing update! Error occurred: {e} \n Mapping Query: {insert_query1}", flush=True)
        #print("Query:", insert_query1)
        #print("Sample Row:", rows[0] if rows else "No rows to insert")
        print('Data and mapping table insert failed for insurance_plans - Existing update!',flush=True)

# Update existing records and mapping entries simultaneously
update_existing_records_and_mapping(src_insurance_plans_df2, tgt_connection, practice_name, table_name)

