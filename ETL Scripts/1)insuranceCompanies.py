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
table_name = 'insurance_companies'

src_insurance_companies  = f"SELECT * FROM insurance_companies"
src_insurance_companies_df = pd.read_sql(src_insurance_companies, src_connection)

src_insurance_companies_df['name_upper'] = src_insurance_companies_df['name'].str.upper()
src_insurance_companies_df['address1_upper'] = src_insurance_companies_df['address1'].str.upper()

tgt_insurance_companies = 'SELECT id as target_id,UPPER(name) as insurance_company_name,UPPER(address1) as address1_name FROM insurance_companies'
tgt_insurance_companies_df = pd.read_sql(tgt_insurance_companies, tgt_connection)

#----------------------------new data insertion--------------------------------
#src_insurance_companies_df1 = src_insurance_companies_df[~src_insurance_companies_df['name'].str.upper().isin(tgt_insurance_companies_df['insurance_company_name'])]
src_insurance_companies_df1 = dd.merge(
    src_insurance_companies_df,
    tgt_insurance_companies_df,
    how='left',
    indicator=True,
    left_on=['name_upper', 'address1_upper'],
    right_on=['insurance_company_name', 'address1_name']
).query('_merge == "left_only"').drop(columns=['_merge', 'insurance_company_name', 'address1_name','target_id','address1_upper'])

#id genration for new data
tgt_insurance_companies_max = f'SELECT CASE WHEN MAX(id) is NULL THEN 1 ELSE MAX(id) + 1 END as max_id FROM {table_name}'
tgt_insurance_companies_max_df = pd.read_sql(tgt_insurance_companies_max, tgt_connection)
max_id = int(tgt_insurance_companies_max_df['max_id'].iloc[0])
src_insurance_companies_df1.insert(0, 'target_id', range(max_id, max_id + len(src_insurance_companies_df1)))

#-------------------------------existing data mapping table update--------------------------------
# Merge the source and target dataframes to find existing records
#src_insurance_companies_df['name_upper'] = src_insurance_companies_df['name'].str.upper()
#src_insurance_companies_df2 = pd.merge(src_insurance_companies_df, tgt_insurance_companies_df, left_on='name_upper', right_on='insurance_company_name', how='inner')
src_insurance_companies_df2 = dd.merge(
    src_insurance_companies_df,
    tgt_insurance_companies_df,
    how='inner',
    indicator=True,
    left_on=['name_upper', 'address1_upper'],
    right_on=['insurance_company_name', 'address1_name']
).drop(columns=['_merge', 'insurance_company_name', 'address1_name','address1_upper'])

src_insurance_companies_df2 = src_insurance_companies_df2[['target_id','id']].rename(columns={'id': 'source_id'})

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
    new_records_df = new_records_df.drop(columns=['id','name_upper','source_id'])
    new_records_df = new_records_df.rename(columns={'target_id': 'id'})
    new_records_df = new_records_df.replace({pd.NaT: None}).replace({np.nan: None})
    if new_records_df.empty:
        print("No new records to insert.",flush=True)
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
        print('Data and mapping table insert successful for insurance_companies - New insert!',flush=True)
    except Exception as e:
        logging.error(f"Data and mapping table insert failed for insurance_companies - New insert!: Error occurred: {e} \n Query: {insert_query} \n Mapping Query: {insert_query1} \n sample row: {rows[0] if rows else 'No rows to insert'}", flush=True)
        #print("Query:", insert_query)
        #print("Sample Row:", rows[0] if rows else "No rows to insert")
        print('Data and mapping table insert failed for insurance_companies - New insert!',flush=True)

# Insert new records and mapping entries simultaneously
insert_new_records_and_mapping(src_insurance_companies_df1, tgt_connection, practice_name, table_name)

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
        print("No existing records to update.",flush=True)
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
        print('Data and mapping table insert successful for insurance_companies - Existing update!')
    except Exception as e:
        logging.error(f"Data and mapping table insert failed for insurance_companies - Existing update! Error occurred: {e} \n Mapping Query: {insert_query1} \n sample row: {rows[0] if rows else 'No rows to insert'}", flush=True)
        #print("Query:", insert_query1)
        #print("Sample Row:", rows[0] if rows else "No rows to insert")
        print('Data and mapping table insert failed for insurance_companies - Existing update!',flush=True)

# Update existing records and mapping entries simultaneously
update_existing_records_and_mapping(src_insurance_companies_df2, tgt_connection, practice_name, table_name)

