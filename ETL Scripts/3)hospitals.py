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
table_name = 'hospitals'

src_hospitals  = f"SELECT * FROM hospitals"
src_hospitals_df = pd.read_sql(src_hospitals, src_connection)
#drop the duplicate names (case-insensitive)
src_hospitals_df['name_upper'] = src_hospitals_df['name'].str.upper()
src_hospitals_df = src_hospitals_df.drop_duplicates(subset=['name_upper'], keep='first')

tgt_hospitals = 'SELECT id as target_id,UPPER(name) as hospital_name FROM hospitals'
tgt_hospitals_df = pd.read_sql(tgt_hospitals, tgt_connection)

#----------------------------new data insertion--------------------------------
src_hospitals_df1 = src_hospitals_df[~src_hospitals_df['name'].str.upper().isin(tgt_hospitals_df['hospital_name'])]
#id genration for new data
tgt_hospitals_max = f'SELECT CASE WHEN MAX(id) is NULL THEN 1 ELSE MAX(id) + 1 END as max_id FROM {table_name}'
tgt_hospitals_max_df = pd.read_sql(tgt_hospitals_max, tgt_connection)
max_id = int(tgt_hospitals_max_df['max_id'].iloc[0])
src_hospitals_df1.insert(0, 'target_id', range(max_id, max_id + len(src_hospitals_df1)))
src_hospitals_df1 = src_hospitals_df1.drop(columns=['name_upper'])

#-------------------------------existing data mapping table update--------------------------------
# Merge the source and target dataframes to find existing records
src_hospitals_df['name_upper'] = src_hospitals_df['name'].str.upper()
src_hospitals_df2 = pd.merge(src_hospitals_df, tgt_hospitals_df, left_on='name_upper', right_on='hospital_name', how='inner')
src_hospitals_df2 = src_hospitals_df2.drop(columns=['name_upper', 'hospital_name'])
src_hospitals_df2 = src_hospitals_df2[['target_id','id']].rename(columns={'id': 'source_id'})

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
        print('Data and mapping table insert successful for hospitals - New insert!',flush=True)
    except Exception as e:
        logging.error(f"Data and mapping table insert successful for hospitals - New insert! Error occurred: {e} and \n Query: {insert_query} and \n Mapping Query: {insert_query1} \n sample row: {rows[0] if rows else 'No rows to insert'}", flush=True)
        #print("Query:", insert_query)
        #print("Sample Row:", rows[0] if rows else "No rows to insert")
        print('Data and mapping table insert failed for hospitals - New insert!',flush=True)

# Insert new records and mapping entries simultaneously
insert_new_records_and_mapping(src_hospitals_df1, tgt_connection, practice_name, table_name)

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
        print('Data and mapping table insert successful for hospitals - Existing update!')
    except Exception as e:
        logging.error(f"Data and mapping table insert failed for hospitals - Existing update! Error occurred: {e} \n Mapping Query: {insert_query1} \n sample row: {rows[0] if rows else 'No rows to insert'}", flush=True)
        #print("Query:", insert_query1)
        #print("Sample Row:", rows[0] if rows else "No rows to insert")
        print('Data and mapping table insert failed for hospitals - Existing update!')

# Update existing records and mapping entries simultaneously
update_existing_records_and_mapping(src_hospitals_df2, tgt_connection, practice_name, table_name)

