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
table_name = 'patients'

src_patients  = f"SELECT * FROM patients WHERE id in ({','.join(map(str, patient_ids))})"
src_patients_df = pd.read_sql(src_patients, src_connection)

tgt_patientType_mapping = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'patient_types'"
tgt_patientType_mapping_df = pd.read_sql(tgt_patientType_mapping, tgt_connection)

tgt_title_mapping = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'titles'"
tgt_title_mapping_df = pd.read_sql(tgt_title_mapping, tgt_connection)

tgt_insurance_company_mapping = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'insurance_companies'"
tgt_insurance_company_mapping_df = pd.read_sql(tgt_insurance_company_mapping, tgt_connection)

tgt_insurance_plan_mapping = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'insurance_companies'"
tgt_insurance_plan_mapping_df = pd.read_sql(tgt_insurance_plan_mapping, tgt_connection)

tgt_hospital_mapping = f"SELECT source_id, target_id FROM mapping_table WHERE source = '{getPractice()}' AND table_name = 'hospitals'"
tgt_hospital_mapping_df = pd.read_sql(tgt_hospital_mapping, tgt_connection)

tgt_insurance_plans = f'SELECT id FROM insurance_plans'
tgt_insurance_plans_df = pd.read_sql(tgt_insurance_plans, tgt_connection)
tgt_insurance_plans_df['id'] = tgt_insurance_plans_df['id'].astype(int)

#--------------------------patient type----------------------------------
#Merging source patients with target patient types mapping to get target patient type_id
src_patients_df['patient_type_id'] = src_patients_df['patient_type_id'].fillna(0)
src_patients_df['patient_type_id'] = src_patients_df['patient_type_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
tgt_patientType_mapping_df['source_id'] = tgt_patientType_mapping_df['source_id'].astype(int)
src_patients_df = pd.merge(src_patients_df, tgt_patientType_mapping_df, left_on='patient_type_id', right_on='source_id', how='left')
src_patients_df = src_patients_df.drop(columns=['patient_type_id', 'source_id'])
src_patients_df = src_patients_df.rename(columns={'target_id': 'patient_type_id'})

#--------------------------title----------------------------------
#Merging source patients with target titles mapping to get target title_id
src_patients_df['title_id'] = src_patients_df['title_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
tgt_title_mapping_df['source_id'] = tgt_title_mapping_df['source_id'].astype(int)
src_patients_df = pd.merge(src_patients_df, tgt_title_mapping_df, left_on='title_id', right_on='source_id', how='left')
src_patients_df = src_patients_df.drop(columns=['title_id', 'source_id'])
src_patients_df = src_patients_df.rename(columns={'target_id': 'title_id'})
src_patients_df['title_id'] = src_patients_df['title_id'].fillna(0).astype(int)  # Fill NaN with 0 and convert to int

#--------------------------insurance company----------------------------------
#Merging source patients with target insurance companies mapping to get target insurance_company_id
src_patients_df['primary_insurance_company_id'] = src_patients_df['primary_insurance_company_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
tgt_insurance_company_mapping_df['source_id'] = tgt_insurance_company_mapping_df['source_id'].astype(int)
src_patients_df = pd.merge(src_patients_df, tgt_insurance_company_mapping_df, left_on='primary_insurance_company_id', right_on='source_id', how='left')
src_patients_df = src_patients_df.drop(columns=['primary_insurance_company_id', 'source_id'])
src_patients_df = src_patients_df.rename(columns={'target_id': 'primary_insurance_company_id'})

#--------------------------secondary insurance company----------------------------------
#Merging source patients with target insurance companies mapping to get target insurance_company_id
#check if entire column is null
if src_patients_df['secondary_insurance_company_id'].isnull().all():
    src_patients_df['secondary_insurance_company_id'] = None
else:
    src_patients_df['secondary_insurance_company_id'] = src_patients_df['secondary_insurance_company_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    tgt_insurance_company_mapping_df['source_id'] = tgt_insurance_company_mapping_df['source_id'].astype(int)
    src_patients_df = pd.merge(src_patients_df, tgt_insurance_company_mapping_df, left_on='secondary_insurance_company_id', right_on='source_id', how='left')
    src_patients_df = src_patients_df.drop(columns=['secondary_insurance_company_id', 'source_id'])
    src_patients_df = src_patients_df.rename(columns={'target_id': 'secondary_insurance_company_id'})

#--------------------------insurance plan----------------------------------
#Merging source patients with target insurance plans mapping to get target insurance_plan_id
src_patients_df['primary_insurance_plan_id'] = src_patients_df['primary_insurance_plan_id'].fillna(0)
src_patients_df['primary_insurance_plan_id'] = src_patients_df['primary_insurance_plan_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
tgt_insurance_plan_mapping_df['source_id'] = tgt_insurance_plan_mapping_df['source_id'].astype(int)
src_patients_df = pd.merge(src_patients_df, tgt_insurance_plan_mapping_df, left_on='primary_insurance_plan_id', right_on='source_id', how='left')
src_patients_df = src_patients_df.drop(columns=['primary_insurance_plan_id', 'source_id'])
src_patients_df = src_patients_df.rename(columns={'target_id': 'primary_insurance_plan_id'})

#src_patients_df['primary_insurance_plan_id'] = src_patients_df['primary_insurance_plan_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
#src_patients_dfx0 = src_patients_df['primary_insurance_plan_id'].drop_duplicates()
#src_patients_dfx = pd.merge(src_patients_dfx0, tgt_insurance_plans_df, left_on='primary_insurance_plan_id', right_on='id', how='left',indicator=True).query('_merge == "left_only"')

#--------------------------secondary insurance plan----------------------------------
#Merging source patients with target insurance plans mapping to get target insurance_plan_id
#check if entire column is null
if src_patients_df['secondary_insurance_plan_id'].isnull().all():
    src_patients_df['secondary_insurance_plan_id'] = None
else:
    src_patients_df['secondary_insurance_plan_id'] = src_patients_df['secondary_insurance_plan_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    tgt_insurance_plan_mapping_df['source_id'] = tgt_insurance_plan_mapping_df['source_id'].astype(int)
    src_patients_df = pd.merge(src_patients_df, tgt_insurance_plan_mapping_df, left_on='secondary_insurance_plan_id', right_on='source_id', how='left')
    src_patients_df = src_patients_df.drop(columns=['secondary_insurance_plan_id', 'source_id'])
    src_patients_df = src_patients_df.rename(columns={'target_id': 'secondary_insurance_plan_id'})

#--------------------------hospital----------------------------------
#Merging source patients with target hospitals mapping to get target hospital_id
#check if entire column is null
if src_patients_df['hospital_id'].isnull().all():
    src_patients_df['hospital_id'] = None
else:
    src_patients_df['hospital_id'] = src_patients_df['hospital_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    tgt_hospital_mapping_df['source_id'] = tgt_hospital_mapping_df['source_id'].astype(int)
    src_patients_df = pd.merge(src_patients_df, tgt_hospital_mapping_df, left_on='hospital_id', right_on='source_id', how='left')
    src_patients_df = src_patients_df.drop(columns=['hospital_id', 'source_id'])
    src_patients_df = src_patients_df.rename(columns={'target_id': 'hospital_id'})

#----------------------------hospital2_id----------------------------------
#Merging source patients with target hospitals mapping to get target hospital2_id
#check if entire column is null
if src_patients_df['hospital2_id'].isnull().all():
    src_patients_df['hospital2_id'] = None
else:
    src_patients_df['hospital2_id'] = src_patients_df['hospital2_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    tgt_hospital_mapping_df['source_id'] = tgt_hospital_mapping_df['source_id'].astype(int)
    src_patients_df = pd.merge(src_patients_df, tgt_hospital_mapping_df, left_on='hospital2_id', right_on='source_id', how='left')
    src_patients_df = src_patients_df.drop(columns=['hospital2_id', 'source_id'])
    src_patients_df = src_patients_df.rename(columns={'target_id': 'hospital2_id'})

#----------------------------hospital3_id----------------------------------
#Merging source patients with target hospitals mapping to get target hospital3_id
#check if entire column is null
if src_patients_df['hospital3_id'].isnull().all():
    src_patients_df['hospital3_id'] = None
else:
    src_patients_df['hospital3_id'] = src_patients_df['hospital3_id'].astype('Int64')
    tgt_hospital_mapping_df['source_id'] = tgt_hospital_mapping_df['source_id'].astype('Int64')
    src_patients_df = pd.merge(src_patients_df, tgt_hospital_mapping_df, left_on='hospital3_id', right_on='source_id', how='left')
    src_patients_df = src_patients_df.drop(columns=['hospital3_id', 'source_id'])
    src_patients_df = src_patients_df.rename(columns={'target_id': 'hospital3_id'})

#--------------------------registered_hospital_id----------------------------------
#Merging source patients with target hospitals mapping to get target registered_hospital_id
#check if entire column is null
if src_patients_df['registered_hospital_id'].isnull().all():
    src_patients_df['registered_hospital_id'] = None
else:
    src_patients_df['registered_hospital_id'] = src_patients_df['registered_hospital_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    tgt_hospital_mapping_df['source_id'] = tgt_hospital_mapping_df['source_id'].astype(int)
    src_patients_df = pd.merge(src_patients_df, tgt_hospital_mapping_df, left_on='registered_hospital_id', right_on='source_id', how='left')
    src_patients_df = src_patients_df.drop(columns=['registered_hospital_id', 'source_id'])
    src_patients_df = src_patients_df.rename(columns={'target_id': 'registered_hospital_id'})

#--------------------------nok_title_id----------------------------------
#Merging source patients with target titles mapping to get target nok_title_id
#check if entire column is null
if src_patients_df['nok_title_id'].isnull().all():
    src_patients_df['nok_title_id'] = None
else:
    src_patients_df['nok_title_id'] = src_patients_df['nok_title_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    tgt_title_mapping_df['source_id'] = tgt_title_mapping_df['source_id'].astype(int)
    src_patients_df = pd.merge(src_patients_df, tgt_title_mapping_df, left_on='nok_title_id', right_on='source_id', how='left')
    src_patients_df = src_patients_df.drop(columns=['nok_title_id', 'source_id'])
    src_patients_df = src_patients_df.rename(columns={'target_id': 'nok_title_id'})

#----------------------------new data insertion--------------------------------
src_patients_df1 = src_patients_df
#id genration for new data
tgt_patients_max = f'SELECT CASE WHEN MAX(id) is NULL THEN 1 ELSE MAX(id) + 1 END as max_id FROM {table_name}'
tgt_patients_max_df = pd.read_sql(tgt_patients_max, tgt_connection)
max_id = int(tgt_patients_max_df['max_id'].iloc[0])
src_patients_df1.insert(0, 'target_id', range(max_id, max_id + len(src_patients_df1)))

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
        print('Data and mapping table insert successful for patients - New insert!')
    except Exception as e:
        logging.error(f"Data and mapping table insert failed for patients - New insert! Error occurred: {e} n Query: {insert_query} \n Mapping Query: {insert_query1} \n sample row: {rows[0] if rows else 'No rows to insert'} \n sample mapping row: {mapping_rows[0] if mapping_rows else 'No mapping rows to insert'}", flush=True)
        #print("Query:", insert_query)
        #print("Sample Row:", rows[0] if rows else "No rows to insert")
        print('Data and mapping table insert failed for patients - New insert!')

# Insert new records and mapping entries simultaneously
insert_new_records_and_mapping(src_patients_df1, tgt_connection, practice_name, table_name)
