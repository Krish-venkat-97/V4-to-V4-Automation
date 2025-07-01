from libs import *
from src.utils import get_src_myconnection, get_tgt_myconnection,get_patient_records,getPractice,getSourceFilePath,getTargetFilePath
import warnings

import warnings
warnings.filterwarnings("ignore")

src_connection = get_src_myconnection()
tgt_connection = get_tgt_myconnection()
tgt_cursor = tgt_connection.cursor()
patient_ids = get_patient_records()
practice_name = getPractice()
source_file_path = getSourceFilePath()
target_file_path = getTargetFilePath()
table_name = 'scan_documents'

src_file_path = f'{source_file_path}'
tgt_file_path = f'{target_file_path}'

landing_scan = """
SELECT a1.*,b.source_id AS source_patient_id
FROM (
SELECT l.patient_id AS target_patient_id,a.source_id AS source_scan_id,a.target_id AS target_scan_id
FROM scan_documents l
INNER JOIN (SELECT * FROM mapping_table WHERE TABLE_NAME = 'scan_documents')a
ON l.id = a.target_id
)a1
INNER JOIN (SELECT * FROM mapping_table WHERE TABLE_NAME = 'patients')b
ON a1.target_patient_id = b.target_id
"""
landing_scan_df = pd.read_sql(landing_scan, tgt_connection)

def getSourceFilePath(row):
    file_path = os.path.join(src_file_path, str(row['source_patient_id']),'scans',str(row['source_scan_id']) + '.pdf')
    return file_path

landing_scan_df['source_file_path']= landing_scan_df.apply(getSourceFilePath, axis=1)

def getTargetFilePath(row):
    file_directory = os.path.join(tgt_file_path, str(row['target_patient_id']), 'scans', 'verified')
    file_path = os.path.join(file_directory, str(row['target_scan_id']) + '.pdf')
    return pd.Series([file_directory, file_path])

landing_scan_df[['target_file_directory', 'target_file_path']] = landing_scan_df.apply(getTargetFilePath, axis=1)

landing_scan_df1 = landing_scan_df[['source_file_path','target_file_directory','target_file_path']]

def fileCheck(row):
    if os.path.exists(row['source_file_path']):
        return 1
    else:
        return 0
landing_scan_df1['file_check'] = landing_scan_df1.apply(fileCheck, axis=1)

landing_scan_df2 = landing_scan_df1[landing_scan_df1['file_check'] == 1]

bar = tqdm(total=len(landing_scan_df2), desc='Copying files')

for index,row in landing_scan_df2.iterrows():
    bar.update(1)
    try:
        is_file_dffolder_exist = os.path.exists(row['target_file_directory'])
        if not is_file_dffolder_exist:
            os.makedirs(row['target_file_directory'])
            is_file_exist = os.path.exists(row['target_file_path'])
            if not is_file_exist:
                shutil.copy(row['source_file_path'],row['target_file_path'])
            else:
                pass
        else:
            is_file_exist = os.path.exists(row['target_file_path'])
            if not is_file_exist:
                shutil.copy(row['source_file_path'],row['target_file_path'])
            else:
                pass
    except:
        logging.error(f"Error copying file from {row['source_file_path']} to {row['target_file_path']}")
        break

print('Letter migration completed successfully!')
bar.close()


