import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
from src.config import config

def getSourceExcel():
    return config['source_excel']['source_excel']

def get_src_myconnection():
    try:
        return pymysql.connect( 
            host= config['source_mysql']['host'], 
            user= config['source_mysql']['user'], 
            db= config['source_mysql']['db'],
            password= config['source_mysql']['password'])
        print('Connection successful')
    except Exception as e:
        print(f"Error: {e}")
        return None
    
src_connection = get_src_myconnection()

def get_tgt_myconnection():
    try:
        return pymysql.connect( 
            host= config['target_mysql']['host'], 
            user= config['target_mysql']['user'], 
            db= config['target_mysql']['db'],
            password= config['target_mysql']['password'])
        print('Connection successful')
    except Exception as e:
        print(f"Error: {e}")
        return None
    
tgt_connection = get_tgt_myconnection()

def safe_value(value):
    if value is None or pd.isnull(value):
        return 'NULL'
    elif isinstance(value,str):
        if '"' in value:
            return '"' + value.replace('"',"'") + '"'
        elif "\\" in value:
            return '"' + value.replace("\\","\\\\") + '"'
        else:
            return '"' + value + '"'
    elif isinstance(value,date):
        return '"' + value.strftime('%Y-%m-%d') + '"'
    else:
        return value


if __name__ == "__main__":
    connection1 = get_src_myconnection()
    if connection1:
        print("Connection to source successful!")
        connection1.close()
    else:
        print("Connection to source failed!")
    connection2 = get_tgt_myconnection()
    if connection2:
        print("Connection to targetsuccessful!")
        connection2.close()
    else:
        print("Connection to target failed!")

def getSourceExcel():
    return config['source_excel']['source_excel']


def getPractice():
    return config['practice']['practice']

def get_patient_records():
    #src_excel = pd.read_excel(getSourceExcel())
    src_excel = pd.read_sql('SELECT id as PID FROM patients', src_connection)
    patient_df = src_excel[['PID']].dropna()  # Drop rows with NaN in the PID column
    patient_df['PID'] = patient_df['PID'].apply(lambda x: str(x).replace('.0', '') if '.0' in str(x) else str(x))
    patient_df['PID'] = patient_df['PID'].astype(int)
    patient_df = patient_df.drop_duplicates()  # Convert to integer
    patient_ids = tuple(patient_df['PID'].tolist())
    return patient_ids

def getSourceFilePath():
    return config['source_file_path']['source_file_path']

def getTargetFilePath():
    return config['target_file_path']['target_file_path']

def getLogFilePath():
    return config['log_directory']['log_directory']

def getMergeFilePath():
    return config['merge_file_path']['merge_file_path']
