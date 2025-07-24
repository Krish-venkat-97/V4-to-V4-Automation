import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_src_myconnection, get_tgt_myconnection,get_patient_records,getPractice

#setup_logging(os.path.splitext(os.path.basename(__file__))[0])

import warnings
warnings.filterwarnings("ignore")

query = f"""
CREATE TABLE IF NOT EXISTS mapping_table (
    source VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    target_id VARCHAR(255) NOT NULL,
    new_or_exist VARCHAR(50) NOT NULL
    );
"""

tgt_connection = get_tgt_myconnection()
tgt_cursor = tgt_connection.cursor()
tgt_cursor.execute(query)
tgt_connection.commit()
print("Mapping table created successfully.",flush=True)