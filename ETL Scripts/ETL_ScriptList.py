from libs import *

etl_scripts_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'etl_scripts')
scripts = sorted([f for f in os.listdir(etl_scripts_dir) if f.endswith('.py') and not f.startswith('__')])

print("ETL scripts found:", scripts)