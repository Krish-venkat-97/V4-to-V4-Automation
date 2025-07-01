# Main script to orchestrate ETL processes
from libs import *
from src.utils import get_src_myconnection, get_tgt_myconnection,get_patient_records,getPractice,getLogFilePath
import warnings


warnings.filterwarnings("ignore")

# Function to set up logging for each script
def setup_script_logging(script_name):
    #a = str(script_name).replace(".py","")
    a = str(script_name).replace("", "").replace(".py", "")
    log_dir = getLogFilePath()
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{a}.log")

    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


checkpoint_file = os.path.join(getLogFilePath(), "checkpoint.txt")

#list of scripts to run
scripts = [
    '0)mappingtable.py', '1)insuranceCompanies.py', '2)insurancePlans.py', '3)hospitals.py',
    '4)Contact_types.py', '5)titles.py', '6)contacts.py', '7)patientTypes.py',
    '8)patients.py', '9)patientHistories.py', '10)medicalHistories.py', '11)episodes.py',
    '12)patientContactDetails.py', '13)letterCategories.py', '14)scanCategories.py',
    '15)letters.py', '16)scanDocuments.py', '17)AppointmentDescription.py',
    '18)procedures.py',
    '20)appointments.py', '21)surgeries.py', '22)prescriptions.py', '23)taxes.py',
    '24)taxDetails.py', '25)invoices.py', '26)invoiceDetails.py', '27)receipts.py',
    '28)receipt_details.py','29)letterMigration.py','30)scanMigration.py','31)scanMigration2.py'
]

# Function to get the last completed script
def get_last_completed_script():
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            last_script = f.read().strip()
            if last_script in scripts:
                return scripts.index(last_script) + 1  # Start from next script
    return 0  # Start from the beginning

# Function to save the last successfully completed script
def save_checkpoint(script_name):
    with open(checkpoint_file, "w") as f:
        f.write(script_name)

# Start execution from the last successful script
start_index = get_last_completed_script()

print(f"Starting from script index: {start_index}")
print(f"Total number of scripts: {len(scripts)}")

if start_index >= len(scripts):
    print("Error: Start index is out of range.")
    logging.error("Start index is out of range.")
else:
    for script in scripts[start_index:]:
        # Set up logging for the current script only if an error occurs
        try:
            #logging.info(f"Starting execution: {script}")
            # Remove the old log file before running the script
            log_file = os.path.join(getLogFilePath(), f"{os.path.splitext(script)[0]}.log")
            if os.path.exists(log_file):
                os.remove(log_file)
            print(f"Running {script}...")

            # Run the script and ensure the progress bar is displayed
            script_path = os.path.join("ETL Scripts", script)
            result = subprocess.run(["python", script_path], capture_output=True, text=True, check=True, encoding='utf-8')

            # Log standard output
            #logging.info(f"Output of {script}: {result.stdout}")
            print(f"Output of {script}: {result.stdout}")

            # Save checkpoint
            save_checkpoint(script)

        except subprocess.CalledProcessError as e:
            setup_script_logging(script)
            logging.error(f"Error in {script}: {e.stderr}")
            print(f"❌ {script} failed! Check {script}.log for details.")
            break  # Stop execution on failure

print("✅ Pipeline execution complete. Check the individual log files for details.")