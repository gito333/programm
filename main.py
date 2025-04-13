import os
import sys
import subprocess
import logging
import datetime

# --- Configuration ---
JSONL_OUTPUT_FOLDER = "jsonl_output" # Define the JSONL output folder name
RESULTS_FOLDER = "results"          # Define the results folder name
SCRAPER_SCRIPT = "scraper_makro.py"
MERGE_SCRIPT = "merge_jsonl.py"
CSV_CONVERTER_SCRIPT = "json_to_csv.py"
# --- End Configuration ---

# Configure logging
log_file = f"main_run_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout) # Also print logs to console
    ]
)
logger = logging.getLogger(__name__)

def create_folders():
    """Creates necessary output folders if they don't exist."""
    try:
        os.makedirs(JSONL_OUTPUT_FOLDER, exist_ok=True)
        logger.info(f"Checked/Created folder: {JSONL_OUTPUT_FOLDER}")
        os.makedirs(RESULTS_FOLDER, exist_ok=True)
        logger.info(f"Checked/Created folder: {RESULTS_FOLDER}")
    except OSError as e:
        logger.error(f"Error creating folders: {e}")
        sys.exit(1) # Exit if folders can't be created

def run_script(script_name):
    """Runs a given Python script as a subprocess."""
    try:
        logger.info(f"--- Running script: {script_name} ---")
        process = subprocess.run(
            [sys.executable, script_name],
            check=True, # Raise an exception if the script fails (non-zero exit code)
            capture_output=True, # Capture stdout/stderr
            text=True, # Decode stdout/stderr as text
            encoding='utf-8' # Specify encoding
        )
        logger.info(f"Output from {script_name}:\n{process.stdout}")
        if process.stderr:
             logger.warning(f"Stderr from {script_name}:\n{process.stderr}")
        logger.info(f"--- Finished script: {script_name} ---")
        return True
    except FileNotFoundError:
        logger.error(f"Error: Script '{script_name}' not found.")
        return False
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running script '{script_name}'. Exit code: {e.returncode}")
        logger.error(f"Stderr from {script_name}:\n{e.stderr}")
        logger.error(f"Stdout from {script_name}:\n{e.stdout}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred while running {script_name}: {e}")
        return False

if __name__ == "__main__":
    logger.info("=== Starting Main Orchestration Script ===")

    create_folders()

    # Step 1: Run the scraper
    if not run_script(SCRAPER_SCRIPT):
        logger.error("Scraper script failed. Aborting.")
        sys.exit(1)

    # Step 2: Merge JSONL files
    if not run_script(MERGE_SCRIPT):
        logger.error("Merging script failed. Aborting.")
        sys.exit(1)

    # Step 3: Convert merged JSONL to CSV
    if not run_script(CSV_CONVERTER_SCRIPT):
        logger.error("CSV conversion script failed.")
        sys.exit(1) # Still exit, but maybe less critical than previous steps failing

    logger.info("=== Main Orchestration Script Finished Successfully ===")
    logger.info(f"Individual JSONL files are in: ./{JSONL_OUTPUT_FOLDER}")
    logger.info(f"Merged JSONL and CSV files are in: ./{RESULTS_FOLDER}")
    logger.info(f"Detailed log for this run: {log_file}")
