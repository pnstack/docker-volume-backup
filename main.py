from utils import *
import os
from dotenv import load_dotenv
import os
import logging
import schedule

# Load the .env file
load_dotenv(".env" ,override=True)

def main():
    # # Load configuration from .env file
    mode = os.getenv("MODE", 'backup') 
    backup_dir = os.getenv("BACKUP_DIR")
    output_dir = os.getenv("OUTPUT_DIR")

    print("backup_drr", backup_dir)

    # logging.info("backup: {backup_dir}")

    # Set up logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(os.path.join(output_dir, 'backup.log')), logging.StreamHandler()])

    if mode == "backup":
        SECOND_INTERVAL = os.getenv("SECOND_INTERVAL")

        backup_directory(backup_dir, output_dir)
        schedule.every(int(SECOND_INTERVAL)).seconds.do(
            backup_directory, backup_dir, output_dir)
        while True:
            schedule.run_pending()
            time.sleep(1)

    if mode == "restore":
        logging.info("Restore mode ")
        s3 = connect_s3_from_env()
        S3_BUCKET = os.getenv("S3_BUCKET")
        RESTORE_DIR = os.getenv("RESTORE_DIR")
        if (not RESTORE_DIR):
             logging.error("Restore dir not found")
             return
        download_dir_from_s3(
            s3, S3_BUCKET, RESTORE_DIR, output_dir)
        restore_directory(output_dir, backup_dir)
        logging.info(f"Restore finished at {get_time()}")

# main
if __name__ == "__main__":
    main()
