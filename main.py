from utils import *
from botocore.exceptions import NoCredentialsError
import os
from dotenv import load_dotenv
import os
import time
import schedule
import logging

# Load the .env file
load_dotenv(".env")

# main
if __name__ == "__main__":
    # # Load configuration from .env file
    mode = os.getenv("MODE")
    backup_dir = os.getenv("BACKUP_DIR")
    output_dir = os.getenv("OUTPUT_DIR")
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
        S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT")
        S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
        S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
        S3_BUCKET = os.getenv("S3_BUCKET")
        S3_PREFIX = os.getenv("S3_PREFIX")
        RESTORE_DIR = os.getenv("RESTORE_DIR")
        if (not S3_ENDPOINT_URL or not S3_ACCESS_KEY or not S3_SECRET_KEY or not S3_BUCKET):
            logging.error("S3 credentials not found")
        s3 = connect_s3(S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY)
        download_dir_from_s3(
            s3, S3_BUCKET, RESTORE_DIR, output_dir)
        restore_directory(output_dir, backup_dir)
        logging.info(f"Restore finished at {get_timestamp()}")
