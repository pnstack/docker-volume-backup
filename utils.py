
from botocore.exceptions import NoCredentialsError
import os
import tarfile
import boto3
from dotenv import load_dotenv
import os
import time
import schedule
import logging
import hashlib


def backup_directory(parent_dir, output_dir):
    """
    Archive each subdirectory of a parent directory to a .tar.gz file.

    Parameters:
    parent_dir (str): The parent directory containing the subdirectories to archive.
    output_dir (str): The directory to save the .tar.gz files to.
    """
    # Create the output directory if it doesn't exist
    timestamp = get_timestamp()
    logging.info(f"Backup started at {timestamp}")
    os.makedirs(output_dir, exist_ok=True)
    # clear the output directory
    for file_name in os.listdir(output_dir):
        file_path = os.path.join(output_dir, file_name)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
        except Exception as e:
            logging.error(e)

    for dir_name in os.listdir(parent_dir):
        dir_path = os.path.join(parent_dir, dir_name)
        # Check if it's a directory
        if os.path.isdir(dir_path):
            logging.info(f"Archiving {dir_path}")
            with tarfile.open(os.path.join(output_dir, f"{dir_name}.tar.gz"), "w:gz") as tar:
                tar.add(dir_path, arcname=dir_name)

    S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT")
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
    S3_BUCKET = os.getenv("S3_BUCKET")
    S3_PREFIX = os.getenv("S3_PREFIX")
    if (not S3_ENDPOINT_URL or not S3_ACCESS_KEY or not S3_SECRET_KEY or not S3_BUCKET):
        logging.error("S3 credentials not found")
        return
    s3 = connect_s3(S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY)
    for file_name in os.listdir(output_dir):
        file_path = os.path.join(output_dir, file_name)
        print(f"Uploading {file_path} to S3 bucket {S3_BUCKET}")
        upload_to_s3(s3, file_path, S3_BUCKET,
                     S3_PREFIX+'/'+timestamp + '/'+file_name)
    logging.info(f"Backup finished at {get_timestamp()}")
    
def restore_directory(restore_dir, output_dir):
    """
    Restore the contents of .tar.gz files in a directory to a specified directory.

    Parameters:
    restore_dir (str): The directory containing the .tar.gz files to restore.
    output_dir (str): The directory to restore the contents to.
    """
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Restore the contents of each .tar.gz file
    for file_name in os.listdir(restore_dir):
        file_path = os.path.join(restore_dir, file_name)
        if file_name.endswith(".tar.gz"):
            logging.info(f"Restoring {file_path}")
            restore_tar_gz(file_path, output_dir)


def restore_tar_gz(tar_path, restore_dir):
    """
    Restore the contents of a .tar.gz file to a specified directory.

    Parameters:
    tar_path (str): The path to the .tar.gz file.
    restore_dir (str): The directory to restore the contents to.
    """
    # Create the restore directory if it doesn't exist
    os.makedirs(restore_dir, exist_ok=True)

    # Open the .tar.gz file
    with tarfile.open(tar_path, "r:gz") as tar:
        # Extract all files to the restore directory
        tar.extractall(path=restore_dir)


def connect_s3(endpoint_url, access_key, secret_key):
    """
    Connect to AWS S3.

    Parameters:
    access_key (str): Your AWS access key.
    secret_key (str): Your AWS secret key.

    Returns:
    s3: The boto3 S3 client.
    """
    s3 = boto3.client('s3', aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key, endpoint_url=endpoint_url)
    return s3

def connect_s3_from_env():
    """
    Connect to AWS S3 using credentials from environment variables.

    Returns:
    s3: The boto3 S3 client.
    """
    S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT")
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
    if (not S3_ENDPOINT_URL or not S3_ACCESS_KEY or not S3_SECRET_KEY):
        logging.error("S3 credentials not found")
        return
    s3 = boto3.client('s3', aws_access_key_id=S3_ACCESS_KEY,
                      aws_secret_access_key=S3_SECRET_KEY, endpoint_url=S3_ENDPOINT_URL)
    return s3

def upload_to_s3(s3, file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket.

    Parameters:
    s3: The boto3 S3 client.
    file_name (str): The file to upload.
    bucket (str): The S3 bucket to upload to.
    object_name (str): The name of the object in the S3 bucket. If not specified, file_name is used.
    """
    if object_name is None:
        object_name = file_name

    try:
        s3.upload_file(file_name, bucket, object_name)
        # calc file sha256 then save to log
        file_hash = hashlib.sha256()
        with open(file_name, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                file_hash.update(byte_block)
        logging.info(f"File {file_name} SHA256: {file_hash.hexdigest()}")

        logging.info(
            f"Upload {file_name} Successful with object name {object_name} to bucket {bucket}")
        return True
    except FileNotFoundError:
        logging.error(f"The file {file_name} was not found")
        return False
    except NoCredentialsError:
        logging.error("Credentials not available")
        return False


def download_from_s3(s3, bucket, object_name, file_name=None):
    """
    Download a file from an S3 bucket.

    Parameters:
    s3: The boto3 S3 client.
    bucket (str): The S3 bucket to download from.
    object_name (str): The name of the object in the S3 bucket.
    file_name (str): The file to download to. If not specified, object_name is used.
    """
    if file_name is None:
        file_name = object_name

    try:
        s3.download_file(bucket, object_name, file_name)
        print("Download Successful")
        return True
    except NoCredentialsError:
        print("Credentials not available")
        return False


def download_dir_from_s3(s3, bucket, s3_folder, local_dir=None):
    """
    Download a directory from an S3 bucket.

    Parameters:
    s3: The boto3 S3 client.
    bucket (str): The S3 bucket to download from.
    s3_folder (str): The name of the folder in the S3 bucket.
    local_dir (str): The local directory to download to. If not specified, s3_folder is used.
    """
    if local_dir is None:
        local_dir = s3_folder

    try:
        paginator = s3.get_paginator('list_objects_v2')
        for result in paginator.paginate(Bucket=bucket, Prefix=s3_folder):
            for file in result.get('Contents', []):
                download_path = os.path.join(
                    local_dir, os.path.basename(file['Key']))
                if not os.path.exists(os.path.dirname(download_path)):
                    os.makedirs(os.path.dirname(download_path))
                s3.download_file(bucket, file['Key'], download_path)
        logging.info("Download Successful")
        return True
    except NoCredentialsError:
        logging.error("Credentials not available")
        return False


def get_timestamp():
    return time.strftime("%Y%m%d-%H%M%S")
