import os
import time
import logging
import hashlib
import tarfile
import boto3
from botocore.exceptions import NoCredentialsError

def connect_s3_from_env():
    endpoint, access_key, secret_key = os.getenv("S3_ENDPOINT"), os.getenv("S3_ACCESS_KEY"), os.getenv("S3_SECRET_KEY")
    if not all([endpoint, access_key, secret_key]):
        logging.error("S3 credentials missing")
        return None
    return boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, endpoint_url=endpoint)


def backup_directory(parent_dir, output_dir):
    timestamp = get_time()
    logging.info(f"Backup started at {timestamp}")
    os.makedirs(output_dir, exist_ok=True)

    # Clear output directory
    for file_name in os.listdir(output_dir):
        try:
            os.remove(os.path.join(output_dir, file_name))
        except Exception as e:
            logging.error(e)

    # Archive subdirectories
    for dir_name in os.listdir(parent_dir):
        dir_path = os.path.join(parent_dir, dir_name)
        if os.path.isdir(dir_path):
            logging.info(f"Archiving {dir_path}")
            with tarfile.open(os.path.join(output_dir, f"{dir_name}.tar.gz"), "w:gz") as tar:
                tar.add(dir_path, arcname=dir_name)

    # Upload to S3
    s3 = connect_s3_from_env()
    if not s3:
        return
    for file_name in os.listdir(output_dir):
        file_path = os.path.join(output_dir, file_name)
        logging.info(f"Uploading {file_path} to S3")
        upload_to_s3(s3, file_path, os.getenv("S3_BUCKET"), f"{os.getenv('S3_PREFIX')}/{timestamp}/{file_name}")

    logging.info(f"Backup finished at {time.strftime('%Y%m%d-%H%M%S')}")


def restore_directory(restore_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    for file_name in os.listdir(restore_dir):
        file_path = os.path.join(restore_dir, file_name)
        if file_name.endswith(".tar.gz"):
            logging.info(f"Restoring {file_path}")
            with tarfile.open(file_path, "r:gz") as tar:
                logging.info(f"Extract {file_path} to {output_dir}")
                tar.extractall(path=output_dir)




def upload_to_s3(s3, file_name, bucket, object_name=None):
    object_name = object_name or file_name
    try:
        s3.upload_file(file_name, bucket, object_name)
        file_hash = hashlib.sha256()
        with open(file_name, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                file_hash.update(chunk)
        logging.info(f"Uploaded {file_name}, SHA256: {file_hash.hexdigest()}")
        return True
    except FileNotFoundError:
        logging.error(f"File {file_name} not found")
    except NoCredentialsError:
        logging.error("No credentials available")
    return False


def download_from_s3(s3, bucket, object_name, file_name=None):
    file_name = file_name or object_name
    try:
        s3.download_file(bucket, object_name, file_name)
        logging.info(f"Downloaded {file_name} from {bucket}")
        return True
    except NoCredentialsError:
        logging.error("No credentials available")
    return False


def download_dir_from_s3(s3, bucket, s3_folder, local_dir=None):
    local_dir = local_dir or s3_folder
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for result in paginator.paginate(Bucket=bucket, Prefix=s3_folder):
            for file in result.get('Contents', []):
                download_path = os.path.join(local_dir, os.path.basename(file['Key']))
                os.makedirs(os.path.dirname(download_path), exist_ok=True)
                s3.download_file(bucket, file['Key'], download_path)
        logging.info("Directory download complete")
        return True
    except NoCredentialsError:
        logging.error("No credentials available")
    return False

def get_time():
   return time.strftime("%Y%m%d-%H%M%S")