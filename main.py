import json
import os
import time
import logging
import hashlib
import tarfile
from typing import Optional, Dict, List
from dataclasses import dataclass
from pathlib import Path
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
import schedule
import humanize
from tqdm import tqdm
from datetime import datetime 
import pwd
import grp

@dataclass
class S3Config:
    """Configuration for S3 connection"""
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    prefix: str = ""

@dataclass
class FileMetadata:
    """Store file metadata including ownership and permissions"""
    path: str
    owner: str
    group: str
    mode: int
    size: int
    modified: str
    checksum: str

class ProgressTracker:
    """Tracks and logs progress for file operations"""
    
    def __init__(self, total_size: int, operation: str):
        self.progress_bar = tqdm(total=total_size, unit='B', unit_scale=True)
        self.operation = operation
        
    def update(self, chunk_size: int):
        self.progress_bar.update(chunk_size)
        
    def close(self):
        self.progress_bar.close()

class S3Client:
    """Handles S3 operations with proper error handling and logging"""
    
    def __init__(self, config: S3Config):
        self.config = config
        self.client = self._connect()
        
    def _connect(self) -> Optional[boto3.client]:
        """Establish connection to S3"""
        try:
            return boto3.client(
                's3',
                aws_access_key_id=self.config.access_key,
                aws_secret_access_key=self.config.secret_key,
                endpoint_url=self.config.endpoint
            )
        except Exception as e:
            logging.error(f"Failed to connect to S3: {str(e)}")
            return None

    def _get_file_size(self, file_path: Path) -> int:
        """Get file size in bytes"""
        return file_path.stat().st_size

    def upload_file(self, file_path: Path, object_name: str) -> bool:
        """Upload file to S3 with progress tracking and verification"""
        if not self.client:
            return False
            
        try:
            file_size = self._get_file_size(file_path)
            logging.info(f"Starting upload of {file_path} ({humanize.naturalsize(file_size)})")
            
            # Calculate initial hash
            file_hash = hashlib.sha256()
            
            # Create progress tracker
            tracker = ProgressTracker(file_size, "Upload")
            
            def upload_progress(chunk_size):
                tracker.update(chunk_size)
                
            # Upload with progress tracking
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    file_hash.update(chunk)
            
            self.client.upload_file(
                str(file_path),
                self.config.bucket,
                f"{self.config.prefix}/{object_name}",
                Callback=upload_progress
            )
            
            tracker.close()
            
            # Verify upload
            response = self.client.head_object(
                Bucket=self.config.bucket,
                Key=f"{self.config.prefix}/{object_name}"
            )
            
            uploaded_size = response['ContentLength']
            if uploaded_size != file_size:
                logging.error(f"Size mismatch for {object_name}: local={file_size}, remote={uploaded_size}")
                return False
                
            logging.info(f"Successfully uploaded {object_name}")
            logging.info(f"SHA256: {file_hash.hexdigest()}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to upload {file_path}: {str(e)}")
            return False

    def download_directory(self, s3_folder: str, local_dir: Path) -> bool:
        """Download directory from S3 with progress tracking"""
        if not self.client:
            return False
            
        try:
            # Get total size of all files
            total_size = 0
            files_to_download = []
            
            paginator = self.client.get_paginator('list_objects_v2')
            for result in paginator.paginate(
                Bucket=self.config.bucket,
                Prefix=f"{self.config.prefix}/{s3_folder}"
            ):
                for file in result.get('Contents', []):
                    total_size += file['Size']
                    files_to_download.append(file)
            
            logging.info(f"Starting download of {len(files_to_download)} files ({humanize.naturalsize(total_size)})")
            
            tracker = ProgressTracker(total_size, "Download")
            
            for file in files_to_download:
                download_path = local_dir / Path(file['Key']).name
                download_path.parent.mkdir(parents=True, exist_ok=True)
                
                def download_progress(chunk_size):
                    tracker.update(chunk_size)
                
                self.client.download_file(
                    self.config.bucket,
                    file['Key'],
                    str(download_path),
                    Callback=download_progress
                )
                
            tracker.close()
            logging.info(f"Successfully downloaded directory from S3: {s3_folder}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to download directory: {str(e)}")
            return False
@dataclass
class BackupManifest:
    """Represents a backup manifest with detailed information"""
    timestamp: str
    backup_date: str
    total_size: int
    total_compressed_size: int
    overall_compression_ratio: float
    files_count: int
    archives: List[Dict[str, any]]
    checksum: str  # SHA256 of all archive checksums concatenated

@dataclass
class ArchiveInfo:
    """Information about a single archive in the backup"""
    name: str
    original_size: int
    compressed_size: int
    compression_ratio: float
    file_count: int
    files: List[Dict[str, any]]
    checksum: str  # SHA256 of the archive

class BackupManager:
    """Manages backup and restore operations with progress tracking"""
    
    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client
        
    @staticmethod
    def _get_timestamp() -> str:
        """Generate timestamp for backup naming"""
        return time.strftime("%Y%m%d-%H%M%S")
        
    def _get_dir_size(self, path: Path) -> int:
        """Calculate total size of a directory"""
        return sum(f.stat().st_size for f in path.glob('**/*') if f.is_file())
        
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of a file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
        
    def _get_file_metadata(self, file_path: Path) -> FileMetadata:
        """Get complete file metadata including ownership and permissions"""
        stat = file_path.stat()
        try:
            owner = pwd.getpwuid(stat.st_uid).pw_name
            group = grp.getgrgid(stat.st_gid).gr_name
        except KeyError:
            owner = str(stat.st_uid)
            group = str(stat.st_gid)
            
        return FileMetadata(
            path=str(file_path),
            owner=owner,
            group=group,
            mode=stat.st_mode,
            size=stat.st_size,
            modified=datetime.fromtimestamp(stat.st_mtime).isoformat(),
            checksum=self._calculate_file_hash(file_path)
        )

    def _apply_metadata(self, file_path: Path, metadata: FileMetadata):
        """Apply stored metadata to restored file"""
        try:
            os.chmod(file_path, metadata.mode)
            
            try:
                uid = pwd.getpwnam(metadata.owner).pw_uid
                gid = grp.getgrnam(metadata.group).gr_gid
            except KeyError:
                uid = int(metadata.owner) if metadata.owner.isdigit() else -1
                gid = int(metadata.group) if metadata.group.isdigit() else -1
            
            if uid != -1 and gid != -1:
                try:
                    os.chown(file_path, uid, gid)
                except PermissionError:
                    logging.warning(f"Cannot set ownership for {file_path}")
                    
        except Exception as e:
            logging.error(f"Failed to apply metadata to {file_path}: {str(e)}")
        
    def backup_directory(self, source_dir: Path, backup_dir: Path) -> bool:
        """Create backup of a directory with progress tracking and manifest"""
        timestamp = self._get_timestamp()
        logging.info(f"Starting backup at {timestamp}")
        
        try:
            # Ensure backup directory exists and is empty
            backup_dir.mkdir(parents=True, exist_ok=True)
            for file in backup_dir.glob("*"):
                file.unlink()
                
            # Initialize manifest data
            manifest_data = {
                'timestamp': timestamp,
                'backup_date': datetime.now().isoformat(),
                'total_size': 0,
                'total_compressed_size': 0,
                'files_count': 0,
                'archives': []
            }
            
            archive_checksums = []  # For overall manifest checksum
            
            # Get total size for progress tracking
            total_size = self._get_dir_size(source_dir)
            logging.info(f"Total size to backup: {humanize.naturalsize(total_size)}")
            
            # Archive each subdirectory
            for dir_path in source_dir.glob("*"):
                if dir_path.is_dir():
                    dir_size = self._get_dir_size(dir_path)
                    archive_path = backup_dir / f"{dir_path.name}.tar.gz"
                    logging.info(f"Archiving {dir_path} ({humanize.naturalsize(dir_size)})")
                    
                    tracker = ProgressTracker(dir_size, "Compression")
                    
                    # Collect file information for manifest
                    archive_files = []
                    file_count = 0
                    
                    with tarfile.open(archive_path, "w:gz") as tar:
                        for file_path in dir_path.rglob("*"):
                            if file_path.is_file():
                                metadata = self._get_file_metadata(file_path)
                                relative_path = str(file_path.relative_to(dir_path))
                                
                                tar_info = tar.gettarinfo(str(file_path), arcname=relative_path)
                                tar_info.uid = pwd.getpwnam(metadata.owner).pw_uid if isinstance(metadata.owner, str) else int(metadata.owner)  
                                tar_info.gid = grp.getgrnam(metadata.group).gr_gid if isinstance(metadata.group, str) else int(metadata.group)
                                tar_info.mode = metadata.mode
                                
                                with open(file_path, "rb") as f:
                                    tar.addfile(tar_info, f)
                                
                                tracker.update(metadata.size)
                    
                    tracker.close()
                    
                    # Calculate archive information
                    compressed_size = archive_path.stat().st_size
                    ratio = (1 - (compressed_size / dir_size)) * 100 if dir_size > 0 else 0
                    archive_hash = self._calculate_file_hash(archive_path)
                    archive_checksums.append(archive_hash)
                    
                    # Add to manifest
                    archive_info = {
                        'name': dir_path.name,
                        'original_size': dir_size,
                        'compressed_size': compressed_size,
                        'compression_ratio': ratio,
                        'file_count': file_count,
                        'files': archive_files,
                        'checksum': archive_hash
                    }
                    manifest_data['archives'].append(archive_info)
                    manifest_data['total_size'] += dir_size
                    manifest_data['total_compressed_size'] += compressed_size
                    manifest_data['files_count'] += file_count
                    
                    logging.info(f"Compression complete: {ratio:.1f}% space saved")
                    
                    # Upload to S3
                    self.s3_client.upload_file(
                        archive_path,
                        f"{timestamp}/{archive_path.name}"
                    )
            
            # Calculate overall manifest checksum
            manifest_data['overall_compression_ratio'] = (
                (1 - (manifest_data['total_compressed_size'] / manifest_data['total_size'])) * 100
                if manifest_data['total_size'] > 0 else 0
            )
            manifest_data['checksum'] = hashlib.sha256(''.join(archive_checksums).encode()).hexdigest()
            
            # Save manifest
            manifest_path = backup_dir / 'backup_manifest.json'
            with open(manifest_path, 'w') as f:
                json.dump(manifest_data, f, indent=2)
            
            # Upload manifest to S3
            self.s3_client.upload_file(
                manifest_path,
                f"{timestamp}/backup_manifest.json"
            )
            
            # Log backup summary
            logging.info("\nBackup Summary:")
            logging.info(f"Timestamp: {manifest_data['timestamp']}")
            logging.info(f"Total original size: {humanize.naturalsize(manifest_data['total_size'])}")
            logging.info(f"Total compressed size: {humanize.naturalsize(manifest_data['total_compressed_size'])}")
            logging.info(f"Overall compression ratio: {manifest_data['overall_compression_ratio']:.1f}%")
            logging.info(f"Total files: {manifest_data['files_count']}")
            logging.info(f"Manifest checksum: {manifest_data['checksum']}")
            
            for archive in manifest_data['archives']:
                logging.info(f"\n{archive['name']}:")
                logging.info(f"  Original size: {humanize.naturalsize(archive['original_size'])}")
                logging.info(f"  Compressed size: {humanize.naturalsize(archive['compressed_size'])}")
                logging.info(f"  Compression ratio: {archive['compression_ratio']:.1f}%")
                logging.info(f"  Files: {archive['file_count']}")
                logging.info(f"  Checksum: {archive['checksum']}")
            
            logging.info(f"\nBackup completed at {self._get_timestamp()}")
            return True
            
        except Exception as e:
            logging.error(f"Backup failed: {str(e)}")
            return False
            
    def restore_directory(self, restore_dir: Path, output_dir: Path) -> bool:
        """Restore from backup with progress tracking"""
        try:
            # Create output directory
            output_dir.mkdir(parents=True, exist_ok=True)
            
            total_progress = 0
            archives = list(restore_dir.glob("*.tar.gz"))
            
            # Calculate total size first
            total_size = 0
            for archive in archives:
                with tarfile.open(archive, "r:gz") as tar:
                    total_size += sum(member.size for member in tar.getmembers() if member.isfile())
            
            logging.info(f"Total size to restore: {humanize.naturalsize(total_size)}")
            tracker = ProgressTracker(total_size, "Extraction")
            
            restored_dirs = set()
            
            for archive in archives:
                # Create a directory for this archive based on archive name without .tar.gz
                archive_name = archive.stem.replace('.tar', '')  # Remove both .tar and .gz
                extract_dir = output_dir / archive_name
                
                archive_size = archive.stat().st_size
                logging.info(f"Restoring {archive_name} ({humanize.naturalsize(archive_size)})")
                
                # Create or clean the extraction directory
                if extract_dir.exists():
                    import shutil
                    shutil.rmtree(extract_dir)
                extract_dir.mkdir(parents=True)
                
                with tarfile.open(archive, "r:gz") as tar:
                    members = tar.getmembers()
                    for member in members:
                        tar.extract(member, path=extract_dir)
                        
                        if member.isfile():
                            file_path = extract_dir / member.name
                            metadata = FileMetadata(
                                path=str(file_path),
                                owner=pwd.getpwuid(member.uid).pw_name if member.uid >= 0 else str(member.uid),
                                group=grp.getgrgid(member.gid).gr_name if member.gid >= 0 else str(member.gid),
                                mode=member.mode,
                                size=member.size,
                                modified=datetime.fromtimestamp(member.mtime).isoformat(),
                                checksum=""
                            )
                            self._apply_metadata(file_path, metadata)
                            tracker.update(member.size)
                            
                restored_dirs.add(extract_dir)
            
            tracker.close()
            
            # Log restore summary
            logging.info("\nRestore Summary:")
            logging.info(f"Total archives restored: {len(restored_dirs)}")
            logging.info(f"Total size restored: {humanize.naturalsize(total_size)}")
            for dir_path in sorted(restored_dirs):
                dir_size = self._get_dir_size(dir_path)
                logging.info(f"  {dir_path.name}: {humanize.naturalsize(dir_size)}")
            
            logging.info(f"Restore completed at {self._get_timestamp()}")
            return True
            
        except Exception as e:
            logging.error(f"Restore failed: {str(e)}")
            return False

def setup_logging(log_dir: Path) -> None:
    """Configure detailed logging with both file and console handlers"""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "backup.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

# Main function remains the same
def main():
    # Load environment variables
    load_dotenv(".env", override=True)
    
    # Initialize paths
    backup_dir = Path(os.getenv("BACKUP_DIR", ""))
    output_dir = Path(os.getenv("OUTPUT_DIR", ""))
    
    if not backup_dir or not output_dir:
        raise ValueError("BACKUP_DIR and OUTPUT_DIR must be specified in .env")
    
    # Set up logging
    setup_logging(output_dir)
    logging.info("Starting backup service")
    logging.info(f"Backup directory: {backup_dir}")
    logging.info(f"Output directory: {output_dir}")
    
    # Initialize S3 configuration
    s3_config = S3Config(
        endpoint=os.getenv("S3_ENDPOINT", ""),
        access_key=os.getenv("S3_ACCESS_KEY", ""),
        secret_key=os.getenv("S3_SECRET_KEY", ""),
        bucket=os.getenv("S3_BUCKET", ""),
        prefix=os.getenv("S3_PREFIX", "")
    )
    
    # Validate S3 configuration
    if not all([s3_config.endpoint, s3_config.access_key, s3_config.secret_key, s3_config.bucket]):
        raise ValueError("Missing required S3 configuration in .env")
    
    # Initialize clients
    s3_client = S3Client(s3_config)
    if not s3_client.client:
        raise RuntimeError("Failed to initialize S3 client")
        
    backup_manager = BackupManager(s3_client)
    
    mode = os.getenv("MODE", "backup")
    logging.info(f"Operating in {mode} mode")
    
    if mode == "backup":
        interval = int(os.getenv("BACKUP_INTERVAL_SECONDS", "3600"))
        retention_days = int(os.getenv("BACKUP_RETENTION_DAYS", "30"))
        
        logging.info(f"Backup interval: {interval} seconds")
        logging.info(f"Backup retention: {retention_days} days")
        
        def scheduled_backup():
            try:
                start_time = time.time()
                logging.info("Starting scheduled backup")
                
                # Perform backup
                success = backup_manager.backup_directory(backup_dir, output_dir)
                
                # Log completion status and duration
                duration = time.time() - start_time
                if success:
                    logging.info(f"Scheduled backup completed successfully in {duration:.2f} seconds")
                else:
                    logging.error(f"Scheduled backup failed after {duration:.2f} seconds")
                
                # Clean old backups if retention is set
                if retention_days > 0:
                    cleanup_old_backups(s3_client, retention_days)
                    
            except Exception as e:
                logging.error(f"Error during scheduled backup: {str(e)}")
        
        # Perform initial backup
        scheduled_backup()
        
        # Schedule recurring backups
        schedule.every(interval).seconds.do(scheduled_backup)
        
        # Run scheduler
        logging.info("Entering scheduler loop")
        while True:
            try:
                schedule.run_pending()
                time.sleep(1)
            except KeyboardInterrupt:
                logging.info("Backup service stopped by user")
                break
            except Exception as e:
                logging.error(f"Error in scheduler loop: {str(e)}")
                time.sleep(60)  # Wait before retrying
            
    elif mode == "restore":
        restore_dir = Path(os.getenv("RESTORE_DIR", ""))
        if not restore_dir:
            raise ValueError("RESTORE_DIR not specified in .env")
            
        logging.info(f"Starting restore from {restore_dir}")
        
        try:
            start_time = time.time()
            
            # Download from S3
            logging.info("Downloading backup files from S3")
            if not s3_client.download_directory(restore_dir, output_dir):
                raise RuntimeError("Failed to download backup files from S3")
            
            # Restore from downloaded files
            logging.info("Restoring from backup files")
            if not backup_manager.restore_directory(output_dir, backup_dir):
                raise RuntimeError("Failed to restore from backup files")
            
            duration = time.time() - start_time
            logging.info(f"Restore completed successfully in {duration:.2f} seconds")
            
        except Exception as e:
            logging.error(f"Restore failed: {str(e)}")
            raise

def cleanup_old_backups(s3_client: S3Client, retention_days: int):
    """Clean up backups older than retention_days"""
    try:
        current_time = time.time()
        cutoff_time = current_time - (retention_days * 24 * 60 * 60)
        
        paginator = s3_client.client.get_paginator('list_objects_v2')
        objects_to_delete = []
        
        # Find old backups
        for result in paginator.paginate(
            Bucket=s3_client.config.bucket,
            Prefix=s3_client.config.prefix
        ):
            for obj in result.get('Contents', []):
                if obj['LastModified'].timestamp() < cutoff_time:
                    objects_to_delete.append({'Key': obj['Key']})
        
        if objects_to_delete:
            # Delete old backups in batches
            batch_size = 1000
            for i in range(0, len(objects_to_delete), batch_size):
                batch = objects_to_delete[i:i + batch_size]
                s3_client.client.delete_objects(
                    Bucket=s3_client.config.bucket,
                    Delete={'Objects': batch}
                )
            
            logging.info(f"Cleaned up {len(objects_to_delete)} old backup files")
        else:
            logging.info("No old backups to clean up")
            
    except Exception as e:
        logging.error(f"Failed to clean up old backups: {str(e)}")

if __name__ == "__main__":
    main()