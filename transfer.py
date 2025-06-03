#!/usr/bin/env python3
"""
Script to copy files from AWS S3 to Google Cloud Storage
Usage: python s3_to_gcs_copy.py
"""

import os
import sys
import logging
import tempfile
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple
import argparse
from datetime import datetime

try:
    import boto3
    from botocore.exceptions import NoCredentialsError, ClientError
except ImportError:
    print("Error: boto3 not installed. Install with: pip install boto3")
    sys.exit(1)

try:
    from google.cloud import storage
    from google.api_core import exceptions as gcs_exceptions
except ImportError:
    print("Error: google-cloud-storage not installed. Install with: pip install google-cloud-storage")
    sys.exit(1)

# Configuration
S3_BUCKET = "anyscale-imagenet"
S3_PREFIX = "ILSVRC/Data/CLS-LOC/test/"
GCS_BUCKET = "anyscale-imagenet"
GCS_PREFIX = "ILSVRC/Data/CLS-LOC/test/"
MAX_WORKERS = 10  # Number of concurrent transfers

class S3ToGCSTransfer:
    def __init__(self, s3_bucket: str, s3_prefix: str, gcs_bucket: str, gcs_prefix: str):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.rstrip('/') + '/'
        self.gcs_bucket = gcs_bucket
        self.gcs_prefix = gcs_prefix.rstrip('/') + '/'

        # Setup logging
        self.setup_logging()
        
        # Initialize clients
        self.s3_client = None
        self.gcs_client = None
        self.gcs_bucket_obj = None
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler('s3_to_gcs_transfer.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def initialize_clients(self):
        """Initialize S3 and GCS clients"""
        try:
            # Initialize S3 client
            self.logger.info("Initializing S3 client...")
            self.s3_client = boto3.client('s3')
            
            # Test S3 credentials
            self.s3_client.head_bucket(Bucket=self.s3_bucket)
            self.logger.info("✓ S3 credentials verified")
            
        except NoCredentialsError:
            self.logger.error("AWS credentials not found. Configure with 'aws configure' or set environment variables")
            return False
        except ClientError as e:
            self.logger.error(f"S3 error: {e}")
            return False
            
        try:
            # Initialize GCS client
            self.logger.info("Initializing GCS client...")
            self.gcs_client = storage.Client()
            self.gcs_bucket_obj = self.gcs_client.bucket(self.gcs_bucket)
            
            # Test GCS credentials
            self.gcs_bucket_obj.reload()
            self.logger.info("✓ GCS credentials verified")
            
        except Exception as e:
            self.logger.error(f"GCS error: {e}")
            self.logger.error("Make sure to authenticate with 'gcloud auth application-default login'")
            return False
            
        return True
        
    def list_s3_objects(self) -> List[str]:
        """List all objects in S3 bucket with the given prefix"""
        self.logger.info(f"Listing objects in s3://{self.s3_bucket}/{self.s3_prefix}")
        
        objects = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        try:
            for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Skip directories (objects ending with '/')
                        if not obj['Key'].endswith('/'):
                            objects.append(obj['Key'])
                            
        except ClientError as e:
            self.logger.error(f"Error listing S3 objects: {e}")
            return []
            
        self.logger.info(f"Found {len(objects)} objects in S3")
        return objects
        
    def transfer_object_direct(self, s3_key: str) -> Tuple[bool, str]:
        """Transfer a single object directly from S3 to GCS"""
        try:
            # Generate GCS key by replacing S3 prefix with GCS prefix
            gcs_key = s3_key.replace(self.s3_prefix, self.gcs_prefix, 1)
            
            # Get object from S3
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            data = response['Body'].read()
            
            # Upload to GCS
            blob = self.gcs_bucket_obj.blob(gcs_key)
            blob.upload_from_string(data)
            
            return True, f"✓ {s3_key} -> {gcs_key}"
            
        except Exception as e:
            return False, f"✗ Failed to transfer {s3_key}: {str(e)}"
            
    def transfer_object_via_file(self, s3_key: str, temp_dir: str) -> Tuple[bool, str]:
        """Transfer a single object via temporary file"""
        try:
            # Generate local file path
            relative_path = s3_key.replace(self.s3_prefix, '', 1)
            local_path = os.path.join(temp_dir, relative_path)
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download from S3
            self.s3_client.download_file(self.s3_bucket, s3_key, local_path)
            
            # Generate GCS key
            gcs_key = s3_key.replace(self.s3_prefix, self.gcs_prefix, 1)
            
            # Upload to GCS
            blob = self.gcs_bucket_obj.blob(gcs_key)
            blob.upload_from_filename(local_path)
            
            # Clean up local file
            os.remove(local_path)
            
            return True, f"✓ {s3_key} -> {gcs_key}"
            
        except Exception as e:
            return False, f"✗ Failed to transfer {s3_key}: {str(e)}"
            
    def transfer_files_direct(self, s3_objects: List[str]) -> Tuple[int, int]:
        """Transfer files directly from S3 to GCS using threading"""
        self.logger.info("Starting direct transfer (S3 -> GCS)...")
        
        successful = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all transfer tasks
            future_to_key = {
                executor.submit(self.transfer_object_direct, s3_key): s3_key 
                for s3_key in s3_objects
            }
            
            # Process completed transfers
            for future in as_completed(future_to_key):
                success, message = future.result()
                if success:
                    successful += 1
                    if successful % 10 == 0:  # Log every 10 successful transfers
                        self.logger.info(f"Progress: {successful}/{len(s3_objects)} files transferred")
                else:
                    failed += 1
                    self.logger.error(message)
                    
        return successful, failed
        
    def transfer_files_via_temp(self, s3_objects: List[str]) -> Tuple[int, int]:
        """Transfer files via temporary directory using threading"""
        self.logger.info("Starting transfer via temporary directory...")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.logger.info(f"Using temporary directory: {temp_dir}")
            
            successful = 0
            failed = 0
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all transfer tasks
                future_to_key = {
                    executor.submit(self.transfer_object_via_file, s3_key, temp_dir): s3_key 
                    for s3_key in s3_objects
                }
                
                # Process completed transfers
                for future in as_completed(future_to_key):
                    success, message = future.result()
                    if success:
                        successful += 1
                        if successful % 10 == 0:  # Log every 10 successful transfers
                            self.logger.info(f"Progress: {successful}/{len(s3_objects)} files transferred")
                    else:
                        failed += 1
                        self.logger.error(message)
                        
        return successful, failed
        
    def verify_transfer(self) -> Tuple[int, int]:
        """Verify the transfer by comparing file counts"""
        self.logger.info("Verifying transfer...")
        
        # Count S3 objects
        s3_objects = self.list_s3_objects()
        s3_count = len(s3_objects)
        
        # Count GCS objects
        gcs_count = 0
        try:
            blobs = self.gcs_client.list_blobs(self.gcs_bucket, prefix=self.gcs_prefix)
            gcs_count = sum(1 for _ in blobs)
        except Exception as e:
            self.logger.error(f"Error counting GCS objects: {e}")
            return s3_count, 0
            
        self.logger.info(f"S3 objects: {s3_count}")
        self.logger.info(f"GCS objects: {gcs_count}")
        
        return s3_count, gcs_count
        
    def run(self, use_direct_transfer: bool = True):
        """Main execution method"""
        start_time = datetime.now()
        self.logger.info("=== S3 to GCS Transfer Started ===")
        self.logger.info(f"Source: s3://{self.s3_bucket}/{self.s3_prefix}")
        self.logger.info(f"Destination: gs://{self.gcs_bucket}/{self.gcs_prefix}")
        
        # Initialize clients
        if not self.initialize_clients():
            self.logger.error("Failed to initialize clients")
            return False
            
        # List S3 objects
        s3_objects = self.list_s3_objects()
        if not s3_objects:
            self.logger.warning("No objects found in S3 or error occurred")
            return False
            
        # Transfer files
        if use_direct_transfer:
            successful, failed = self.transfer_files_direct(s3_objects)
        else:
            successful, failed = self.transfer_files_via_temp(s3_objects)
            
        # Log results
        total_time = datetime.now() - start_time
        self.logger.info(f"Transfer completed in {total_time}")
        self.logger.info(f"Successful transfers: {successful}")
        self.logger.info(f"Failed transfers: {failed}")
        
        # Verify transfer
        s3_count, gcs_count = self.verify_transfer()
        if s3_count == gcs_count:
            self.logger.info("✓ Transfer verification passed")
        else:
            self.logger.warning(f"⚠ File count mismatch: S3({s3_count}) vs GCS({gcs_count})")
            
        self.logger.info("=== Transfer Complete ===")
        return failed == 0

def main():
    parser = argparse.ArgumentParser(description='Copy files from S3 to Google Cloud Storage')
    parser.add_argument('--method', choices=['direct', 'temp'], default='direct',
                       help='Transfer method: direct (S3->GCS) or temp (via local files)')
    parser.add_argument('--s3-bucket', default=S3_BUCKET, help='S3 bucket name')
    parser.add_argument('--s3-prefix', default=S3_PREFIX, help='S3 prefix/path')
    parser.add_argument('--gcs-bucket', default=GCS_BUCKET, help='GCS bucket name')
    parser.add_argument('--gcs-prefix', default=GCS_PREFIX, help='GCS prefix/path')
    
    args = parser.parse_args()
    
    # Create transfer instance
    transfer = S3ToGCSTransfer(
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        gcs_bucket=args.gcs_bucket,
        gcs_prefix=args.gcs_prefix
    )
    
    # Run transfer
    success = transfer.run(use_direct_transfer=(args.method == 'direct'))
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
