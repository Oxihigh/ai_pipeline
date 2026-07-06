import os
import boto3
import logging
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Any, Optional
from config import PipelineConfig
from botocore.config import Config
import threading
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class S3Client:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.mock = config.mock_s3
        self._missing_file_lock = threading.Lock()
        
        if not self.mock:
            self.s3 = boto3.client(
                "s3", 
                region_name=self.config.aws_region,
                endpoint_url=self.config.s3_endpoint_url,
                config=Config(max_pool_connections=self.config.max_s3_workers)
            )
        else:
            logger.info("Initializing Local Mock S3 Client...")
            self.mock_dir = os.path.abspath(os.path.join(os.getcwd(), "mock_s3_bucket"))
            
            # Create simulated S3 directories
            self.source_dir = os.path.join(self.mock_dir, self.config.s3_source_prefix)
            self.input_dir = os.path.join(self.mock_dir, self.config.s3_input_prefix)
            self.output_dir = os.path.join(self.mock_dir, self.config.s3_output_prefix)
            
            os.makedirs(self.source_dir, exist_ok=True)
            os.makedirs(self.input_dir, exist_ok=True)
            os.makedirs(self.output_dir, exist_ok=True)
            
            self._seed_mock_transcripts()

    def _seed_mock_transcripts(self):
        """Seeds mock transcripts on the filesystem for test run validation."""
        logger.info(f"Seeding mock transcripts in local mock bucket: {self.source_dir}")
        for i in range(1, 101):
            uuid = f"mock-uuid-{i:04d}"
            file_path = os.path.join(self.source_dir, f"{uuid}.txt")
            if not os.path.exists(file_path):
                sentiments = ["Positive", "Neutral", "Frustrated", "Anxious"]
                sentiment = sentiments[i % len(sentiments)]
                issues = [
                    "wire instructions were delayed",
                    "closing escrow date needs to be extended by 5 days",
                    "verification of the initial earnest money deposit",
                    "dispute about closing costs fees"
                ]
                issue = issues[i % len(issues)]
                
                content = (
                    f"Time: 2026-07-02T10:{i:02d}:00\n"
                    f"Agent: Thank you for calling Escrow Support. My name is Sarah. How can I help you today?\n"
                    f"Customer: Hi Sarah, I'm calling about my Escrow account {uuid}. I'm really {sentiment} because {issue}.\n"
                    f"Agent: I completely understand your concern. Let me check our records for account {uuid}.\n"
                    f"Agent: I see the update here. I will prioritize this and update your closing coordinator.\n"
                    f"Customer: Great. Thank you for resolving this. What are my next steps?\n"
                    f"Agent: Please watch your secure message inbox for updates. Have a great day!"
                )
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(content)

    def fetch_transcript_content(self, uuid: str) -> Tuple[str, str, Optional[str]]:
        """
        Retrieves the content of a single transcript from S3.
        Returns: (uuid, content, error_message)
        """
        loan_num = uuid.split("_")[0] if "_" in uuid else uuid
        key = f"{self.config.s3_source_prefix}{loan_num}.txt"
        
        if self.mock:
            local_file = os.path.join(self.source_dir, f"{loan_num}.txt")
            if not os.path.exists(local_file):
                return uuid, "", f"Transcript file {loan_num}.txt not found in mock bucket"
            try:
                with open(local_file, "r", encoding="utf-8") as f:
                    content = f.read()
                return uuid, content, None
            except Exception as e:
                return uuid, "", f"Error reading mock file: {e}"
        else:
            try:
                response = self.s3.get_object(Bucket=self.config.s3_source_bucket, Key=key)
                content = response["Body"].read().decode("utf-8")
                return uuid, content, None
            except self.s3.exceptions.NoSuchKey:
                with self._missing_file_lock:
                    with open("missing_transcripts.txt", "a") as f:
                        f.write(f"{uuid}\n")
                return uuid, "", f"S3 object not found (key: {key})"
            except ClientError as e:
                if e.response.get('Error', {}).get('Code') == 'AccessDenied':
                    with self._missing_file_lock:
                        with open("missing_transcripts.txt", "a") as f:
                            f.write(f"{uuid}\n")
                    return uuid, "", f"Transcript not found (AccessDenied due to missing ListBucket): {key}"
                return uuid, "", f"S3 error: {e}"
            except Exception as e:
                return uuid, "", f"S3 error: {e}"

    def fetch_transcripts_parallel(self, uuids: List[str]) -> List[Tuple[str, str, Optional[str]]]:
        """
        Fetches transcript contents for a list of UUIDs in parallel using ThreadPoolExecutor.
        Returns a list of tuples: (uuid, content, error_message)
        """
        results = []
        workers = self.config.max_s3_workers
        logger.info(f"Fetching {len(uuids)} transcripts from S3 in parallel using {workers} workers...")
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_uuid = {executor.submit(self.fetch_transcript_content, uuid): uuid for uuid in uuids}
            for future in as_completed(future_to_uuid):
                try:
                    uuid, content, err = future.result()
                    results.append((uuid, content, err))
                except Exception as e:
                    uuid = future_to_uuid[future]
                    results.append((uuid, "", f"Thread pool execution error: {e}"))
                    
        return results

    def upload_file(self, local_path: str, s3_key: str):
        """Uploads a local file to S3 (or copies to mock directory)."""
        logger.info(f"Uploading local file {local_path} to S3 Key: {s3_key}...")
        if self.mock:
            dest_path = os.path.join(self.mock_dir, s3_key)
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            shutil.copy2(local_path, dest_path)
            logger.info(f"Mock S3 Upload Complete: Copy to {dest_path}")
        else:
            try:
                self.s3.upload_file(local_path, self.config.s3_bucket, s3_key)
                logger.info("AWS S3 Upload Complete.")
            except Exception as e:
                logger.error(f"Failed to upload {local_path} to S3: {e}")
                raise e

    def list_objects(self, prefix: str) -> List[str]:
        """Lists object keys in S3 folder (or mock folder) with the given prefix."""
        if self.mock:
            target_dir = os.path.join(self.mock_dir, prefix)
            if not os.path.exists(target_dir):
                return []
            keys = []
            for root, _, files in os.walk(target_dir):
                for file in files:
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, self.mock_dir)
                    # Normalize to forward slashes for S3-like key
                    keys.append(rel_path.replace("\\", "/"))
            return keys
        else:
            try:
                keys = []
                paginator = self.s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self.config.s3_bucket, Prefix=prefix):
                    if "Contents" in page:
                        for obj in page["Contents"]:
                            keys.append(obj["Key"])
                return keys
            except Exception as e:
                logger.error(f"Failed to list S3 objects in {prefix}: {e}")
                raise e

    def download_file(self, s3_key: str, local_path: str):
        """Downloads an object from S3 (or copies from mock directory)."""
        logger.info(f"Downloading S3 key {s3_key} to local path {local_path}...")
        if self.mock:
            src_path = os.path.join(self.mock_dir, s3_key)
            if not os.path.exists(src_path):
                raise FileNotFoundError(f"Mock S3 file {s3_key} does not exist.")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            shutil.copy2(src_path, local_path)
            logger.info("Mock S3 Download Complete.")
        else:
            try:
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                self.s3.download_file(self.config.s3_bucket, s3_key, local_path)
                logger.info("AWS S3 Download Complete.")
            except Exception as e:
                logger.error(f"Failed to download S3 key {s3_key}: {e}")
                raise e
