import time
import os
import json
import logging
import boto3
from typing import Dict, Any, Tuple, Optional
from config import PipelineConfig

logger = logging.getLogger(__name__)

class BedrockClient:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.mock = config.mock_s3
        
        if not self.mock:
            self.bedrock = boto3.client(
                "bedrock", 
                region_name=self.config.aws_region,
                endpoint_url=self.config.bedrock_endpoint_url
            )
        else:
            logger.info("Initializing Local Mock Bedrock Client...")
            # We will use this to track mock job states
            self.mock_jobs: Dict[str, Dict[str, Any]] = {}

    def create_batch_job(self, job_name: str, input_s3_uri: str, output_s3_uri: str) -> str:
        """
        Initiates a Bedrock Batch Inference model invocation job.
        Returns the job ARN/ID.
        """
        if self.mock:
            job_id = f"mock-job-{int(time.time())}"
            job_arn = f"arn:aws:bedrock:{self.config.aws_region}:123456789012:model-invocation-job/{job_id}"
            
            logger.info(f"[Mock] Creating model invocation job '{job_name}'...")
            logger.info(f"[Mock] Input URI: {input_s3_uri}")
            logger.info(f"[Mock] Output URI: {output_s3_uri}")
            
            # Simulate the batch processing by generating mock output files in the mock S3 location
            self._simulate_batch_inference(input_s3_uri, output_s3_uri, job_id)
            
            self.mock_jobs[job_arn] = {
                "jobName": job_name,
                "jobArn": job_arn,
                "status": "Submitted",
                "createdAt": time.time(),
                "completedAt": None
            }
            return job_arn
        else:
            logger.info(f"Submitting Bedrock Batch Inference job '{job_name}'...")
            try:
                response = self.bedrock.create_model_invocation_job(
                    jobName=job_name,
                    roleArn=self.config.bedrock_role_arn,
                    modelId=self.config.bedrock_model_id,
                    inputDataConfig={
                        "s3InputDataConfig": {
                            "s3Uri": input_s3_uri,
                            "s3InputFormat": "JSONL"
                        }
                    },
                    outputDataConfig={
                        "s3OutputDataConfig": {
                            "s3Uri": output_s3_uri
                        }
                    }
                )
                job_arn = response["jobArn"]
                logger.info(f"Successfully created Bedrock Batch Job. Job ARN: {job_arn}")
                return job_arn
            except Exception as e:
                logger.error(f"Failed to create Bedrock Batch Job: {e}")
                raise e

    def get_job_status(self, job_arn: str) -> Tuple[str, Optional[str]]:
        """
        Retrieves the status of a Bedrock Batch job.
        Returns: (status_string, error_message)
        """
        if self.mock:
            job = self.mock_jobs.get(job_arn)
            if not job:
                return "Failed", "Job not found in mock store"
                
            elapsed = time.time() - job["createdAt"]
            # Simulate step-by-step progress
            if job["status"] == "Submitted" and elapsed > 2:
                job["status"] = "InProgress"
                logger.info(f"[Mock Job status transition]: Submitted -> InProgress")
            elif job["status"] == "InProgress" and elapsed > 5:
                job["status"] = "Completed"
                job["completedAt"] = time.time()
                logger.info(f"[Mock Job status transition]: InProgress -> Completed")
                
            return job["status"], None
        else:
            try:
                response = self.bedrock.get_model_invocation_job(jobIdentifier=job_arn)
                status = response["status"]
                error_message = response.get("failureMessage")
                return status, error_message
            except Exception as e:
                logger.error(f"Failed to check status for Bedrock Batch Job {job_arn}: {e}")
                return "Failed", str(e)

    def _simulate_batch_inference(self, input_s3_uri: str, output_s3_uri: str, job_id: str):
        """
        Generates simulated Bedrock output JSONL files in the local mock output S3 prefix.
        It parses the input files to generate realistic Claude JSON responses.
        """
        # Parse bucket and prefix out of the input URI
        # Input URI looks like: s3://bucket/bedrock-input/
        mock_dir = os.path.abspath(os.path.join(os.getcwd(), "mock_s3_bucket"))
        
        # Remove s3://bucket/ part
        input_prefix = input_s3_uri.replace(f"s3://{self.config.s3_bucket}/", "")
        output_prefix = output_s3_uri.replace(f"s3://{self.config.s3_bucket}/", "")
        
        input_folder = os.path.join(mock_dir, input_prefix)
        output_folder = os.path.join(mock_dir, output_prefix, job_id)  # Bedrock outputs into a folder named after the job ID or nested
        
        os.makedirs(output_folder, exist_ok=True)
        
        if not os.path.exists(input_folder):
            logger.warning(f"Simulated S3 input directory {input_folder} does not exist. No files to simulate.")
            return
            
        input_files = [f for f in os.listdir(input_folder) if f.endswith(".jsonl")]
        logger.info(f"[Mock Simulation] Simulating inference for {len(input_files)} input JSONL files...")
        
        for file_name in input_files:
            input_file_path = os.path.join(input_folder, file_name)
            output_file_path = os.path.join(output_folder, f"{file_name}.out")
            
            records_written = 0
            with open(input_file_path, "r", encoding="utf-8") as infile, \
                 open(output_file_path, "w", encoding="utf-8") as outfile:
                 
                for line in infile:
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                        uuid = record["recordId"]
                        prompt = record["modelInput"]["messages"][0]["content"][0]["text"]
                        
                        # Generate a mock Claude structured JSON response
                        # Seeded by the uuid index
                        idx = int(uuid.split("-")[-1]) if "-" in uuid and uuid.split("-")[-1].isdigit() else 1
                        sentiments = ["Positive", "Neutral", "Frustrated", "Anxious"]
                        reasons = ["Escrow status check", "Extend closing date", "Wire instruction query", "Deposit verification"]
                        issues = ["None", "Closing coordinator out of office", "Bank wire verification delayed", "Discrepancy in fees"]
                        
                        ai_response_obj = {
                            "call_reason": reasons[idx % len(reasons)],
                            "customer_sentiment": sentiments[idx % len(sentiments)],
                            "main_escrow_issue": issues[idx % len(issues)],
                            "next_steps": "Follow up with escrow agent in 24 hours."
                        }
                        
                        mock_out_record = {
                            "recordId": uuid,
                            "modelOutput": {
                                "id": f"msg_mock_{uuid}",
                                "model": self.config.bedrock_model_id,
                                "type": "message",
                                "role": "assistant",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": json.dumps(ai_response_obj)
                                    }
                                ],
                                "usage": {
                                    "input_tokens": 150,
                                    "output_tokens": 60
                                }
                            }
                        }
                        outfile.write(json.dumps(mock_out_record) + "\n")
                        records_written += 1
                    except Exception as e:
                        logger.error(f"Error simulating record in file {file_name}: {e}")
            
            # Write a simulated manifest.json in the output directory
            manifest_path = os.path.join(output_folder, "manifest.json")
            manifest_data = {
                "totalRecords": records_written,
                "successfulRecords": records_written,
                "failedRecords": 0,
                "totalInputTokens": records_written * 150,
                "totalOutputTokens": records_written * 60
            }
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest_data, f, indent=2)
                
            logger.info(f"[Mock Simulation] Created output {output_file_path} with {records_written} responses.")
