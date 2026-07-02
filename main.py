import os
import sys
import argparse
import logging
import json
import time
from pathlib import Path
from typing import List

from config import PipelineConfig, ConfigurationError
from state_manager import StateManager
from db_client import DBClient
from s3_client import S3Client
from bedrock_client import BedrockClient
from parser import ResultParser

# Configure logging to write to both stdout and a log file with structured layout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("PipelineOrchestrator")

def run_extraction(config: PipelineConfig, state_manager: StateManager, limit: int = None):
    """Stage 1: Extract qualifying UUIDs from Oracle and store in SQLite."""
    logger.info("=========================================")
    logger.info("STAGE 1: Oracle Database Extraction")
    logger.info("=========================================")
    
    db_client = DBClient(config)
    uuids = db_client.extract_escrow_uuids()
    
    if limit:
        logger.info(f"Applying limit: restricting extraction list to first {limit} records.")
        uuids = uuids[:limit]
        
    if not uuids:
        logger.warning("No records extracted from the database.")
        return
        
    inserted = state_manager.add_identified_records(uuids)
    logger.info(f"Database extraction finished. Total identified records in DB: {len(uuids)}.")
    logger.info(f"Newly inserted records into tracking table: {inserted}.")
    
    stats = state_manager.get_stats()
    logger.info(f"Current Pipeline DB Stats: {stats}")

def run_preparation(config: PipelineConfig, state_manager: StateManager, s3_client: S3Client, limit: int = None):
    """Stage 2: Fetch transcripts from S3 in parallel, construct JSONL, and upload to S3."""
    logger.info("=========================================")
    logger.info("STAGE 2: Transcript Preparation & Batch Upload")
    logger.info("=========================================")
    
    # Reset failed records so they are retried if we are running again
    resets = state_manager.reset_failed_records()
    if resets > 0:
        logger.info(f"Reset {resets} previously FAILED records back to IDENTIFIED for retry.")
        
    records = state_manager.get_records_by_status("IDENTIFIED", limit=limit)
    if not records:
        logger.info("No records in 'IDENTIFIED' status to prepare. Skipping stage.")
        return
        
    logger.info(f"Retrieved {len(records)} records in 'IDENTIFIED' status to process.")
    
    # Load prompt template
    prompt_template = config.load_prompt_template()
    
    local_input_dir = os.path.join(os.getcwd(), "data", "input")
    os.makedirs(local_input_dir, exist_ok=True)
    
    # We will fetch and build batch files in chunks
    chunk_size = config.batch_chunk_size
    batch_idx = 1
    
    current_batch_records = []
    current_jsonl_lines = []
    current_file_size_bytes = 0
    
    uploaded_batch_files = []
    
    # S3 Batch formatting: 
    # Bedrock Batch Inference takes JSONL where each line has:
    # {"recordId": "...", "modelInput": {"messages": [{"role": "user", "content": [{"type": "text", "text": "..."}]}]}}
    
    def write_current_batch():
        nonlocal batch_idx, current_jsonl_lines, current_batch_records, current_file_size_bytes
        if not current_jsonl_lines:
            return
            
        file_name = f"batch_{int(time.time())}_{batch_idx}.jsonl"
        local_path = os.path.join(local_input_dir, file_name)
        
        logger.info(f"Writing {len(current_jsonl_lines)} records to local JSONL: {local_path} ({current_file_size_bytes / (1024*1024):.2f} MB)...")
        with open(local_path, "w", encoding="utf-8") as f:
            f.write("\n".join(current_jsonl_lines) + "\n")
            
        # Upload to S3
        s3_key = f"{config.s3_input_prefix}{file_name}"
        s3_client.upload_file(local_path, s3_key)
        
        uploaded_batch_files.append((s3_key, list(current_batch_records)))
        
        # Clean up local file immediately to keep disk footprint minimal
        try:
            os.remove(local_path)
            logger.info(f"Deleted local temporary file: {local_path}")
        except Exception as e:
            logger.warning(f"Could not delete temporary file {local_path}: {e}")
            
        # Reset counters
        batch_idx += 1
        current_jsonl_lines = []
        current_batch_records = []
        current_file_size_bytes = 0

    # Process all retrieved records in S3 fetch chunks
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        uuids = [r["intac_uuid"] for r in chunk]
        
        # Retrieve contents in parallel from S3
        results = s3_client.fetch_transcripts_parallel(uuids)
        
        success_uuids = []
        failed_uuids = []
        
        for uuid, content, error in results:
            if error:
                logger.warning(f"Failed to retrieve transcript for UUID {uuid}: {error}")
                state_manager.mark_records_as_failed([uuid], error)
                failed_uuids.append(uuid)
                continue
                
            # Build Bedrock request JSON
            formatted_prompt = prompt_template.replace("{transcript}", content)
            
            # Format according to Claude Messages API syntax
            model_input = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1024,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": formatted_prompt
                            }
                        ]
                    }
                ]
            }
            
            line_obj = {
                "recordId": uuid,
                "modelInput": model_input
            }
            
            line_str = json.dumps(line_obj)
            line_bytes_len = len(line_str.encode("utf-8"))
            
            # Check limits (Max records or Max size limit)
            # If adding this line exceeds limits, write current batch first
            if (len(current_jsonl_lines) + 1 > config.max_records_per_file) or \
               ((current_file_size_bytes + line_bytes_len) > config.max_mb_per_file * 1024 * 1024):
                logger.info("Approaching file size or record count limits. Creating new batch shard.")
                write_current_batch()
                
            current_jsonl_lines.append(line_str)
            current_batch_records.append(uuid)
            current_file_size_bytes += line_bytes_len + 1 # Include newline character
            success_uuids.append(uuid)
            
    # Write any remaining lines to a final batch file
    write_current_batch()
    
    # Mark successfully prepared records as BATCHED in SQLite state database
    # Note: We associate them with the job run during the submit stage
    for s3_key, uuids_in_batch in uploaded_batch_files:
        # We don't have the job ID yet, so we mark them with a temp job name placeholder
        # and we update it during the inference step
        state_manager.mark_records_as_batched(uuids_in_batch, "PENDING_SUBMISSION", s3_key)
        
    logger.info("Stage 2: Preparation and Batch uploads complete.")
    stats = state_manager.get_stats()
    logger.info(f"Current Pipeline DB Stats: {stats}")

def run_inference(config: PipelineConfig, state_manager: StateManager, bedrock_client: BedrockClient, poll: bool = True) -> str:
    """Stage 3: Submit Bedrock Batch Inference Job and optionally poll for completion."""
    logger.info("=========================================")
    logger.info("STAGE 3: Amazon Bedrock Batch Inference")
    logger.info("=========================================")
    
    # Check if we have BATCHED records pending submission
    records = state_manager.get_records_by_status("BATCHED")
    # Also find records tagged as 'PENDING_SUBMISSION'
    pending_records = [r for r in records if r["batch_job_id"] == "PENDING_SUBMISSION"]
    
    if not pending_records:
        # Check if there are any BATCHED records that are already running
        running_records = [r for r in records if r["batch_job_id"] != "PENDING_SUBMISSION"]
        if running_records:
            active_job_arn = running_records[0]["batch_job_id"]
            logger.info(f"Found active running job in state DB: {active_job_arn}")
            if poll:
                monitor_job(bedrock_client, active_job_arn)
            return active_job_arn
            
        logger.info("No records in 'BATCHED' status awaiting submission. Skipping Stage.")
        return ""
        
    uuids = [r["intac_uuid"] for r in pending_records]
    logger.info(f"Submitting Bedrock Batch Inference job for {len(uuids)} records.")
    
    # Input folder S3 path
    input_s3_uri = f"s3://{config.s3_bucket}/{config.s3_input_prefix}"
    output_s3_uri = f"s3://{config.s3_bucket}/{config.s3_output_prefix}"
    
    job_name = f"escrow-inference-pipeline-{int(time.time())}"
    
    # Submit job
    job_arn = bedrock_client.create_batch_job(job_name, input_s3_uri, output_s3_uri)
    
    # Update SQLite database: link records to the actual job ARN
    state_manager.mark_records_as_batched(uuids, job_arn, "MULTIPLE_SHARDS")
    logger.info(f"State DB updated. All records linked to Job ARN: {job_arn}")
    
    if poll:
        monitor_job(bedrock_client, job_arn)
        
    return job_arn

def monitor_job(bedrock_client: BedrockClient, job_arn: str):
    """Polls Bedrock Job Status until completion."""
    logger.info(f"Starting status monitoring loop for job: {job_arn}")
    while True:
        status, err = bedrock_client.get_job_status(job_arn)
        logger.info(f"Job Status: {status}")
        
        if status in ["Completed", "CompletedWithErrors"]:
            logger.info("Bedrock Batch Inference Job Completed!")
            break
        elif status in ["Failed", "Stopped"]:
            logger.error(f"Bedrock Batch Inference Job terminated. Status: {status}. Error: {err}")
            raise RuntimeError(f"Bedrock batch job failed: {err}")
        elif status in ["Stopping"]:
            logger.warning("Bedrock Batch Inference Job is stopping...")
            
        # Poll every 20 seconds
        time.sleep(20)

def run_consolidation(config: PipelineConfig, state_manager: StateManager, s3_client: S3Client, job_arn: str = None):
    """Stage 4: Parse outputs from S3 and write final consolidated CSV."""
    logger.info("=========================================")
    logger.info("STAGE 4: Result Parsing & CSV Consolidation")
    logger.info("=========================================")
    
    parser = ResultParser(config, state_manager, s3_client)
    
    if job_arn:
        logger.info(f"Running output parsing for specified job: {job_arn}")
        stats = parser.download_and_parse_outputs(job_arn)
        logger.info(f"Output parsing stats: {stats}")
    else:
        # Find active batch job ARN from BATCHED records in database
        records = state_manager.get_records_by_status("BATCHED")
        if records:
            job_arns = list(set([r["batch_job_id"] for r in records if r["batch_job_id"] != "PENDING_SUBMISSION"]))
            if job_arns:
                logger.info(f"Discovered active job ARNs in State DB: {job_arns}")
                for arn in job_arns:
                    stats = parser.download_and_parse_outputs(arn)
                    logger.info(f"Output parsing stats for {arn}: {stats}")
            else:
                logger.warning("No active Bedrock Job ARN recorded in 'BATCHED' database rows.")
        else:
            logger.info("No records in 'BATCHED' status in the database. Parsing stage proceeds using cached completed items.")
            
    # Export all COMPLETED records in local state manager to CSV
    parser.export_to_master_csv()

def main():
    parser = argparse.ArgumentParser(description="Escrow Call Transcript AI Processing Pipeline CLI")
    parser.add_argument(
        "stage", 
        choices=["extract", "prepare", "inference", "consolidate", "run-all"],
        help="Pipeline execution stage to run."
    )
    parser.add_argument("--mock-db", action="store_true", help="Use local mock generator instead of Oracle DB")
    parser.add_argument("--mock-s3", action="store_true", help="Use local mock files instead of AWS S3 & Bedrock")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of transcripts to extract/process")
    parser.add_argument("--job-arn", type=str, default=None, help="Force parse a specific Bedrock Job ARN (consolidate stage only)")
    parser.add_argument("--no-poll", action="store_true", help="Submit Bedrock Batch Job and exit without waiting for status")
    
    args = parser.parse_args()
    
    # Initialize configuration
    try:
        config = PipelineConfig(mock_db=args.mock_db, mock_s3=args.mock_s3)
    except ConfigurationError as ce:
        logger.error(f"Configuration validation failed: {ce}")
        logger.error("Please configure your .env file or system environment variables. Reference .env.example.")
        sys.exit(1)
        
    logger.info(f"Loaded config: {config}")
    
    # Initialize State DB
    state_manager = StateManager(config.local_state_db)
    
    s3_client = S3Client(config)
    bedrock_client = BedrockClient(config)
    
    try:
        if args.stage == "extract":
            run_extraction(config, state_manager, limit=args.limit)
            
        elif args.stage == "prepare":
            run_preparation(config, state_manager, s3_client, limit=args.limit)
            
        elif args.stage == "inference":
            run_inference(config, state_manager, bedrock_client, poll=not args.no_poll)
            
        elif args.stage == "consolidate":
            run_consolidation(config, state_manager, s3_client, job_arn=args.job_arn)
            
        elif args.stage == "run-all":
            logger.info("=========================================")
            logger.info("STARTING FULL END-TO-END PIPELINE RUN")
            logger.info("=========================================")
            
            run_extraction(config, state_manager, limit=args.limit)
            run_preparation(config, state_manager, s3_client, limit=args.limit)
            
            job_arn = run_inference(config, state_manager, bedrock_client, poll=not args.no_poll)
            
            if not args.no_poll and job_arn:
                run_consolidation(config, state_manager, s3_client, job_arn=job_arn)
            else:
                logger.info("Pipeline ran in async submit mode. Monitor the batch job in Bedrock console and run consolidation later.")
                
            logger.info("=========================================")
            logger.info("FULL PIPELINE PIPELINE RUN FINISHED")
            logger.info("=========================================")
            
    except Exception as e:
        logger.exception(f"Pipeline execution halted due to error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
