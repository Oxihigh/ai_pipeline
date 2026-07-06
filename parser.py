import os
import json
import csv
import logging
from typing import Dict, Any, List, Optional
from config import PipelineConfig
from state_manager import StateManager
from s3_client import S3Client

logger = logging.getLogger(__name__)

class ResultParser:
    def __init__(self, config: PipelineConfig, state_manager: StateManager, s3_client: S3Client):
        self.config = config
        self.state_manager = state_manager
        self.s3_client = s3_client

    def download_and_parse_outputs(self, job_arn: str) -> Dict[str, int]:
        """
        1. Lists all output files in the output S3 location corresponding to the job.
        2. Downloads/streams each JSONL output file.
        3. Parses the AI responses.
        4. Updates the SQLite state manager.
        
        Returns a dictionary of parsing statistics (success, errors).
        """
        # Parse the job ID out of the ARN to locate the nested folder if any
        # Bedrock batch output structure usually appends a folder named after the job UUID or similar,
        # or puts files directly in the configured prefix.
        job_id = job_arn.split("/")[-1]
        
        # We search both under output_prefix directly and output_prefix/job_id/
        search_prefixes = [
            f"{self.config.s3_output_prefix}{job_id}/",
            self.config.s3_output_prefix
        ]
        
        output_keys = []
        for prefix in search_prefixes:
            try:
                keys = self.s3_client.list_objects(prefix)
                # Keep only files that are outputs (usually .jsonl.out or .out or .jsonl)
                # Ignore manifest.json
                valid_keys = [k for k in keys if k.endswith(".out") or (k.endswith(".jsonl") and "manifest.json" not in k)]
                if valid_keys:
                    output_keys = valid_keys
                    logger.info(f"Found {len(output_keys)} Bedrock output file(s) under prefix: {prefix}")
                    break
            except Exception as e:
                logger.warning(f"Error listing prefix {prefix}: {e}")
                
        if not output_keys:
            logger.error(f"No Bedrock batch inference output files found for job {job_arn}.")
            return {"success": 0, "failed": 0}

        local_temp_dir = os.path.join(os.getcwd(), "data", "temp_output")
        os.makedirs(local_temp_dir, exist_ok=True)
        
        stats = {"success": 0, "failed": 0}
        
        for key in output_keys:
            file_name = os.path.basename(key)
            local_path = os.path.join(local_temp_dir, file_name)
            
            try:
                self.s3_client.download_file(key, local_path)
                
                success_records = []
                failed_records = []
                
                with open(local_path, "r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f, 1):
                        if not line.strip():
                            continue
                        try:
                            record = json.loads(line)
                            uuid = record.get("recordId")
                            if not uuid:
                                logger.warning(f"Line {line_num} in {file_name} missing recordId. Skipping.")
                                continue
                                
                            # Check for Bedrock error for this record
                            if "error" in record:
                                err_msg = record["error"].get("message", "Unknown Bedrock error")
                                logger.warning(f"Record {uuid} encountered Bedrock inference error: {err_msg}")
                                failed_records.append(uuid)
                                stats["failed"] += 1
                                continue
                                
                            # Extract Claude Messages response text
                            model_output = record.get("modelOutput", {})
                            content = model_output.get("content", [])
                            
                            ai_text = ""
                            if content and isinstance(content, list):
                                # Claude Messages format
                                ai_text = content[0].get("text", "")
                            elif "results" in model_output:
                                # Standard Llama or old-format models support
                                ai_text = model_output["results"][0].get("outputText", "")
                            else:
                                # Fallback to dump output as string
                                ai_text = json.dumps(model_output)
                                
                            success_records.append((uuid, ai_text))
                            stats["success"] += 1
                            
                        except Exception as e:
                            logger.error(f"Error parsing line {line_num} in {file_name}: {e}")
                            stats["failed"] += 1

                # Bulk write parsing results to the SQLite state database
                if success_records:
                    self.state_manager.mark_records_as_completed(success_records)
                if failed_records:
                    self.state_manager.mark_records_as_failed(failed_records, "Bedrock inference record failure")
                    
                logger.info(f"Finished parsing file {file_name}. Records parsed: {len(success_records)}, Failed: {len(failed_records)}.")
                
            except Exception as e:
                logger.error(f"Failed to process output file {key}: {e}")
            finally:
                # Clean up local temporary file to maintain design constraints
                if os.path.exists(local_path):
                    try:
                        os.remove(local_path)
                    except Exception as e:
                        logger.warning(f"Could not remove temporary file {local_path}: {e}")
                        
        return stats

    def export_to_master_csv(self):
        """
        Reads all COMPLETED (and optionally FAILED) records from the SQLite database
        and compiles them into a single consolidated master CSV file.
        Attempts to parse structured JSON fields out of the raw response.
        """
        logger.info(f"Compiling processed records into master CSV at {self.config.local_master_csv}...")
        
        # Retrieve all records from DB
        with self.state_manager._get_connection() as conn:
            # Querying everything to generate the full CSV
            cursor = conn.execute("""
                SELECT intac_uuid, status, db_extracted_at, completed_at, ai_response, error_message,
                       loan_number, latest_escrow_call_dt, long_comment_code, long_comment_date,
                       long_comment_user_id, full_comment, comment_actv_flg, db_intac_uuid
                FROM transcripts
                ORDER BY db_extracted_at ASC
            """)
            rows = cursor.fetchall()

        if not rows:
            logger.warning("No records found in state database to export.")
            return

        csv_headers = [
            "intac_uuid",
            "db_intac_uuid",
            "status",
            "db_extracted_at",
            "completed_at",
            "loan_number",
            "latest_escrow_call_dt",
            "long_comment_code",
            "long_comment_date",
            "long_comment_user_id",
            "full_comment",
            "comment_actv_flg",
            "call_reason",
            "customer_sentiment",
            "main_escrow_issue",
            "next_steps",
            "raw_ai_response",
            "error_message"
        ]
        
        written_count = 0
        try:
            with open(self.config.local_master_csv, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
                writer.writeheader()
                
                for row in rows:
                    row_dict = dict(row)
                    
                    # Parse custom keys out of JSON if possible
                    call_reason = ""
                    customer_sentiment = ""
                    main_escrow_issue = ""
                    next_steps = ""
                    
                    raw_res = row_dict.get("ai_response") or ""
                    if raw_res:
                        try:
                            # Clean potential markdown wrappers e.g. ```json ... ```
                            cleaned_res = raw_res.strip()
                            if cleaned_res.startswith("```json"):
                                cleaned_res = cleaned_res[7:]
                            if cleaned_res.endswith("```"):
                                cleaned_res = cleaned_res[:-3]
                            cleaned_res = cleaned_res.strip()
                            
                            parsed_json = json.loads(cleaned_res)
                            call_reason = parsed_json.get("call_reason", "")
                            customer_sentiment = parsed_json.get("customer_sentiment", "")
                            main_escrow_issue = parsed_json.get("main_escrow_issue", "")
                            next_steps = parsed_json.get("next_steps", "")
                        except Exception:
                            # If it's not a JSON, treat it as general response
                            call_reason = raw_res
                    
                    writer.writerow({
                        "intac_uuid": row_dict["intac_uuid"],
                        "db_intac_uuid": row_dict.get("db_intac_uuid") or "",
                        "status": row_dict["status"],
                        "db_extracted_at": row_dict["db_extracted_at"],
                        "completed_at": row_dict["completed_at"] or "",
                        "loan_number": row_dict.get("loan_number") or "",
                        "latest_escrow_call_dt": row_dict.get("latest_escrow_call_dt") or "",
                        "long_comment_code": row_dict.get("long_comment_code") or "",
                        "long_comment_date": row_dict.get("long_comment_date") or "",
                        "long_comment_user_id": row_dict.get("long_comment_user_id") or "",
                        "full_comment": row_dict.get("full_comment") or "",
                        "comment_actv_flg": row_dict.get("comment_actv_flg") or "",
                        "call_reason": call_reason,
                        "customer_sentiment": customer_sentiment,
                        "main_escrow_issue": main_escrow_issue,
                        "next_steps": next_steps,
                        "raw_ai_response": raw_res,
                        "error_message": row_dict["error_message"] or ""
                    })
                    written_count += 1
                    
            logger.info(f"Successfully wrote {written_count} rows to master CSV file: {self.config.local_master_csv}")
            
        except Exception as e:
            logger.error(f"Failed to export master CSV: {e}")
            raise e
