import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

class ConfigurationError(Exception):
    """Raised when there is an issue with configuration parameters."""
    pass

class PipelineConfig:
    def __init__(self, mock_db: bool = False, mock_s3: bool = False):
        self.mock_db = mock_db
        self.mock_s3 = mock_s3
        
        # Oracle DB Configurations
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_host = os.getenv("DB_HOST")
        self.db_port = int(os.getenv("DB_PORT", "1521"))
        self.db_service_name = os.getenv("DB_SERVICE_NAME")
        self.db_query = os.getenv("DB_QUERY", "SELECT INTAC_UUID FROM call_records WHERE classification = 'Escrow'")
        
        # AWS & S3 Configuration
        self.aws_region = os.getenv("AWS_REGION") or os.getenv("REGION") or "us-east-1"
        self.s3_bucket = os.getenv("S3_BUCKET_NAME") or os.getenv("S3_BUCKET")
        self.s3_endpoint_url = os.getenv("S3_ENDPOINT_URL")
        
        self.s3_source_bucket = os.getenv("S3_SOURCE_BUCKET_NAME") or os.getenv("S3_SOURCE_BUCKET")
        self.s3_source_prefix = os.getenv("S3_SOURCE_PREFIX") or os.getenv("SOURCE_PREFIX") or "transcripts/"
        
        # Bedrock inputs and outputs prefixes
        self.s3_input_prefix = os.getenv("S3_INPUT_PREFIX") or os.getenv("INPUT_PREFIX") or "bedrock-input/"
        self.s3_output_prefix = os.getenv("S3_OUTPUT_PREFIX") or os.getenv("OUTPUT_PREFIX") or "bedrock-output/"
        
        # Auto-parse S3_SOURCE_URL if specified
        s3_source_url = os.getenv("S3_SOURCE_URL")
        if s3_source_url:
            from urllib.parse import urlparse
            parsed = urlparse(s3_source_url)
            if parsed.scheme in ("http", "https"):
                self.s3_endpoint_url = f"{parsed.scheme}://{parsed.netloc}"
                path_parts = [p for p in parsed.path.split("/") if p]
                if path_parts:
                    if not self.s3_source_bucket:
                        self.s3_source_bucket = path_parts[0]
                    if len(path_parts) > 1 and (not os.getenv("S3_SOURCE_PREFIX") and not os.getenv("SOURCE_PREFIX")):
                        self.s3_source_prefix = "/".join(path_parts[1:])
            elif parsed.scheme == "s3":
                if not self.s3_source_bucket:
                    self.s3_source_bucket = parsed.netloc
                if not os.getenv("S3_SOURCE_PREFIX") and not os.getenv("SOURCE_PREFIX"):
                    self.s3_source_prefix = parsed.path.lstrip("/")
                    
        if not self.s3_source_bucket:
            self.s3_source_bucket = self.s3_bucket
            
        self.s3_source_prefix = self.s3_source_prefix.strip("/") + "/" if self.s3_source_prefix else ""
        self.s3_input_prefix = self.s3_input_prefix.strip("/") + "/" if self.s3_input_prefix else ""
        self.s3_output_prefix = self.s3_output_prefix.strip("/") + "/" if self.s3_output_prefix else ""
        
        # Bedrock Configuration
        self.bedrock_role_arn = os.getenv("BEDROCK_ROLE_ARN") or os.getenv("ROLE_ARN")
        self.bedrock_model_id = os.getenv("BEDROCK_MODEL_ID") or os.getenv("MODEL_ID") or "anthropic.claude-3-sonnet-20240229-v1:0"
        self.bedrock_endpoint_url = os.getenv("BEDROCK_ENDPOINT_URL")
        
        # Performance/Limits Config
        self.max_s3_workers = int(os.getenv("MAX_S3_WORKERS", "50"))
        self.batch_chunk_size = int(os.getenv("BATCH_CHUNK_SIZE", "1000"))
        self.max_records_per_file = int(os.getenv("BEDROCK_MAX_RECORDS_PER_FILE", "45000"))
        self.max_mb_per_file = float(os.getenv("BEDROCK_MAX_MB_PER_FILE", "90"))
        
        # File Paths
        self.local_master_csv = os.getenv("LOCAL_MASTER_CSV_PATH", "output/master_consolidated.csv")
        self.local_state_db = os.getenv("LOCAL_STATE_DB_PATH", "pipeline.db")
        self.prompt_template_path = os.getenv("PROMPT_TEMPLATE_PATH", "prompt_template.txt")
        
        # Ensure directories exist
        Path(self.local_master_csv).parent.mkdir(parents=True, exist_ok=True)
        Path(self.local_state_db).parent.mkdir(parents=True, exist_ok=True)
        
        self.validate()

    def validate(self):
        """Validates configuration parameters, allowing blanks only if mocking is active."""
        if not self.mock_db:
            missing_db = []
            if not self.db_user: missing_db.append("DB_USER")
            if not self.db_password: missing_db.append("DB_PASSWORD")
            if not self.db_host: missing_db.append("DB_HOST")
            if not self.db_service_name: missing_db.append("DB_SERVICE_NAME")
            if missing_db:
                raise ConfigurationError(f"Missing required Oracle DB configuration: {', '.join(missing_db)}")
                
        if not self.mock_s3:
            missing_aws = []
            if not self.s3_bucket: missing_aws.append("S3_BUCKET_NAME / S3_BUCKET")
            if not self.bedrock_role_arn: missing_aws.append("BEDROCK_ROLE_ARN / ROLE_ARN")
            if missing_aws:
                raise ConfigurationError(f"Missing required AWS configuration: {', '.join(missing_aws)}")
                
        # Validate prompt template path exists
        if not Path(self.prompt_template_path).exists():
            # If default template is missing, write a default placeholder
            with open(self.prompt_template_path, "w") as f:
                f.write("Analyze the transcript:\n{transcript}")

    def load_prompt_template(self) -> str:
        """Reads the prompt template from the file."""
        with open(self.prompt_template_path, "r", encoding="utf-8") as f:
            return f.read()

    def __repr__(self):
        return (f"<PipelineConfig mock_db={self.mock_db} mock_s3={self.mock_s3} "
                f"bucket={self.s3_bucket} model={self.bedrock_model_id}>")
