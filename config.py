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
        self.aws_region = os.getenv("AWS_REGION", "us-east-1")
        self.s3_bucket = os.getenv("S3_BUCKET_NAME")
        self.s3_source_prefix = os.getenv("S3_SOURCE_PREFIX", "transcripts/").strip("/") + "/"
        self.s3_input_prefix = os.getenv("S3_INPUT_PREFIX", "bedrock-input/").strip("/") + "/"
        self.s3_output_prefix = os.getenv("S3_OUTPUT_PREFIX", "bedrock-output/").strip("/") + "/"
        self.s3_endpoint_url = os.getenv("S3_ENDPOINT_URL")
        
        # Bedrock Configuration
        self.bedrock_role_arn = os.getenv("BEDROCK_ROLE_ARN")
        self.bedrock_model_id = os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-sonnet-20240229-v1:0")
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
            if not self.s3_bucket: missing_aws.append("S3_BUCKET_NAME")
            if not self.bedrock_role_arn: missing_aws.append("BEDROCK_ROLE_ARN")
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
