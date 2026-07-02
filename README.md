# Escrow Call Transcript AI Processing Pipeline

A scalable, end-to-end, restartable AI processing pipeline that:
1. Queries an Oracle Database to identify Escrow-related call records.
2. Extracts their unique identifiers (`INTAC_UUID`).
3. Fetches transcript contents directly from Amazon S3 in parallel (using multiple threads) to avoid storing large volumes of transcripts locally.
4. Generates Amazon Bedrock Batch Inference JSONL input files with prompt template formatting.
5. Initiates, monitors, and waits for a Bedrock Batch Inference job.
6. Downloads outputs from S3 and parses the structured responses.
7. Consolidates all outputs into a single structured master CSV.

---

## Key Design Features
* **Modular CLI Execution**: Run individual stages (`extract`, `prepare`, `inference`, `consolidate`) or run everything end-to-end (`run-all`).
* **Robust Restartability**: Uses a local SQLite database (`pipeline.db`) to log the current status of every record. If a network interruption occurs, the pipeline picks up exactly where it left off, avoiding duplicate database queries, S3 downloads, or Bedrock API invocations.
* **EC2 & IAM Compatibility**: Auto-resolves credentials using the AWS credential chain when running on EC2 instances with attached IAM instance profiles.
* **Parallel Processing**: Employs Python's `concurrent.futures.ThreadPoolExecutor` to speed up S3 downloads of 70,000+ transcripts.
* **Zero Footprint**: Deletes temporary local batch input files after uploading them to S3.
* **Local Offline Simulation**: Supports `--mock-db` and `--mock-s3` flags to run the entire pipeline offline for testing and validation.

---

## File Structure
* `main.py`: CLI entrypoint coordinating the orchestrations.
* `config.py`: Configuration loading, directory creations, and validation.
* `db_client.py`: Oracle DB thin connection client and mock record generator.
* `s3_client.py`: AWS S3 integrations (parallel downloads, uploads, and local file mock bucket).
* `bedrock_client.py`: Bedrock batch API integration (job submission, status polling, and local output simulation).
* `state_manager.py`: SQLite transaction wrapper for progress tracking.
* `parser.py`: Bedrock JSONL parser and Master CSV generator.
* `prompt_template.txt`: Custom prompt template targeting Claude models.
* `requirements.txt`: Project package dependencies.

---

## Installation & Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment Settings**:
   Copy `.env.example` to `.env` and fill in your Oracle DSN, S3 bucket name, IAM role ARN, and paths.
   ```bash
   cp .env.example .env
   ```

---

## How to Run

### 1. Offline Local Test Run (Dry-Run)
You can run the full pipeline locally with synthetic database and transcript generation (no AWS/Oracle access needed):
```bash
python main.py run-all --mock-db --mock-s3
```
This will:
* Generate 100 mock call transcripts in `mock_s3_bucket/transcripts/`.
* Build, upload, and run a mock Bedrock batch job.
* Output results to `output/master_consolidated.csv`.

### 2. Run Individual Stages in Production
If you want to run the pipeline incrementally:

* **Stage 1: Oracle Database Extraction**
  ```bash
  python main.py extract
  ```
  *Queries Oracle and populates the local state database with IDENTIFIED UUIDs.*

* **Stage 2: S3 Transcript Retrieval & Bedrock Input Generation**
  ```bash
  python main.py prepare
  ```
  *Retrieves transcripts from S3, assembles the prompt JSONL files, uploads them to the input prefix, and marks records as BATCHED.*

* **Stage 3: Submit Bedrock Job & Poll Status**
  ```bash
  python main.py inference
  ```
  *Submits the S3 input folder to Bedrock Batch inference, links records in the DB to the Job ID, and waits until the job completes.*

* **Stage 4: Download Outputs and Compile master CSV**
  ```bash
  python main.py consolidate
  ```
  *Downloads JSONL output files, parses Claude's response fields, caches them, and exports everything to output/master_consolidated.csv.*

### 3. Run Full Production Pipeline End-to-End
To run all stages sequentially:
```bash
python main.py run-all
```

---

## Verification & Testing
To run the automated unit/integration test suite:
```bash
pytest tests/test_pipeline.py
```
