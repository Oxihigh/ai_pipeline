import os
import shutil
import json
import csv
import pytest
from pathlib import Path

from config import PipelineConfig
from state_manager import StateManager
from db_client import DBClient
from s3_client import S3Client
from bedrock_client import BedrockClient
from parser import ResultParser

# Set up test paths
TEST_DB_PATH = "test_pipeline.db"
TEST_CSV_PATH = "test_output/master_consolidated.csv"
TEST_PROMPT_PATH = "test_prompt.txt"

@pytest.fixture(autouse=True)
def setup_and_teardown():
    # Clean up test directories/files before
    for path in [TEST_DB_PATH, TEST_CSV_PATH, TEST_PROMPT_PATH]:
        if os.path.exists(path):
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
    if os.path.exists("mock_s3_bucket"):
        shutil.rmtree("mock_s3_bucket")
    if os.path.exists("test_output"):
        shutil.rmtree("test_output")
    if os.path.exists("data"):
        shutil.rmtree("data")
        
    # Write a dummy prompt template
    with open(TEST_PROMPT_PATH, "w") as f:
        f.write("System: Analyze\nTranscript:\n{transcript}")
        
    yield
    
    # Clean up after tests
    for path in [TEST_DB_PATH, TEST_CSV_PATH, TEST_PROMPT_PATH]:
        if os.path.exists(path):
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
    if os.path.exists("mock_s3_bucket"):
        shutil.rmtree("mock_s3_bucket")
    if os.path.exists("test_output"):
        shutil.rmtree("test_output")
    if os.path.exists("data"):
        shutil.rmtree("data")

def get_test_config():
    # Create configuration targeting mock mode
    os.environ["LOCAL_STATE_DB_PATH"] = TEST_DB_PATH
    os.environ["LOCAL_MASTER_CSV_PATH"] = TEST_CSV_PATH
    os.environ["PROMPT_TEMPLATE_PATH"] = TEST_PROMPT_PATH
    os.environ["S3_BUCKET_NAME"] = "test-bucket"
    os.environ["BEDROCK_ROLE_ARN"] = "arn:aws:iam::123:role/test"
    return PipelineConfig(mock_db=True, mock_s3=True)

def test_config_loading():
    config = get_test_config()
    assert config.mock_db is True
    assert config.mock_s3 is True
    assert config.local_state_db == TEST_DB_PATH
    assert config.local_master_csv == TEST_CSV_PATH
    assert config.load_prompt_template().startswith("System: Analyze")

def test_state_manager_transitions():
    config = get_test_config()
    sm = StateManager(config.local_state_db)
    
    # Verify initial stats
    stats = sm.get_stats()
    assert stats["TOTAL"] == 0
    
    # Add new identified records
    inserted = sm.add_identified_records(["uuid-1", "uuid-2"])
    assert inserted == 2
    
    # Try inserting duplicates
    inserted_dup = sm.add_identified_records(["uuid-1", "uuid-3"])
    assert inserted_dup == 1 # Only uuid-3 is new
    
    stats = sm.get_stats()
    assert stats["IDENTIFIED"] == 3
    assert stats["TOTAL"] == 3
    
    # Mark as batched
    sm.mark_records_as_batched(["uuid-1", "uuid-2"], "job-arn-123", "batch_1.jsonl")
    
    stats = sm.get_stats()
    assert stats["IDENTIFIED"] == 1
    assert stats["BATCHED"] == 2
    
    # Mark completed
    sm.mark_records_as_completed([("uuid-1", "AI response text for 1")])
    stats = sm.get_stats()
    assert stats["BATCHED"] == 1
    assert stats["COMPLETED"] == 1
    
    # Retrieve complete records directly and check response
    records = sm.get_records_by_status("COMPLETED")
    assert len(records) == 1
    assert records[0]["intac_uuid"] == "uuid-1"
    assert records[0]["ai_response"] == "AI response text for 1"

def test_s3_mock_client():
    config = get_test_config()
    s3 = S3Client(config)
    
    # Verify seed files
    assert os.path.exists("mock_s3_bucket/transcripts/mock-uuid-0001.txt")
    
    # Fetch content
    uuid, content, err = s3.fetch_transcript_content("mock-uuid-0001")
    assert err is None
    assert "sarah" in content.lower()
    
    # Parallel fetch
    results = s3.fetch_transcripts_parallel(["mock-uuid-0001", "mock-uuid-0002", "missing-uuid"])
    assert len(results) == 3
    
    # Map results by uuid for verification
    res_map = {r[0]: r for r in results}
    assert res_map["mock-uuid-0001"][2] is None
    assert res_map["missing-uuid"][2] is not None # Missing file error

def test_result_parser_and_csv_generation():
    config = get_test_config()
    sm = StateManager(config.local_state_db)
    s3 = S3Client(config)
    
    # Pre-populate records in DB
    sm.add_identified_records(["mock-uuid-0001", "mock-uuid-0002"])
    
    # Set them to COMPLETED manually
    mock_response = json.dumps({
        "call_reason": "Escrow Closing Inquiry",
        "customer_sentiment": "Positive",
        "main_escrow_issue": "None",
        "next_steps": "Wait for documents"
    })
    sm.mark_records_as_completed([
        ("mock-uuid-0001", mock_response),
        ("mock-uuid-0002", "Invalid JSON - General text analysis response")
    ])
    
    # Test CSV export
    parser = ResultParser(config, sm, s3)
    parser.export_to_master_csv()
    
    # Check CSV existence and content
    assert os.path.exists(TEST_CSV_PATH)
    
    with open(TEST_CSV_PATH, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) == 2
        
        # Verify first row parsed fields
        row1 = [r for r in reader if r["intac_uuid"] == "mock-uuid-0001"][0]
        assert row1["status"] == "COMPLETED"
        assert row1["call_reason"] == "Escrow Closing Inquiry"
        assert row1["customer_sentiment"] == "Positive"
        assert row1["main_escrow_issue"] == "None"
        assert row1["next_steps"] == "Wait for documents"
        assert row1["raw_ai_response"] == mock_response
        
        # Verify second row (fallback parsing for non-JSON string)
        row2 = [r for r in reader if r["intac_uuid"] == "mock-uuid-0002"][0]
        assert row2["status"] == "COMPLETED"
        assert row2["call_reason"] == "Invalid JSON - General text analysis response"
        assert row2["customer_sentiment"] == ""
