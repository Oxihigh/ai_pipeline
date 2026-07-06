import oracledb
import logging
from typing import List, Dict, Any
from config import PipelineConfig

logger = logging.getLogger(__name__)

class DBClient:
    def __init__(self, config: PipelineConfig):
        self.config = config

    def extract_escrow_uuids(self) -> List[Dict[str, Any]]:
        """
        Connects to the Oracle database using python-oracledb in Thin mode
        and runs the configured SQL query to retrieve transcript rows.
        
        If config.mock_db is True, it will bypass Oracle and return a set of mock records.
        """
        if self.config.mock_db:
            logger.info("Mock Database mode enabled. Generating 100 mock records for validation.")
            return [
                {
                    "intac_uuid": f"mock-uuid-{i:04d}",
                    "loan_number": f"mock-uuid-{i:04d}",
                    "latest_escrow_call_dt": "2026-07-02T10:00:00",
                    "long_comment_code": "ESCROW",
                    "long_comment_date": f"2026-07-02T10:{i:02d}:00",
                    "long_comment_user_id": "MOCK_USER",
                    "full_comment": "This is a mock full comment containing details for validation.",
                    "comment_actv_flg": "Y"
                }
                for i in range(1, 101)
            ]

        dsn = f"{self.config.db_host}:{self.config.db_port}/{self.config.db_service_name}"
        logger.info(f"Connecting to Oracle DB at {self.config.db_host}:{self.config.db_port} as {self.config.db_user}...")
        
        connection = None
        try:
            # Connect in default 'Thin' mode - no Oracle client software is required.
            connection = oracledb.connect(
                user=self.config.db_user,
                password=self.config.db_password,
                dsn=dsn
            )
            logger.info("Successfully connected to Oracle DB.")
            
            with connection.cursor() as cursor:
                logger.info(f"Running extraction query: {self.config.db_query}")
                cursor.execute(self.config.db_query)
                
                rows = cursor.fetchall()
                
                # Retrieve column names
                colnames = []
                if cursor.description:
                    colnames = [col[0].upper() for col in cursor.description]
                
                records = []
                for row in rows:
                    if not row:
                        continue
                    if len(row) == 1:
                        # Simple one-column query fallback (e.g. INTAC_UUID)
                        val = str(row[0]).strip() if row[0] is not None else None
                        if val:
                            records.append({
                                "intac_uuid": val,
                                "loan_number": val
                            })
                    else:
                        # Multi-column comment query matching the new SQL
                        row_dict = dict(zip(colnames, row))
                        
                        loan_number = str(row_dict.get("LOAN_NUMBER") or "").strip()
                        comment_date = str(row_dict.get("LONG_COMMENT_DATE") or "").strip()
                        intac_uuid = str(row_dict.get("INTAC_UUID") or "").strip()
                        
                        # Generate unique identifier for this comment row using INTAC_UUID
                        if intac_uuid and comment_date:
                            uuid = f"{intac_uuid}_{comment_date}"
                        elif intac_uuid:
                            uuid = intac_uuid
                        elif loan_number and comment_date:
                            uuid = f"{loan_number}_{comment_date}"
                        elif loan_number:
                            uuid = loan_number
                        else:
                            continue
                            
                        records.append({
                            "intac_uuid": uuid,
                            "loan_number": loan_number,
                            "db_intac_uuid": intac_uuid or loan_number,
                            "latest_escrow_call_dt": str(row_dict.get("LATEST_ESCROW_CALL_DT") or "").strip(),
                            "long_comment_code": str(row_dict.get("LONG_COMMENT_CODE") or "").strip(),
                            "long_comment_date": comment_date,
                            "long_comment_user_id": str(row_dict.get("LONG_COMMENT_USER_ID") or "").strip(),
                            "full_comment": str(row_dict.get("FULL_COMMENT") or "").strip(),
                            "comment_actv_flg": str(row_dict.get("COMMENT_ACTV_FLG") or "").strip()
                        })
                        
                logger.info(f"Retrieved {len(records)} qualifying transcript records from Oracle DB.")
                return records
                
        except Exception as e:
            logger.error(f"Error during Oracle DB query execution: {e}")
            raise e
        finally:
            if connection:
                try:
                    connection.close()
                    logger.info("Oracle DB connection closed.")
                except Exception as e:
                    logger.warning(f"Error while closing database connection: {e}")
