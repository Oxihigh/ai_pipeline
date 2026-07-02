import oracledb
import logging
from typing import List
from config import PipelineConfig

logger = logging.getLogger(__name__)

class DBClient:
    def __init__(self, config: PipelineConfig):
        self.config = config

    def extract_escrow_uuids(self) -> List[str]:
        """
        Connects to the Oracle database using python-oracledb in Thin mode
        and runs the configured SQL query to retrieve INTAC_UUIDs.
        
        If config.mock_db is True, it will bypass Oracle and return a set of mock UUIDs.
        """
        if self.config.mock_db:
            logger.info("Mock Database mode enabled. Generating 100 mock UUIDs for validation.")
            return [f"mock-uuid-{i:04d}" for i in range(1, 101)]

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
                
                # Fetch all rows. If there are 70,000+ rows, fetchall is fine because
                # we are only storing strings of UUIDs (approx 3MB in RAM).
                rows = cursor.fetchall()
                
                # Extract UUID from the first column of the query results
                uuids = [str(row[0]).strip() for row in rows if row[0] is not None]
                logger.info(f"Retrieved {len(uuids)} qualifying transcript UUIDs from Oracle DB.")
                return uuids
                
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
