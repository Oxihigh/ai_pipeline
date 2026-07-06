import sqlite3
import contextlib
from datetime import datetime
from typing import List, Dict, Tuple, Any

class StateManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_db()

    @contextlib.contextmanager
    def _get_connection(self):
        """Context manager that yields a sqlite3 connection, commits on success, and closes it on exit."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_db(self):
        """Initializes the database schema if it does not exist."""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS transcripts (
                    intac_uuid TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    db_extracted_at DATETIME NOT NULL,
                    batched_at DATETIME,
                    completed_at DATETIME,
                    batch_job_id TEXT,
                    batch_file_name TEXT,
                    error_message TEXT,
                    ai_response TEXT
                )
            """)
            
            # Dynamically add the new columns if they do not exist
            cols = [
                ("loan_number", "TEXT"),
                ("latest_escrow_call_dt", "TEXT"),
                ("long_comment_code", "TEXT"),
                ("long_comment_date", "TEXT"),
                ("long_comment_user_id", "TEXT"),
                ("full_comment", "TEXT"),
                ("comment_actv_flg", "TEXT"),
                ("db_intac_uuid", "TEXT")
            ]
            for col_name, col_type in cols:
                try:
                    conn.execute(f"ALTER TABLE transcripts ADD COLUMN {col_name} {col_type}")
                except sqlite3.OperationalError:
                    # Column already exists
                    pass
                    
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON transcripts(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_batch_job ON transcripts(batch_job_id)")
            conn.commit()

    def add_identified_records(self, records: List[Any]) -> int:
        """
        Inserts new records in 'IDENTIFIED' status.
        Ignores duplicates to prevent overwriting existing progress.
        Returns the number of new records inserted.
        """
        now = datetime.now().isoformat()
        inserted = 0
        with self._get_connection() as conn:
            for record in records:
                if isinstance(record, str):
                    uuid = record
                    rec_dict = {"intac_uuid": uuid, "loan_number": uuid, "db_intac_uuid": uuid}
                else:
                    uuid = record["intac_uuid"]
                    rec_dict = record
                    
                try:
                    cursor = conn.execute(
                        """
                        INSERT OR IGNORE INTO transcripts (
                            intac_uuid, status, db_extracted_at, loan_number,
                            latest_escrow_call_dt, long_comment_code, long_comment_date,
                            long_comment_user_id, full_comment, comment_actv_flg,
                            db_intac_uuid
                        ) VALUES (?, 'IDENTIFIED', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            uuid, now, rec_dict.get("loan_number"),
                            rec_dict.get("latest_escrow_call_dt"), rec_dict.get("long_comment_code"),
                            rec_dict.get("long_comment_date"), rec_dict.get("long_comment_user_id"),
                            rec_dict.get("full_comment"), rec_dict.get("comment_actv_flg"),
                            rec_dict.get("db_intac_uuid")
                        )
                    )
                    if cursor.rowcount > 0:
                        inserted += 1
                except sqlite3.Error:
                    pass
            conn.commit()
        return inserted

    def get_records_by_status(self, status: str, limit: int = None) -> List[Dict[str, Any]]:
        """Retrieves records by status. Useful for picking up work."""
        query = "SELECT * FROM transcripts WHERE status = ?"
        params = [status]
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
            
        with self._get_connection() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def mark_records_as_batched(self, uuids: List[str], batch_job_id: str, batch_file_name: str):
        """Updates records status to BATCHED and records the associated batch job metadata."""
        now = datetime.now().isoformat()
        with self._get_connection() as conn:
            # Chunking updates if needed, though SQLite handles thousands easily
            conn.executemany(
                """
                UPDATE transcripts 
                SET status = 'BATCHED', batched_at = ?, batch_job_id = ?, batch_file_name = ?
                WHERE intac_uuid = ?
                """,
                [(now, batch_job_id, batch_file_name, uuid) for uuid in uuids]
            )
            conn.commit()

    def mark_records_as_completed(self, records: List[Tuple[str, str]]):
        """
        Marks records as completed.
        records: List of tuples (intac_uuid, ai_response)
        """
        now = datetime.now().isoformat()
        with self._get_connection() as conn:
            conn.executemany(
                """
                UPDATE transcripts 
                SET status = 'COMPLETED', completed_at = ?, ai_response = ?, error_message = NULL
                WHERE intac_uuid = ?
                """,
                [(now, ai_response, uuid) for uuid, ai_response in records]
            )
            conn.commit()

    def mark_records_as_failed(self, uuids: List[str], error_message: str):
        """Marks records as failed with the given error message."""
        now = datetime.now().isoformat()
        with self._get_connection() as conn:
            conn.executemany(
                """
                UPDATE transcripts 
                SET status = 'FAILED', error_message = ?
                WHERE intac_uuid = ?
                """,
                [(error_message, uuid) for uuid in uuids]
            )
            conn.commit()

    def reset_failed_records(self) -> int:
        """Resets failed records back to IDENTIFIED so they can be prepared again."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "UPDATE transcripts SET status = 'IDENTIFIED', error_message = NULL WHERE status = 'FAILED'"
            )
            count = cursor.rowcount
            conn.commit()
            return count

    def reset_batch(self, batch_job_id: str) -> int:
        """Resets records in a specific batch back to IDENTIFIED status (e.g. if the batch job failed)."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE transcripts 
                SET status = 'IDENTIFIED', batch_job_id = NULL, batch_file_name = NULL, error_message = NULL 
                WHERE batch_job_id = ?
                """,
                (batch_job_id,)
            )
            count = cursor.rowcount
            conn.commit()
            return count

    def get_stats(self) -> Dict[str, int]:
        """Returns statistics on record counts by status."""
        stats = {"IDENTIFIED": 0, "BATCHED": 0, "COMPLETED": 0, "FAILED": 0, "TOTAL": 0}
        with self._get_connection() as conn:
            rows = conn.execute("SELECT status, COUNT(*) as cnt FROM transcripts GROUP BY status").fetchall()
            total = 0
            for row in rows:
                stats[row["status"]] = row["cnt"]
                total += row["cnt"]
            stats["TOTAL"] = total
        return stats
