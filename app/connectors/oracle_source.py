# connectors/oracle_source.py
import os
import oracledb
from contextlib import contextmanager
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MonitoringConnectionError(Exception):
   """Custom exception for monitoring-specific connection issues"""
   pass

class OracleConnector:
   def __init__(self, 
                wallet_location: Optional[str] = None, 
                wallet_password: Optional[str] = None,
                connection_string: Optional[str] = None,
                use_mock: bool = None):
       """
       Initialize Oracle connector for sync monitoring
       
       Args:
           wallet_location: Path to the wallet directory
           wallet_password: Wallet password (required if wallet has password)
           connection_string: TNS alias or full connection string
           use_mock: Force mock mode (useful for demos)
       """
       # Check if we should use mock mode
       if use_mock is None:
           use_mock = os.getenv('USE_REAL_ORACLE', 'false').lower() != 'true'
       
       self.use_mock = use_mock
       
       if self.use_mock:
           logger.info("🔧 Using mock Oracle mode for demo")
           self.mock_count = int(os.getenv('ORACLE_MOCK_COUNT', '107'))  # HR.EMPLOYEES default
           return
       
       # Real Oracle setup (your existing code)
       self.wallet_location = wallet_location or os.getenv('TNS_ADMIN')
       if not self.wallet_location:
           raise ValueError("Wallet location not provided. Set TNS_ADMIN environment variable or pass wallet_location parameter.")
       
       self.wallet_password = wallet_password or os.getenv('WALLET_PASSWORD')
       if not self.wallet_password:
           logger.warning("No wallet password provided. This will fail if wallet is password-protected.")
       
       self.user = os.getenv('ORACLE_USER')
       self.password = os.getenv('ORACLE_PASSWORD')
       self.dsn = connection_string or os.getenv('ORACLE_DSN')
       
       if not self.dsn:
           raise ValueError("Oracle DSN not provided. Set ORACLE_DSN environment variable or pass connection_string parameter.")
       
       if not self.user or not self.password:
           raise ValueError("Oracle credentials not provided. Set ORACLE_USER and ORACLE_PASSWORD environment variables.")
       
       os.environ['TNS_ADMIN'] = self.wallet_location
       logger.info(f"✅ Initialized connector for {self.dsn} with wallet at {self.wallet_location}")
   
   @contextmanager
   def get_connection(self):
       """Context manager for database connections"""
       if self.use_mock:
           yield None  # No real connection in mock mode
           return
           
       conn = None
       try:
           conn = oracledb.connect(
               user=self.user,
               password=self.password,
               dsn=self.dsn,
               config_dir=self.wallet_location,
               wallet_location=self.wallet_location,
               wallet_password=self.wallet_password
           )
           yield conn
       except Exception as e:
           logger.error(f"Connection error: {str(e)}")
           if "ORA-12541" in str(e):
               raise MonitoringConnectionError("TNS:no listener - Check if Oracle is running")
           elif "ORA-01017" in str(e):
               raise MonitoringConnectionError("Invalid username/password")
           raise MonitoringConnectionError(f"Failed to connect for sync monitoring: {e}")
       finally:
           if conn:
               conn.close()
   
   def test_connection(self) -> bool:
       """Test database connection"""
       if self.use_mock:
           logger.info("✅ Mock connection test passed")
           return True
           
       try:
           with self.get_connection() as conn:
               cursor = conn.cursor()
               cursor.execute("SELECT 1 FROM DUAL")
               result = cursor.fetchone()
               cursor.close()
               return result[0] == 1
       except Exception as e:
           logger.error(f"Connection test failed: {e}")
           return False
   
   # ===== MONITORING-SPECIFIC METHODS =====
   
   def get_table_count(self, table_name: str, use_hint: bool = True) -> int:
       """
       Get row count for sync monitoring
       
       Args:
           table_name: Table to count (e.g., 'HR.EMPLOYEES')
           use_hint: Whether to use parallel hint for large tables
       """
       if self.use_mock:
           logger.info(f"📊 Mock count for {table_name}: {self.mock_count}")
           return self.mock_count
       
       with self.get_connection() as conn:
           cursor = conn.cursor()
           
           if use_hint:
               query = f"SELECT /*+ PARALLEL(4) */ COUNT(*) FROM {table_name}"
           else:
               query = f"SELECT COUNT(*) FROM {table_name}"
               
           cursor.execute(query)
           count = cursor.fetchone()[0]
           cursor.close()
           logger.info(f"📊 Real count for {table_name}: {count}")
           return count
   
   def get_latest_timestamp(self, table_name: str, timestamp_column: str) -> Optional[datetime]:
       """
       Get most recent record timestamp for freshness monitoring
       
       Args:
           table_name: Table name (e.g., 'HR.EMPLOYEES')
           timestamp_column: Column to check (e.g., 'HIRE_DATE' or 'LAST_UPDATE_DATE')
       """
       if self.use_mock:
           # Simulate 13-hour lag
           mock_ts = datetime.now() - timedelta(hours=13)
           logger.info(f"📅 Mock latest timestamp: {mock_ts}")
           return mock_ts
       
       with self.get_connection() as conn:
           cursor = conn.cursor()
           query = f"SELECT MAX({timestamp_column}) FROM {table_name}"
           cursor.execute(query)
           result = cursor.fetchone()[0]
           cursor.close()
           
           if result:
               logger.info(f"📅 Latest {timestamp_column} in {table_name}: {result}")
           return result
   
   def get_extraction_metadata(self) -> Dict[str, Any]:
       """Get Oracle-specific metadata for monitoring dashboard"""
       if self.use_mock:
           return {
               "source_type": "Oracle (Mock Mode)",
               "extraction_method": "Direct Query (Demo)",
               "timezone": "UTC",
               "nls_date_format": "YYYY-MM-DD HH24:MI:SS",
               "parallel_degree": 4,
               "connection_status": "mock"
           }
       
       metadata = {
           "source_type": "Oracle Database",
           "extraction_method": "Direct Query",
           "parallel_degree": 4,
           "connection_status": "connected"
       }
       
       try:
           with self.get_connection() as conn:
               cursor = conn.cursor()
               
               # Get version
               cursor.execute("SELECT BANNER FROM V$VERSION WHERE ROWNUM = 1")
               metadata["version"] = cursor.fetchone()[0]
               
               # Get timezone
               cursor.execute("SELECT DBTIMEZONE FROM DUAL")
               metadata["timezone"] = cursor.fetchone()[0]
               
               # Get NLS date format
               cursor.execute("SELECT VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER = 'NLS_DATE_FORMAT'")
               metadata["nls_date_format"] = cursor.fetchone()[0]
               
               cursor.close()
       except Exception as e:
           logger.warning(f"Could not fetch all metadata: {e}")
       
       return metadata
   
   def get_sample_data(self, table_name: str, limit: int = 5) -> list:
       """Get sample rows for data quality checks"""
       if self.use_mock:
           # Return mock employee data
           return [
               {"EMPLOYEE_ID": 100, "FIRST_NAME": "Steven", "HIRE_DATE": "2003-06-17"},
               {"EMPLOYEE_ID": 101, "FIRST_NAME": "Neena", "HIRE_DATE": "2005-09-21"},
               {"EMPLOYEE_ID": 102, "FIRST_NAME": "Lex", "HIRE_DATE": "2001-01-13"},
           ][:limit]
       
       with self.get_connection() as conn:
           cursor = conn.cursor()
           cursor.execute(f"SELECT * FROM {table_name} WHERE ROWNUM <= :1", [limit])
           columns = [col[0] for col in cursor.description]
           rows = cursor.fetchall()
           cursor.close()
           
           return [dict(zip(columns, row)) for row in rows]
   
   def check_table_exists(self, table_name: str) -> bool:
       """Verify if table exists (useful for initial setup)"""
       if self.use_mock:
           return True
       
       with self.get_connection() as conn:
           cursor = conn.cursor()
           # Parse schema and table name
           parts = table_name.upper().split('.')
           if len(parts) == 2:
               schema, table = parts
               query = """
                   SELECT COUNT(*) FROM ALL_TABLES 
                   WHERE OWNER = :1 AND TABLE_NAME = :2
               """
               cursor.execute(query, [schema, table])
           else:
               query = "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = :1"
               cursor.execute(query, [table_name.upper()])
           
           exists = cursor.fetchone()[0] > 0
           cursor.close()
           return exists

# Convenience function for backward compatibility
def get_oracle_count(table_name: str = None) -> int:
   """Quick function to get count without instantiating connector"""
   table_name = table_name or os.getenv('ORACLE_TABLE', 'HR.EMPLOYEES')
   connector = OracleConnector()
   return connector.get_table_count(table_name)