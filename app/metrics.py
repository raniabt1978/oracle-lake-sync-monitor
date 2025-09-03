# app/metrics.py
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from pathlib import Path

from config_paths import get_db_path
from connectors.oracle_source import OracleConnector

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SyncMetrics:
    """Calculate sync metrics between Oracle and Hive"""
    
    def __init__(self, oracle_connector: Optional[OracleConnector] = None):
        """
        Initialize metrics calculator
        
        Args:
            oracle_connector: Optional Oracle connector instance
        """
        self.db_path = get_db_path()
        self.oracle = oracle_connector or OracleConnector()
        self.oracle_table = 'HR.EMPLOYEES'
        
    def get_hive_count(self) -> int:
        """Get current count from Hive (SQLite)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            count = cursor.execute("SELECT COUNT(*) FROM employees_partitioned").fetchone()[0]
            return count
    
    def calculate_sync_gap(self) -> Dict[str, Any]:
        """Calculate the sync gap between Oracle and Hive"""
        try:
            # Get counts from both systems
            oracle_count = self.oracle.get_table_count(self.oracle_table)
            hive_count = self.get_hive_count()
            
            # Calculate gap
            gap_count = oracle_count - hive_count
            gap_percent = (gap_count / oracle_count * 100) if oracle_count > 0 else 0
            
            return {
                'oracle_count': oracle_count,
                'hive_count': hive_count,
                'gap_count': gap_count,
                'gap_percent': round(gap_percent, 2),
                'status': 'IN_SYNC' if gap_percent < 5 else 'OUT_OF_SYNC',
                'severity': self._get_severity(gap_percent)
            }
        except Exception as e:
            logger.error(f"Error calculating sync gap: {e}")
            return {
                'error': str(e),
                'status': 'ERROR',
                'severity': 'CRITICAL'
            }
    
    def detect_missing_partitions(self, days_to_check: int = 30) -> Dict[str, Any]:
        """Detect missing partitions in recent data"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get partition statistics for recent days
            start_date = (datetime.now() - timedelta(days=days_to_check)).strftime('%Y-%m-%d')
            
            cursor.execute("""
                SELECT 
                    year, month, day,
                    COUNT(*) as record_count,
                    MIN(hire_date) as min_date,
                    MAX(hire_date) as max_date
                FROM employees_partitioned
                WHERE hire_date >= ?
                GROUP BY year, month, day
                ORDER BY year DESC, month DESC, day DESC
            """, (start_date,))
            
            partitions = cursor.fetchall()
            
            # Detect gaps
            missing_partitions = []
            if partitions:
                # Check for days with zero records (completely missing)
                cursor.execute("""
                    SELECT DISTINCT DATE(hire_date) as partition_date
                    FROM employees_partitioned
                    WHERE hire_date >= ?
                    ORDER BY partition_date DESC
                """, (start_date,))
                
                existing_dates = {row[0] for row in cursor.fetchall()}
                
                # Generate expected dates
                current_date = datetime.now().date()
                check_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                
                while check_date <= current_date:
                    if check_date.strftime('%Y-%m-%d') not in existing_dates:
                        # Check if it's a business day
                        if check_date.weekday() < 5:  # Monday = 0, Sunday = 6
                            missing_partitions.append({
                                'date': check_date.strftime('%Y-%m-%d'),
                                'expected_records': 'Unknown'
                            })
                    check_date += timedelta(days=1)
            
            return {
                'total_partitions': len(partitions),
                'missing_count': len(missing_partitions),
                'missing_partitions': missing_partitions[:5],  # Top 5
                'status': 'HEALTHY' if len(missing_partitions) == 0 else 'MISSING_DATA',
                'severity': self._get_severity(len(missing_partitions) * 10)  # 10% per missing day
            }
    
    def check_data_freshness(self) -> Dict[str, Any]:
        """Check how fresh the data is in Hive"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get latest hire date and load timestamp
            cursor.execute("""
                SELECT 
                    MAX(hire_date) as latest_hire_date,
                    MAX(load_timestamp) as latest_load,
                    COUNT(*) as total_records
                FROM employees_partitioned
            """)
            
            result = cursor.fetchone()
            latest_hire = result[0]
            latest_load = result[1]
            
            if latest_hire:
                # Calculate lag
                latest_hire_dt = datetime.strptime(latest_hire, '%Y-%m-%d')
                lag_days = (datetime.now() - latest_hire_dt).days
                
                # For load timestamp
                if latest_load:
                    try:
                        latest_load_dt = datetime.fromisoformat(latest_load.replace('Z', '+00:00'))
                        load_lag_hours = (datetime.now() - latest_load_dt.replace(tzinfo=None)).total_seconds() / 3600
                    except:
                        load_lag_hours = None
                else:
                    load_lag_hours = None
                
                return {
                    'latest_hire_date': latest_hire,
                    'data_lag_days': lag_days,
                    'latest_load_timestamp': latest_load,
                    'load_lag_hours': round(load_lag_hours, 2) if load_lag_hours else None,
                    'status': 'CURRENT' if lag_days < 7 else 'STALE',
                    'severity': self._get_severity(lag_days * 5)  # 5% per day of lag
                }
            
            return {
                'status': 'NO_DATA',
                'severity': 'CRITICAL'
            }
    
    def detect_duplicates(self) -> Dict[str, Any]:
        """Detect duplicate records"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Method 1: Check for duplicate employee_ids (our injected duplicates)
            cursor.execute("""
                SELECT COUNT(*) 
                FROM employees_partitioned
                WHERE employee_id > 1000
            """)
            injected_dups = cursor.fetchone()[0]
            
            # Method 2: Check for actual duplicates by name and hire date
            cursor.execute("""
                SELECT 
                    first_name, last_name, hire_date, 
                    COUNT(*) as dup_count
                FROM employees_partitioned
                GROUP BY first_name, last_name, hire_date
                HAVING COUNT(*) > 1
            """)
            
            natural_dups = cursor.fetchall()
            
            # Get sample duplicates for display
            sample_dups = []
            if injected_dups > 0:
                cursor.execute("""
                    SELECT 
                        e1.employee_id, e1.first_name, e1.last_name,
                        e2.employee_id as dup_id
                    FROM employees_partitioned e1
                    JOIN employees_partitioned e2 
                        ON e1.first_name = e2.first_name 
                        AND e1.last_name = e2.last_name
                        AND e1.employee_id < e2.employee_id
                    WHERE e2.employee_id > 1000
                    LIMIT 3
                """)
                
                for row in cursor.fetchall():
                    sample_dups.append({
                        'original_id': row[0],
                        'duplicate_id': row[3],
                        'name': f"{row[1]} {row[2]}"
                    })
            
            total_duplicates = injected_dups + len(natural_dups)
            
            return {
                'duplicate_count': total_duplicates,
                'injected_duplicates': injected_dups,
                'natural_duplicates': len(natural_dups),
                'sample_duplicates': sample_dups,
                'status': 'CLEAN' if total_duplicates == 0 else 'DUPLICATES_FOUND',
                'severity': self._get_severity(total_duplicates * 5)  # 5% per duplicate
            }
    
    def check_stuck_partitions(self, hours_threshold: int = 24) -> Dict[str, Any]:
        """Detect partitions that haven't been updated recently"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            threshold_time = (datetime.now() - timedelta(hours=hours_threshold)).isoformat()
            
            # Find partitions with old load timestamps
            cursor.execute("""
                SELECT 
                    year, month, day,
                    COUNT(*) as record_count,
                    MAX(load_timestamp) as last_update
                FROM employees_partitioned
                GROUP BY year, month, day
                HAVING MAX(load_timestamp) < ?
                ORDER BY last_update ASC
                LIMIT 10
            """, (threshold_time,))
            
            stuck_partitions = []
            for row in cursor.fetchall():
                try:
                    last_update_dt = datetime.fromisoformat(row[4].replace('Z', '+00:00'))
                    hours_behind = (datetime.now() - last_update_dt.replace(tzinfo=None)).total_seconds() / 3600
                    
                    stuck_partitions.append({
                        'partition': f"{row[0]}-{row[1]:02d}-{row[2]:02d}",
                        'record_count': row[3],
                        'hours_behind': round(hours_behind, 1)
                    })
                except:
                    pass
            
            return {
                'stuck_count': len(stuck_partitions),
                'stuck_partitions': stuck_partitions[:5],  # Top 5
                'threshold_hours': hours_threshold,
                'status': 'HEALTHY' if len(stuck_partitions) == 0 else 'STUCK_PARTITIONS',
                'severity': self._get_severity(len(stuck_partitions) * 10)
            }
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics in one call"""
        return {
            'sync_gap': self.calculate_sync_gap(),
            'missing_partitions': self.detect_missing_partitions(),
            'data_freshness': self.check_data_freshness(),
            'duplicates': self.detect_duplicates(),
            'stuck_partitions': self.check_stuck_partitions(),
            'last_check': datetime.now().isoformat(),
            'overall_health': self._calculate_overall_health()
        }
    
    def _get_severity(self, metric_value: float) -> str:
        """Determine severity level based on metric value"""
        if metric_value >= 30:
            return 'CRITICAL'
        elif metric_value >= 15:
            return 'WARNING'
        elif metric_value >= 5:
            return 'MINOR'
        else:
            return 'OK'
    
    def _calculate_overall_health(self) -> str:
        """Calculate overall system health"""
        # This is a simplified version - you can make it more sophisticated
        metrics = {
            'sync': self.calculate_sync_gap(),
            'partitions': self.detect_missing_partitions(),
            'freshness': self.check_data_freshness(),
            'duplicates': self.detect_duplicates()
        }
        
        severities = [m.get('severity', 'OK') for m in metrics.values()]
        
        if 'CRITICAL' in severities:
            return 'CRITICAL'
        elif 'WARNING' in severities:
            return 'WARNING'
        elif 'MINOR' in severities:
            return 'MINOR'
        else:
            return 'HEALTHY'

# Test function
def test_metrics():
    """Test the metrics module"""
    print("🔍 Testing Sync Metrics...\n")
    
    metrics = SyncMetrics()
    
    # Test individual metrics
    print("1. SYNC GAP:")
    sync_gap = metrics.calculate_sync_gap()
    print(f"   Oracle: {sync_gap.get('oracle_count', 'N/A')} records")
    print(f"   Hive: {sync_gap.get('hive_count', 'N/A')} records")
    print(f"   Gap: {sync_gap.get('gap_percent', 'N/A')}%")
    print(f"   Status: {sync_gap.get('status', 'N/A')}")
    
    print("\n2. MISSING PARTITIONS:")
    missing = metrics.detect_missing_partitions()
    print(f"   Missing: {missing.get('missing_count', 'N/A')} partitions")
    print(f"   Status: {missing.get('status', 'N/A')}")
    
    print("\n3. DATA FRESHNESS:")
    freshness = metrics.check_data_freshness()
    print(f"   Latest data: {freshness.get('latest_hire_date', 'N/A')}")
    print(f"   Lag: {freshness.get('data_lag_days', 'N/A')} days")
    print(f"   Status: {freshness.get('status', 'N/A')}")
    
    print("\n4. DUPLICATES:")
    dups = metrics.detect_duplicates()
    print(f"   Found: {dups.get('duplicate_count', 'N/A')} duplicates")
    print(f"   Status: {dups.get('status', 'N/A')}")
    
    print("\n5. OVERALL HEALTH:")
    print(f"   {metrics._calculate_overall_health()}")

if __name__ == "__main__":
    test_metrics()