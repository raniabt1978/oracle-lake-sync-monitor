# app/data_problems/volume_issues.py
import sqlite3
import random
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path

# Add app directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from config_paths import get_db_path

class VolumeIssueInjector:
    def __init__(self, db_path=None, use_real_oracle=False):
        self.db_path = db_path or get_db_path()
        self.use_real_oracle = use_real_oracle
        
        if self.use_real_oracle:
            from connectors.oracle_source import OracleConnector
            self.oracle_connector = OracleConnector()
        
    def get_oracle_count(self):
        """Get Oracle count - real or simulated"""
        if self.use_real_oracle:
            return self.oracle_connector.get_table_count('HR.EMPLOYEES')
        else:
            # Simulated Oracle count
            return 107  # Standard HR.EMPLOYEES count
        
    def create_sync_gap(self, target_gap_percent=15):
        """Delete random employees to create gap between Oracle and Hive"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get Oracle count
        oracle_count = self.get_oracle_count()
        print(f"📊 Oracle count: {oracle_count} employees")
        
        # Get current Hive (SQLite) count
        current_count = cursor.execute("SELECT COUNT(*) FROM employees_partitioned").fetchone()[0]
        print(f"📊 Hive current count: {current_count} employees")
        
        # Calculate how many to delete
        target_count = int(oracle_count * (1 - target_gap_percent/100))
        to_delete = current_count - target_count
        
        if to_delete <= 0:
            print(f"⚠️  Already at or below target gap. No deletion needed.")
            conn.close()
            return
        
        print(f"📉 Creating {target_gap_percent}% sync gap...")
        print(f"   Target Hive count: {target_count}")
        print(f"   Will delete: {to_delete} employees")
        
        # Get random employee IDs to delete (prefer recent hires to simulate lag)
        cursor.execute("""
            SELECT employee_id 
            FROM employees_partitioned 
            ORDER BY hire_date DESC, RANDOM() 
            LIMIT ?
        """, (to_delete,))
        ids_to_delete = [row[0] for row in cursor.fetchall()]
        
        # Delete them
        cursor.executemany("DELETE FROM employees_partitioned WHERE employee_id = ?", 
                          [(emp_id,) for emp_id in ids_to_delete])
        
        # Verify final count
        final_count = cursor.execute("SELECT COUNT(*) FROM employees_partitioned").fetchone()[0]
        actual_gap = ((oracle_count - final_count) / oracle_count) * 100
        
        conn.commit()
        conn.close()
        
        print(f"✅ Deleted {len(ids_to_delete)} employees")
        print(f"   Final Hive count: {final_count}")
        print(f"   Actual gap: {actual_gap:.1f}%")
        
    def create_missing_partitions(self, num_days=2):
        """Remove entire days of data to simulate missing partitions"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        print(f"\n🗓️  Creating {num_days} missing partition days...")
        
        # Find recent days with data (last 30 days)
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        cursor.execute("""
            SELECT DISTINCT year, month, day, COUNT(*) as cnt
            FROM employees_partitioned 
            WHERE hire_date >= ?
            GROUP BY year, month, day
            HAVING cnt > 0
            ORDER BY year DESC, month DESC, day DESC
        """, (thirty_days_ago,))
        
        recent_days = cursor.fetchall()
        
        if len(recent_days) < num_days:
            print(f"⚠️  Only {len(recent_days)} days available, adjusting to delete {len(recent_days)} days")
            num_days = len(recent_days)
        
        if num_days > 0:
            # Delete data from random recent days
            days_to_delete = random.sample(recent_days, num_days)
            
            total_deleted = 0
            for year, month, day, count in days_to_delete:
                cursor.execute("""
                    DELETE FROM employees_partitioned 
                    WHERE year = ? AND month = ? AND day = ?
                """, (year, month, day))
                print(f"   Deleted {count} records from {year}-{month:02d}-{day:02d}")
                total_deleted += count
            
            print(f"✅ Created {num_days} missing partitions (removed {total_deleted} records)")
        else:
            print("⚠️  No recent partitions found to delete")
        
        conn.commit()
        conn.close()
        
    def create_stuck_partition(self, days_ago=3):
        """Simulate a partition that stopped receiving updates"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        print(f"\n🔒 Creating stuck partition (no updates for {days_ago} days)...")
        
        # Find a partition from days_ago
        target_date = datetime.now() - timedelta(days=days_ago)
        
        # Update load_timestamp for some records to simulate they're stuck
        cursor.execute("""
            UPDATE employees_partitioned 
            SET load_timestamp = ?
            WHERE year = ? AND month = ? AND day = ?
        """, (
            target_date.isoformat(),
            target_date.year,
            target_date.month,
            target_date.day
        ))
        
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        print(f"✅ Made {affected} records appear stuck from {target_date.date()}")
        
    def create_duplicate_records(self, num_duplicates=5):
        """Create duplicate employee records"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        print(f"\n👥 Creating {num_duplicates} duplicate records...")
        
        # Get random employees to duplicate
        cursor.execute("""
            SELECT * FROM employees_partitioned 
            ORDER BY RANDOM() 
            LIMIT ?
        """, (num_duplicates,))
        
        employees_to_dup = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        
        duplicated = 0
        for emp in employees_to_dup:
            emp_dict = dict(zip(column_names, emp))
            # Change employee_id to make it unique
            emp_dict['employee_id'] = emp_dict['employee_id'] + 1000
            emp_dict['is_duplicate'] = 1
            emp_dict['load_timestamp'] = datetime.now().isoformat()
            
            columns = ', '.join(emp_dict.keys())
            placeholders = ', '.join(['?' for _ in emp_dict])
            
            try:
                cursor.execute(f"""
                    INSERT INTO employees_partitioned ({columns})
                    VALUES ({placeholders})
                """, list(emp_dict.values()))
                duplicated += 1
                print(f"   Duplicated employee {emp[1]} {emp[2]}")
            except sqlite3.IntegrityError:
                print(f"   Skipped duplicate for employee_id {emp_dict['employee_id']} (already exists)")
        
        conn.commit()
        conn.close()
        
        print(f"✅ Created {duplicated} duplicate records")

# Convenience function
def inject_all_volume_issues(db_path='data/demo_hive.db', use_real_oracle=False):
    """Run all volume issue injections"""
    print("🚀 Injecting all volume issues...\n")
    
    injector = VolumeIssueInjector(db_path, use_real_oracle)
    
    # Create various issues
    injector.create_sync_gap(target_gap_percent=15)
    injector.create_missing_partitions(num_days=2)
    injector.create_stuck_partition(days_ago=3)
    injector.create_duplicate_records(num_duplicates=5)
    
    print("\n✅ All volume issues injected!")

if __name__ == "__main__":
    # Run all injections when script is run directly
    inject_all_volume_issues()