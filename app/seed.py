# app/seed.py
import sqlite3
import random
from datetime import datetime, timedelta
import os
from pathlib import Path
from config_paths import get_db_path, PATHS

# Realistic HCM data
DEPARTMENTS = {
    10: {'name': 'Administration', 'manager_id': 200},
    20: {'name': 'Marketing', 'manager_id': 201},
    30: {'name': 'Purchasing', 'manager_id': 114},
    40: {'name': 'Human Resources', 'manager_id': 203},
    50: {'name': 'Shipping', 'manager_id': 121},
    60: {'name': 'IT', 'manager_id': 103},
    70: {'name': 'Public Relations', 'manager_id': 204},
    80: {'name': 'Sales', 'manager_id': 145},
    90: {'name': 'Executive', 'manager_id': 100},
    100: {'name': 'Finance', 'manager_id': 108},
    110: {'name': 'Accounting', 'manager_id': 205}
}

# Real Oracle HR job structure with salary ranges
JOBS = {
    'AD_PRES': {'title': 'President', 'min_sal': 20000, 'max_sal': 40000},
    'AD_VP': {'title': 'Administration Vice President', 'min_sal': 15000, 'max_sal': 30000},
    'IT_PROG': {'title': 'Programmer', 'min_sal': 4000, 'max_sal': 10000},
    'FI_MGR': {'title': 'Finance Manager', 'min_sal': 8200, 'max_sal': 16000},
    'FI_ACCOUNT': {'title': 'Accountant', 'min_sal': 4200, 'max_sal': 9000},
    'SA_MAN': {'title': 'Sales Manager', 'min_sal': 10000, 'max_sal': 20000},
    'SA_REP': {'title': 'Sales Representative', 'min_sal': 6000, 'max_sal': 12000},
    'ST_MAN': {'title': 'Stock Manager', 'min_sal': 5500, 'max_sal': 8500},
    'ST_CLERK': {'title': 'Stock Clerk', 'min_sal': 2000, 'max_sal': 5000},
    'HR_REP': {'title': 'Human Resources Representative', 'min_sal': 4000, 'max_sal': 9000}
}

# Realistic first and last names
FIRST_NAMES = [
    'James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda',
    'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
    'Thomas', 'Sarah', 'Charles', 'Karen', 'Christopher', 'Nancy', 'Daniel', 'Lisa'
]

LAST_NAMES = [
    'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
    'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
    'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson'
]

class HiveDataSeeder:
    def __init__(self, db_path='../data/demo_hive.db'):
        """Initialize seeder with database path"""
        self.db_path = db_path
        self.conn = None
        
    def setup_database(self):
        """Create database and run schema"""
        print("🚀 Setting up database...")
        
        # Connect to database (creates file if doesn't exist)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Enable column access by name
    
        # Read and execute schema using PATHS
        schema_path = PATHS['schema']
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
        
        # Execute schema
        self.conn.executescript(schema_sql)
        self.conn.commit()
        print("✅ Database schema created")
        
    def clear_existing_data(self):
        """Clean slate - remove existing data"""
        print("🧹 Clearing existing data...")
        tables = ['employees_partitioned', 'departments', 'audit_runs', 'data_quality_issues']
        
        cursor = self.conn.cursor()
        for table in tables:
            cursor.execute(f"DELETE FROM {table}")
        self.conn.commit()
        print("✅ Existing data cleared")
        
    def seed_departments(self):
        """Insert department data"""
        print("🏢 Seeding departments...")
        cursor = self.conn.cursor()
        
        for dept_id, dept_info in DEPARTMENTS.items():
            cursor.execute("""
                INSERT INTO departments (department_id, department_name, manager_id, location_id)
                VALUES (?, ?, ?, ?)
            """, (dept_id, dept_info['name'], dept_info['manager_id'], 1700))  # 1700 = Seattle
            
        self.conn.commit()
        print(f"✅ Inserted {len(DEPARTMENTS)} departments")

    def generate_realistic_employee(self, emp_id, hire_date):
        """Generate one employee with HCM-realistic data"""
        # Random name
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        email = f"{first_name.lower()}.{last_name.lower()}@company.com"
        
        # Assign to department (weighted - more in Sales and IT)
        dept_weights = {
            60: 0.2,   # IT - 20%
            80: 0.25,  # Sales - 25%
            50: 0.15,  # Shipping - 15%
            100: 0.1,  # Finance - 10%
            30: 0.1,   # Purchasing - 10%
        }
        
        rand = random.random()
        cumulative = 0
        department_id = 10  # Default
        
        for dept, weight in [(60, 0.2), (80, 0.25), (50, 0.15), (100, 0.1), (30, 0.1)]:
            cumulative += weight
            if rand < cumulative:
                department_id = dept
                break
        else:
            department_id = random.choice([10, 20, 40, 70, 90, 110])
        
        # Assign job based on department
        if department_id == 60:  # IT
            job_id = 'IT_PROG'
        elif department_id == 80:  # Sales
            job_id = random.choice(['SA_MAN', 'SA_REP']) if emp_id % 5 == 0 else 'SA_REP'
        elif department_id == 100:  # Finance
            job_id = 'FI_MGR' if emp_id % 10 == 0 else 'FI_ACCOUNT'
        else:
            job_id = random.choice(['ST_CLERK', 'HR_REP', 'ST_MAN'])
        
        # Realistic salary based on job and experience
        job_info = JOBS[job_id]
        base_salary = random.uniform(job_info['min_sal'], job_info['max_sal'])
        
        # Add experience factor (longer tenure = higher salary)
        years_employed = (datetime.now() - hire_date).days / 365
        experience_multiplier = 1 + (years_employed * 0.03)  # 3% per year
        salary = round(base_salary * experience_multiplier, 2)
        
        # Manager assignment (executives manage managers, managers manage employees)
        if job_id in ['AD_PRES', 'AD_VP']:
            manager_id = None  # Top level
        elif 'MGR' in job_id or 'MAN' in job_id:
            manager_id = DEPARTMENTS[department_id]['manager_id']
        else:
            # Regular employees report to department manager
            manager_id = DEPARTMENTS[department_id]['manager_id']
        
        # Commission only for sales
        commission_pct = round(random.uniform(0.1, 0.3), 2) if department_id == 80 else None
        
        # Phone number (US format)
        phone = f"{random.randint(200,999)}.{random.randint(100,999)}.{random.randint(1000,9999)}"
        
        return {
            'employee_id': emp_id,
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'phone_number': phone,
            'hire_date': hire_date.strftime('%Y-%m-%d'),
            'job_id': job_id,
            'salary': salary,
            'commission_pct': commission_pct,
            'manager_id': manager_id,
            'department_id': department_id
        }
    
    def is_business_day(self, date):
        """Check if date is a business day (Mon-Fri, not a holiday)"""
        # Skip weekends
        if date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False
            
        # Skip major US holidays (simplified)
        holidays = [
            (1, 1),   # New Year
            (7, 4),   # July 4th
            (12, 25), # Christmas
            (12, 24), # Christmas Eve
            (11, 25), # Around Thanksgiving
        ]
        
        for month, day in holidays:
            if date.month == month and date.day == day:
                return False
                
        return True
    
    def seed_employees(self, total_count=107):
        """Seed employees with realistic patterns"""
        print(f"👥 Seeding {total_count} employees...")
        cursor = self.conn.cursor()
        
        # Generate hire dates over past 20 years with realistic patterns
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365*20)  # 20 years of history
        
        employees_created = 0
        emp_id = 100  # Start from 100 like Oracle HR
        
        while employees_created < total_count:
            # Random hire date
            days_ago = random.randint(0, (end_date - start_date).days)
            hire_date = end_date - timedelta(days=days_ago)
            
            # Only hire on business days
            if not self.is_business_day(hire_date):
                continue
                
            # Generate employee
            emp = self.generate_realistic_employee(emp_id, hire_date)
            
            # Calculate partition columns
            year = hire_date.year
            month = hire_date.month
            day = hire_date.day
            
            # Insert with partition info
            cursor.execute("""
                INSERT INTO employees_partitioned (
                    employee_id, first_name, last_name, email, phone_number,
                    hire_date, job_id, salary, commission_pct, manager_id,
                    department_id, year, month, day, load_timestamp, source_system
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                emp['employee_id'], emp['first_name'], emp['last_name'], 
                emp['email'], emp['phone_number'], emp['hire_date'],
                emp['job_id'], emp['salary'], emp['commission_pct'],
                emp['manager_id'], emp['department_id'],
                year, month, day,
                datetime.now().isoformat(), 'ORACLE_HR'
            ))
            
            employees_created += 1
            emp_id += 1
            
        self.conn.commit()
        print(f"✅ Created {employees_created} employees with realistic HCM data")

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("🔒 Database connection closed")

def main():
    """Main execution function"""
    print("\n🌟 Starting Hive Data Seeding Process\n")
    
    seeder = HiveDataSeeder()
    
    try:
        # Setup
        seeder.setup_database()
        seeder.clear_existing_data()
        
        # Seed data
        seeder.seed_departments()
        seeder.seed_employees(total_count=107)  # Start with full 107 to match Oracle
        
        # Summary
        print("\n📊 Seeding Summary:")
        print("- Database: data/demo_hive.db")
        print("- Employees: 107 (matching Oracle)")
        print("- Departments: 11")
        print("- Ready for problem injection!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        seeder.close()

if __name__ == "__main__":
    main()
