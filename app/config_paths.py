# app/config_paths.py
from pathlib import Path

# Project root is parent of 'app' directory
PROJECT_ROOT = Path(__file__).parent.parent.resolve()

# Define all paths relative to project root
PATHS = {
    'db': PROJECT_ROOT / 'data' / 'demo_hive.db',
    'schema': PROJECT_ROOT / 'app' / 'models' / 'schema.sql',
    'data_dir': PROJECT_ROOT / 'data',
    'app_dir': PROJECT_ROOT / 'app',
}

def get_db_path():
    """Get database path, create data directory if needed"""
    PATHS['data_dir'].mkdir(exist_ok=True)
    return str(PATHS['db'])