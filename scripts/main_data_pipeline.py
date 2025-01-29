"""
Product Data Pipeline v4.0

This script is designed to generate, clean, and store product data 
for a platform I made up.

Please install dependencies via CLI, before running:

pip install -r requirements.txt

requirements.txt file can be found in the root directory of the 
project on GitHub for download.

"""

# Standard library imports
import logging
import os
import random
import sys
from datetime import datetime as dt
from datetime import timedelta
from pathlib import Path
import pandas as pd
from typing import Dict, Union, List, Any
import time

# Third-party imports
import duckdb
import minio
from dotenv import load_dotenv
from faker import Faker
from dbt.cli.main import dbtRunner

# Local imports
from analytics_queries import (
    run_lifecycle_analysis,
    run_purchase_analysis,
    run_demographics_analysis,
    run_business_analysis,
    run_engagement_analysis,
    run_churn_analysis,
    run_session_analysis,
    run_funnel_analysis,
    save_analysis_results
)

# Initialize paths and configuration
PROJECT_ROOT = Path(__file__).parents[1]
load_dotenv(dotenv_path=PROJECT_ROOT / 'pdp_config.env')

# Configure logging
log_dir = Path('logs')
log_dir.mkdir(parents=True, exist_ok=True)

# Set logging level
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.getenv('LOG_FILE', 'logs/generate_fake_data.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# Constants
DEFAULT_NUM_ROWS = int(os.getenv('DEFAULT_NUM_ROWS', 8000))
START_DATETIME = dt.strptime(os.getenv('START_DATETIME', '2022-01-01 10:30'), '%Y-%m-%d %H:%M')
END_DATETIME = dt.strptime(os.getenv('END_DATETIME', '2024-12-31 23:59'), '%Y-%m-%d %H:%M')
VALID_STATUSES = os.getenv('VALID_STATUSES', 'pending,completed,failed').split(',')

# Random seed configuration
fake = Faker()
Faker.seed(int(os.getenv('FAKER_SEED', 42)))
random.seed(int(os.getenv('RANDOM_SEED', 42)))

# Database configuration
DBT_ROOT = PROJECT_ROOT / 'dbt_pipeline_demo'
db_dir = DBT_ROOT / 'databases'
db_dir.mkdir(parents=True, exist_ok=True)
db_path = db_dir / 'dbt_pipeline_demo.duckdb'

# S3 Configuration
S3_CONFIG: Dict[str, Union[str, bool]] = {
    'endpoint': os.getenv('MINIO_ENDPOINT'),
    'access_key': os.getenv('MINIO_ACCESS_KEY'),
    'secret_key': os.getenv('MINIO_SECRET_KEY'),
    'bucket': os.getenv('MINIO_BUCKET_NAME'),
    'use_ssl': os.getenv('MINIO_USE_SSL', 'False').lower() in {'true', '1', 'yes'},
}

def generate_data() -> List[Dict[str, Any]]:
    """Generate synthetic data for the pipeline."""
    try:
        data = []
        for _ in range(DEFAULT_NUM_ROWS):
            # Generate user data
            first_name = fake.first_name()
            last_name = fake.last_name()
            is_active = random.random() < 0.8

            # Generate timestamps
            account_created = fake.date_time_between(start_date=START_DATETIME, end_date=END_DATETIME)
            account_updated = fake.date_time_between(start_date=account_created, end_date=END_DATETIME)
            account_deleted = None if is_active else fake.date_time_between(start_date=account_updated, end_date=END_DATETIME)
            
            login_time = fake.date_time_between(
                start_date=account_created,
                end_date=account_deleted if account_deleted else END_DATETIME
            )
            logout_time = login_time + timedelta(hours=random.uniform(0.5, 4))
            
            # Create record
            record = {
                "user_id": fake.uuid4(),
                "first_name": first_name,
                "last_name": last_name,
                "email": f"{first_name.lower()}_{last_name.lower()}@{fake.domain_name()}",
                "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=72).isoformat(),
                "phone_number": fake.phone_number(),
                "address": fake.address(),
                "city": fake.city(),
                "state": fake.state(),
                "postal_code": fake.postcode(),
                "country": fake.country(),
                "company": fake.company(),
                "job_title": fake.job(),
                "ip_address": fake.ipv4(),
                "is_active": lambda x: 'yes' if is_active == 1 else 'no',
                "login_time": login_time.isoformat(),
                "logout_time": logout_time.isoformat(),
                "account_created": account_created.isoformat(),
                "account_updated": account_updated.isoformat(),
                "account_deleted": account_deleted.isoformat() if account_deleted else None,
                "session_duration_minutes": (logout_time - login_time).total_seconds() / 60,
                "product_id": fake.uuid4(),
                "product_name": fake.random_element(["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta", "Iota", "Kappa", "Lambda", "Omicron", "Sigma", "Tau", "Upsilon", "Phi", "Omega"]),
                "price": round(fake.pyfloat(min_value=100, max_value=5000, right_digits=2), 2),
                "purchase_status": fake.random_element(["completed", "pending", "failed"]),
                "user_agent": fake.user_agent()
            }
            data.append(record)
            
        return data
    except Exception as e:
        log.error("Error generating data: %s", str(e))
        raise

def make_ua_table(df: pd.DataFrame) -> None:
    """Create the source user_activity table in DuckDB."""
    try:
        conn = duckdb.connect(str(db_path))
        
        # Create schemas
        conn.execute("""
            CREATE SCHEMA IF NOT EXISTS dbt_pipeline_demo;
        """)
        
        # Create source table with proper types
        conn.execute("DROP TABLE IF EXISTS user_activity")
        conn.execute("""
            CREATE TABLE user_activity AS
            SELECT * FROM df;
        """)
        
        count = conn.execute("SELECT COUNT(*) FROM user_activity").fetchone()[0]
        log.info(f"Created user_activity table with {count} records")
        
    except Exception as e:
        log.error("Error creating user_activity table: %s", str(e))
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def run_dbt_models() -> None:
    """Run dbt models to transform the data."""
    try:
        original_dir = os.getcwd()
        os.chdir(DBT_ROOT)
        
        conn = duckdb.connect(str(db_path))
        
        # Clean up existing tables
        conn.execute("""
            DROP TABLE IF EXISTS product_schema CASCADE;
            DROP TABLE IF EXISTS stg_product_schema CASCADE;
            DROP TABLE IF EXISTS dim_user CASCADE;
        """)
        
        # Run models in dependency order
        dbt = dbtRunner()
        models = ['dim_user', 'stg_product_schema', 'product_schema']
        
        for model in models:
            log.info(f"Running dbt model: {model}")
            result = dbt.invoke([
                "run",
                "--target", "dev",
                "--select", model,
                "--full-refresh"
            ])
            if not result.success:
                raise Exception(f"Failed to create {model}")
            
    except Exception as e:
        log.error("Error running dbt models: %s", str(e))
        raise
    finally:
        if 'conn' in locals():
            conn.close()
        os.chdir(original_dir)

def generate_reports() -> None:
    """Generate analytics reports from transformed data."""
    try:
        reports_dir = PROJECT_ROOT / 'reports'
        reports_dir.mkdir(parents=True, exist_ok=True)

        conn = duckdb.connect(str(db_path))
        
        # Run analytics
        analysis_results = {
            "lifecycle_analysis": run_lifecycle_analysis(conn),
            "purchase_analysis": run_purchase_analysis(conn),
            "demographics_analysis": run_demographics_analysis(conn),
            "business_analysis": run_business_analysis(conn),
            "engagement_analysis": run_engagement_analysis(conn),
            "churn_analysis": run_churn_analysis(conn),
            "session_analysis": run_session_analysis(conn),
            "funnel_analysis": run_funnel_analysis(conn)
        }

        save_analysis_results(analysis_results, reports_dir)
        
    except Exception as e:
        log.error("Error generating reports: %s", str(e))
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def upload_data() -> None:
    """Upload transformed data to S3."""
    try:
        conn = duckdb.connect(str(db_path))
        
        # Get final data
        final_df = conn.sql("""
            SELECT * FROM product_schema
        """).df()
        
        # Upload to S3
        client = minio.Minio(
            S3_CONFIG['endpoint'],
            access_key=S3_CONFIG['access_key'],
            secret_key=S3_CONFIG['secret_key'],
            secure=S3_CONFIG['use_ssl']
        )
        
        final_df.to_json('temp_upload.json', orient='records')
        client.fput_object(
            bucket_name=S3_CONFIG['bucket'],
            object_name='cleaned_data.json',
            file_path='temp_upload.json'
        )
        os.remove('temp_upload.json')
        
        log.info(f"Data uploaded to S3 successfully: {len(final_df)} rows")
        
    except Exception as e:
        log.error("Error uploading data: %s", str(e))
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def main() -> None:
    """Execute the main data pipeline workflow."""
    try:
        # 1. Generate fresh data
        data = generate_data()
        df = pd.DataFrame(data)
        
        # 2. Create source table
        make_ua_table(df)

        # 3. Run dbt transformations
        run_dbt_models()
        
        # 4. Small delay to ensure tables are available
        time.sleep(1)  

        # 5. Generate reports
        generate_reports()

        # 6. Upload to S3
        upload_data()

        # Add this debug code in your main_data_pipeline.py after dbt run:
        conn = duckdb.connect(str(db_path))
        tables = conn.sql("SELECT * FROM information_schema.tables").df()
        print("Available tables:", tables)

    except Exception as e:
        log.error("Pipeline failed: %s", str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
