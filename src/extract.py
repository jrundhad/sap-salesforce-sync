import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import logging

# Load environment variables
load_dotenv(dotenv_path='config/.env')

# Configure logging
logging.basicConfig(
    filename='logs/sync.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_db_engine():
    """Create and return a SQLAlchemy database engine."""
    try:
        db_url = (
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        engine = create_engine(db_url)
        logging.info("Successfully created database engine")
        return engine
    except Exception as e:
        logging.error(f"Failed to create database engine: {e}")
        raise

def extract_customers():
    """Extract all customer records from KNA1 table."""
    try:
        engine = get_db_engine()
        query = "SELECT kunnr, name1, ort01, land1, telf1, erdat FROM kna1;"
        df = pd.read_sql_query(query, engine)
        logging.info(f"Extracted {len(df)} customer records from KNA1")
        return df
    except Exception as e:
        logging.error(f"Failed to extract customers: {e}")
        raise

def extract_sales_orders():
    """Extract all sales order records from VBAK table."""
    try:
        engine = get_db_engine()
        query = "SELECT vbeln, kunnr, erdat, netwr, waerk, matnr, kwmeng FROM vbak;"
        df = pd.read_sql_query(query, engine)
        logging.info(f"Extracted {len(df)} sales order records from VBAK")
        return df
    except Exception as e:
        logging.error(f"Failed to extract sales orders: {e}")
        raise

if __name__ == "__main__":
    customers = extract_customers()
    orders = extract_sales_orders()
    print("Customers:")
    print(customers)
    print("\nSales Orders:")
    print(orders)