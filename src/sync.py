import logging
import schedule
import time
from datetime import datetime
from dotenv import load_dotenv

from extract import extract_customers, extract_sales_orders
from transform import transform_customers, transform_sales_orders
from load import get_salesforce_connection, upsert_accounts, upsert_opportunities

# Load environment variables
load_dotenv(dotenv_path='config/.env')

# Configure logging
logging.basicConfig(
    filename='logs/sync.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_sync():
    """Run the full SAP to Salesforce sync pipeline."""
    start_time = datetime.now()
    logging.info(f"Starting SAP to Salesforce sync at {start_time}")
    print(f"\nStarting sync at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Step 1 - Extract
        print("Step 1: Extracting data from SAP (PostgreSQL)...")
        customers = extract_customers()
        orders = extract_sales_orders()
        logging.info(f"Extraction complete - {len(customers)} customers, {len(orders)} orders")

        # Step 2 - Transform
        print("Step 2: Transforming data to Salesforce format...")
        sf_accounts = transform_customers(customers)
        sf_opportunities = transform_sales_orders(orders, customers)
        logging.info("Transformation complete")

        # Step 3 - Load
        print("Step 3: Loading data into Salesforce...")
        sf = get_salesforce_connection()
        upsert_accounts(sf, sf_accounts)
        upsert_opportunities(sf, sf_opportunities)

        end_time = datetime.now()
        duration = (end_time - start_time).seconds
        logging.info(f"Sync completed successfully in {duration} seconds")
        print(f"\nSync completed successfully in {duration} seconds")

    except Exception as e:
        logging.error(f"Sync failed: {e}")
        print(f"Sync failed: {e}")

def schedule_sync():
    """Schedule the sync to run every 24 hours."""
    print("SAP to Salesforce Sync Tool")
    print("Scheduler started - sync will run every 24 hours")
    print("Running initial sync now...")

    # Run immediately on start
    run_sync()

    # Then schedule every 24 hours
    schedule.every(24).hours.do(run_sync)

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        # Run once and exit
        run_sync()
    else:
        # Run on schedule
        schedule_sync()