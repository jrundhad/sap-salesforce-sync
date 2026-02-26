import logging
import requests
from simple_salesforce import Salesforce
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv(dotenv_path='config/.env')

# Configure logging
logging.basicConfig(
    filename='logs/sync.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_salesforce_connection():
    """Create and return a Salesforce connection using Client Credentials Flow."""
    try:
        token_url = "https://orgfarm-ba5f0b3a42-dev-ed.develop.my.salesforce.com/services/oauth2/token"
        payload = {
            'grant_type': 'client_credentials',
            'client_id': os.getenv('SF_CLIENT_ID'),
            'client_secret': os.getenv('SF_CLIENT_SECRET')
        }
        response = requests.post(token_url, data=payload)
        if response.status_code != 200:
            print(f"Auth error: {response.json()}")
            response.raise_for_status()
        token_data = response.json()

        sf = Salesforce(
            instance_url=token_data['instance_url'],
            session_id=token_data['access_token']
        )
        logging.info("Successfully connected to Salesforce via Client Credentials Flow")
        return sf
    except Exception as e:
        logging.error(f"Failed to connect to Salesforce: {e}")
        raise

def upsert_accounts(sf, accounts_df):
    """Upsert Account records into Salesforce using External_SAP_ID__c as key."""
    success_count = 0
    error_count = 0

    for _, row in accounts_df.iterrows():
        try:
            record = {
                'Name': row['Name'],
                'BillingCity': row['BillingCity'],
                'BillingCountry': row['BillingCountry'],
                'Phone': row['Phone'],
                'Industry': row['Industry'],
                'Type': row['Type']
            }
            sf.Account.upsert(f"External_SAP_ID__c/{row['External_SAP_ID__c']}", record)
            success_count += 1
            logging.info(f"Upserted Account: {row['Name']}")
        except Exception as e:
            error_count += 1
            logging.error(f"Failed to upsert Account {row['Name']}: {e}")

    logging.info(f"Accounts sync complete - Success: {success_count}, Errors: {error_count}")
    print(f"Accounts sync complete - Success: {success_count}, Errors: {error_count}")

def upsert_opportunities(sf, opportunities_df):
    """Upsert Opportunity records into Salesforce using External_SAP_Order_ID__c as key."""
    success_count = 0
    error_count = 0

    for _, row in opportunities_df.iterrows():
        try:
            query = f"SELECT Id FROM Account WHERE External_SAP_ID__c = '{row['External_SAP_ID__c']}'"
            result = sf.query(query)

            if result['totalSize'] == 0:
                logging.warning(f"No Account found for SAP ID {row['External_SAP_ID__c']}, skipping order {row['External_SAP_Order_ID__c']}")
                error_count += 1
                continue

            account_id = result['records'][0]['Id']

            record = {
                'Name': row['Name'],
                'Amount': float(row['Amount']),
                'CloseDate': row['CloseDate'],
                'StageName': row['StageName'],
                'AccountId': account_id,
                'Description': row['Description'],
                'CurrencyIsoCode': row['CurrencyIsoCode']
            }
            sf.Opportunity.upsert(f"External_SAP_Order_ID__c/{row['External_SAP_Order_ID__c']}", record)
            success_count += 1
            logging.info(f"Upserted Opportunity: {row['Name']}")
        except Exception as e:
            error_count += 1
            logging.error(f"Failed to upsert Opportunity {row['Name']}: {e}")

    logging.info(f"Opportunities sync complete - Success: {success_count}, Errors: {error_count}")
    print(f"Opportunities sync complete - Success: {success_count}, Errors: {error_count}")

if __name__ == "__main__":
    from extract import extract_customers, extract_sales_orders
    from transform import transform_customers, transform_sales_orders

    customers = extract_customers()
    orders = extract_sales_orders()

    sf_accounts = transform_customers(customers)
    sf_opportunities = transform_sales_orders(orders, customers)

    sf = get_salesforce_connection()
    upsert_accounts(sf, sf_accounts)
    upsert_opportunities(sf, sf_opportunities)