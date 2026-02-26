import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename='logs/sync.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def transform_customers(df):
    """Transform KNA1 customer data into Salesforce Account format."""
    try:
        sf_accounts = pd.DataFrame()

        sf_accounts['Name'] = df['name1']
        sf_accounts['BillingCity'] = df['ort01']
        sf_accounts['BillingCountry'] = df['land1'].map({'CA': 'Canada', 'US': 'United States'})
        sf_accounts['Phone'] = df['telf1']
        sf_accounts['External_SAP_ID__c'] = df['kunnr']
        sf_accounts['Industry'] = 'Technology'
        sf_accounts['Type'] = 'Customer'

        logging.info(f"Transformed {len(sf_accounts)} customer records for Salesforce")
        return sf_accounts
    except Exception as e:
        logging.error(f"Failed to transform customers: {e}")
        raise

def transform_sales_orders(df, customers_df):
    """Transform VBAK sales order data into Salesforce Opportunity format."""
    try:
        sf_opportunities = pd.DataFrame()

        sf_opportunities['Name'] = df['vbeln'].apply(lambda x: f"SAP Order {x}")
        sf_opportunities['Amount'] = df['netwr']
        sf_opportunities['CloseDate'] = df['erdat'].apply(
            lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else datetime.today().strftime('%Y-%m-%d')
        )
        sf_opportunities['StageName'] = df['matnr'].map({
            'SAP-IMPL-ENT': 'Closed Won',
            'SAP-IMPL-STD': 'Closed Won',
            'SAP-CONSULT-SVC': 'Closed Won',
            'SAP-SUPPORT-PRE': 'Closed Won'
        }).fillna('Closed Won')
        sf_opportunities['External_SAP_Order_ID__c'] = df['vbeln']
        sf_opportunities['External_SAP_ID__c'] = df['kunnr']
        sf_opportunities['CurrencyIsoCode'] = df['waerk']
        sf_opportunities['Description'] = df['matnr'].map({
            'SAP-IMPL-ENT': 'SAP Enterprise Implementation',
            'SAP-IMPL-STD': 'SAP Standard Implementation',
            'SAP-CONSULT-SVC': 'SAP Consulting Services',
            'SAP-SUPPORT-PRE': 'SAP Support & Maintenance'
        })

        logging.info(f"Transformed {len(sf_opportunities)} sales order records for Salesforce")
        return sf_opportunities
    except Exception as e:
        logging.error(f"Failed to transform sales orders: {e}")
        raise

if __name__ == "__main__":
    from extract import extract_customers, extract_sales_orders

    customers = extract_customers()
    orders = extract_sales_orders()

    sf_accounts = transform_customers(customers)
    sf_opportunities = transform_sales_orders(orders, customers)

    print("Transformed Accounts:")
    print(sf_accounts)
    print("\nTransformed Opportunities:")
    print(sf_opportunities)