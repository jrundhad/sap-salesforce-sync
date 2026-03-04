import pytest
import pandas as pd
from src.transform import transform_customers, transform_sales_orders

def get_mock_customers():
    return pd.DataFrame({
        'kunnr': ['C001', 'C002'],
        'name1': ['Nexus Digital Consulting', 'Apex Technology Group'],
        'ort01': ['Toronto', 'Vancouver'],
        'land1': ['CA', 'CA'],
        'telf1': ['416-555-0101', '604-555-0102'],
        'erdat': ['2026-02-24', '2026-02-24']
    })

def get_mock_orders():
    return pd.DataFrame({
        'vbeln': ['SO001', 'SO002'],
        'kunnr': ['C001', 'C001'],
        'erdat': pd.to_datetime(['2026-02-24', '2026-02-24']),
        'netwr': [250000.00, 85000.00],
        'waerk': ['CAD', 'CAD'],
        'matnr': ['SAP-IMPL-ENT', 'SAP-SUPPORT-PRE'],
        'kwmeng': [1.000, 1.000]
    })

def test_transform_customers_columns():
    """Test that transformed customers have correct Salesforce column names."""
    customers = get_mock_customers()
    result = transform_customers(customers)
    expected_columns = ['Name', 'BillingCity', 'BillingCountry', 'Phone', 'External_SAP_ID__c', 'Industry', 'Type']
    assert list(result.columns) == expected_columns

def test_transform_customers_row_count():
    """Test that all customer rows are retained after transformation."""
    customers = get_mock_customers()
    result = transform_customers(customers)
    assert len(result) == 2

def test_transform_customers_country_mapping():
    """Test that country codes are mapped to full country names."""
    customers = get_mock_customers()
    result = transform_customers(customers)
    assert result['BillingCountry'].iloc[0] == 'Canada'

def test_transform_customers_external_id():
    """Test that SAP customer ID is mapped to External_SAP_ID__c."""
    customers = get_mock_customers()
    result = transform_customers(customers)
    assert result['External_SAP_ID__c'].iloc[0] == 'C001'

def test_transform_orders_columns():
    """Test that transformed orders have correct Salesforce column names."""
    customers = get_mock_customers()
    orders = get_mock_orders()
    result = transform_sales_orders(orders, customers)
    assert 'Name' in result.columns
    assert 'Amount' in result.columns
    assert 'External_SAP_Order_ID__c' in result.columns

def test_transform_orders_row_count():
    """Test that all order rows are retained after transformation."""
    customers = get_mock_customers()
    orders = get_mock_orders()
    result = transform_sales_orders(orders, customers)
    assert len(result) == 2

def test_transform_orders_amount():
    """Test that order amounts are correctly mapped."""
    customers = get_mock_customers()
    orders = get_mock_orders()
    result = transform_sales_orders(orders, customers)
    assert result['Amount'].iloc[0] == 250000.00

def test_transform_orders_name_format():
    """Test that opportunity names follow the correct format."""
    customers = get_mock_customers()
    orders = get_mock_orders()
    result = transform_sales_orders(orders, customers)
    assert result['Name'].iloc[0] == 'SAP Order SO001'