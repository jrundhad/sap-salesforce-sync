# SAP to Salesforce Data Sync Tool

A Python-based ETL pipeline that automatically extracts customer and sales order data from SAP (simulated via PostgreSQL), transforms it into Salesforce-compatible format, and loads it into Salesforce CRM as Accounts and Opportunities. Includes an AI-powered Streamlit dashboard for real-time pipeline visibility and natural language querying of Salesforce data.

---

## Business Problem

Enterprise sales teams using Salesforce CRM often lack visibility into customer and order data stored in SAP ERP systems. This forces sales reps to manually cross-reference two systems to get a complete picture of a customer — leading to outdated account data, missed follow-ups, and lost revenue opportunities.

This tool acts as the integration layer between SAP and Salesforce, ensuring sales teams always have accurate, up-to-date customer and order information directly in their CRM — surfaced through an intelligent dashboard that answers business questions in plain English.

---

## Solution

An automated ETL pipeline that runs on a 24-hour schedule and syncs data from SAP into Salesforce without any manual intervention. The pipeline uses upsert logic to prevent duplicate records on every run. A Streamlit dashboard powered by Claude AI provides real-time visibility into pipeline health, revenue trends, and account data — with a natural language interface for ad-hoc queries.

---

## Architecture
```
PostgreSQL (Mock SAP)
        │
        ▼
   extract.py          → Pulls data from KNA1 (customers) and VBAK (sales orders)
        │
        ▼
  transform.py         → Maps SAP field names to Salesforce field names
        │
        ▼
    load.py            → Upserts Accounts and Opportunities into Salesforce via API
        │
        ▼
    sync.py            → Orchestrates the full pipeline on a 24-hour schedule
        │
        ▼
  dashboard.py         → Streamlit dashboard pulling live data from Salesforce
                          with AI-powered natural language query interface
```

---

## Tech Stack

- **Python 3** — Core pipeline logic
- **PostgreSQL** — Mock SAP database using real SAP table structures (KNA1, VBAK)
- **Salesforce Developer Edition** — Target CRM environment
- **simple-salesforce** — Salesforce REST API client
- **pandas** — Data transformation and manipulation
- **SQLAlchemy** — Database connection management
- **python-dotenv** — Secure credential management
- **schedule** — Pipeline scheduling
- **Streamlit** — Interactive dashboard framework
- **Plotly** — Data visualizations
- **Anthropic Claude API** — AI-powered natural language query interface

---

## Data Flow

### SAP to Salesforce Field Mapping

**Customer Master (KNA1) → Salesforce Account**
| SAP Field | SAP Description | Salesforce Field |
|-----------|----------------|-----------------|
| KUNNR | Customer ID | External_SAP_ID__c |
| NAME1 | Customer Name | Name |
| ORT01 | City | BillingCity |
| LAND1 | Country | BillingCountry |
| TELF1 | Phone | Phone |

**Sales Orders (VBAK) → Salesforce Opportunity**
| SAP Field | SAP Description | Salesforce Field |
|-----------|----------------|-----------------|
| VBELN | Sales Order Number | External_SAP_Order_ID__c |
| NETWR | Net Order Value | Amount |
| ERDAT | Order Date | CloseDate |
| MATNR | Material Number | Description |
| WAERK | Currency | CurrencyIsoCode |

---

## Project Structure
```
sap-salesforce-sync/
├── config/
│   └── .env                  # Credentials (never committed to GitHub)
├── src/
│   ├── extract.py            # Extracts data from PostgreSQL
│   ├── transform.py          # Maps SAP fields to Salesforce fields
│   ├── load.py               # Upserts data into Salesforce
│   ├── sync.py               # Orchestrates the full pipeline
│   └── dashboard.py          # AI-powered Streamlit dashboard
├── logs/
│   └── sync.log              # Pipeline execution logs
├── tests/
│   └── test_transform.py     # Unit tests for transformation logic
├── requirements.txt
└── README.md
```

---

## Setup Instructions

### Prerequisites
- Python 3.x
- PostgreSQL
- Salesforce Developer Edition org
- Anthropic API key (for AI assistant)

### 1. Clone the repository
```bash
git clone https://github.com/jrundhad/sap-salesforce-sync.git
cd sap-salesforce-sync
```

### 2. Create and activate virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure credentials
Create a `config/.env` file with the following:
```
SF_CLIENT_ID=your_salesforce_consumer_key
SF_CLIENT_SECRET=your_salesforce_consumer_secret

DB_HOST=localhost
DB_PORT=5432
DB_NAME=sap_salesforce_sync
DB_USER=your_postgres_username
DB_PASSWORD=your_postgres_password

ANTHROPIC_API_KEY=your_anthropic_api_key
```

### 5. Set up PostgreSQL database
Create the database and tables using the SAP table structures defined in the project.

### 6. Run the pipeline

Run once:
```bash
python3 src/sync.py --once
```

Run on 24-hour schedule:
```bash
python3 src/sync.py
```

### 7. Launch the dashboard
```bash
streamlit run src/dashboard.py
```

---

## Dashboard

The Streamlit dashboard pulls live data directly from Salesforce and provides three sections:

**Pipeline Health** — real-time metrics showing total accounts, total opportunities, total pipeline value in CAD, and last sync status with timestamp.

**Business Intelligence** — four interactive charts including top 10 accounts by revenue, revenue breakdown by service type, monthly revenue trend, and customer distribution by city.

**Account Explorer** — a filterable table allowing users to drill into accounts and opportunities by city and service type.

**AI Sales Assistant** — a natural language chat interface powered by Claude that allows sales managers to ask plain English questions about their pipeline and accounts. Example queries:
- "Which client has the highest total revenue?"
- "Show me all clients in Calgary"
- "How many enterprise implementation deals do we have?"
- "What is the average deal size across all opportunities?"

---

## Key Features

- **Upsert logic** — Prevents duplicate records on every sync run using External ID fields
- **OAuth2 authentication** — Secure Salesforce API access using Client Credentials Flow
- **Structured logging** — Every sync step logged to file with timestamps
- **Error handling** — Failed records are logged without stopping the pipeline
- **Modular architecture** — Each component is independently testable and maintainable
- **Scheduled execution** — Runs automatically every 24 hours
- **Live Salesforce data** — Dashboard queries Salesforce API directly, not the source database
- **AI-powered querying** — Natural language interface for non-technical sales users

---

## Unit Tests

8 unit tests covering the transformation layer:
```bash
pytest tests/test_transform.py -v
```
```
tests/test_transform.py::test_transform_customers_columns PASSED
tests/test_transform.py::test_transform_customers_row_count PASSED
tests/test_transform.py::test_transform_customers_country_mapping PASSED
tests/test_transform.py::test_transform_customers_external_id PASSED
tests/test_transform.py::test_transform_orders_columns PASSED
tests/test_transform.py::test_transform_orders_row_count PASSED
tests/test_transform.py::test_transform_orders_amount PASSED
tests/test_transform.py::test_transform_orders_name_format PASSED

8 passed in 0.29s
```

---

## Sample Pipeline Output
```
Starting sync at 2026-03-03 17:33:22
Step 1: Extracting data from SAP (PostgreSQL)...
Step 2: Transforming data to Salesforce format...
Step 3: Loading data into Salesforce...
Accounts sync complete - Success: 25, Errors: 0
Opportunities sync complete - Success: 50, Errors: 0
Sync completed successfully in 52 seconds
```