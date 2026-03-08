import streamlit as st
import pandas as pd
import plotly.express as px
from simple_salesforce import Salesforce
from dotenv import load_dotenv
import os
import requests
import anthropic

# Load environment variables
load_dotenv(dotenv_path='config/.env')

# Page configuration
st.set_page_config(
    page_title="SAP to Salesforce Sync Dashboard",
    page_icon="🔄",
    layout="wide"
)

# Salesforce connection
@st.cache_resource
def get_salesforce_connection():
    token_url = "https://orgfarm-ba5f0b3a42-dev-ed.develop.my.salesforce.com/services/oauth2/token"
    payload = {
        'grant_type': 'client_credentials',
        'client_id': os.getenv('SF_CLIENT_ID'),
        'client_secret': os.getenv('SF_CLIENT_SECRET')
    }
    response = requests.post(token_url, data=payload)
    token_data = response.json()
    sf = Salesforce(
        instance_url=token_data['instance_url'],
        session_id=token_data['access_token']
    )
    return sf

# Load data from Salesforce
@st.cache_data(ttl=300)
def load_accounts():
    sf = get_salesforce_connection()
    result = sf.query_all("""
        SELECT Id, Name, BillingCity, BillingCountry, Phone, 
               Industry, Type, External_SAP_ID__c
        FROM Account 
    """)
    return pd.DataFrame(result['records']).drop(columns=['attributes'])

@st.cache_data(ttl=300)
def load_opportunities():
    sf = get_salesforce_connection()
    result = sf.query_all("""
        SELECT Id, Name, Amount, CloseDate, StageName, 
               Description, AccountId, External_SAP_Order_ID__c,
               CurrencyIsoCode
        FROM Opportunity 
    """)
    return pd.DataFrame(result['records']).drop(columns=['attributes'])

def load_sync_log():
    try:
        with open('logs/sync.log', 'r') as f:
            lines = f.readlines()
        return lines
    except:
        return []

# Load data
with st.spinner("Loading data from Salesforce..."):
    accounts = load_accounts()
    opportunities = load_opportunities()
    log_lines = load_sync_log()

# Merge accounts and opportunities
merged = opportunities.merge(
    accounts[['Id', 'Name', 'BillingCity']],
    left_on='AccountId',
    right_on='Id',
    how='left',
    suffixes=('_opp', '_acc')
)

# Header
st.title("🔄 SAP to Salesforce Sync Dashboard")
st.markdown("Real-time visibility into your SAP ERP and Salesforce CRM integration")
st.divider()

# --- SECTION 1: PIPELINE HEALTH ---
st.subheader("Pipeline Health")

last_sync = "No sync recorded"
last_status = "Unknown"
for line in reversed(log_lines):
    if "Sync completed successfully" in line:
        last_sync = line.split(" - ")[0]
        last_status = "Success"
        break
    elif "Sync failed" in line:
        last_sync = line.split(" - ")[0]
        last_status = "Failed"
        break

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Accounts", len(accounts))
with col2:
    st.metric("Total Opportunities", len(opportunities))
with col3:
    st.metric("Total Pipeline Value", f"${opportunities['Amount'].sum():,.0f} CAD")
with col4:
    status_color = "🟢" if last_status == "Success" else "🔴"
    st.metric("Last Sync Status", f"{status_color} {last_status}")

st.caption(f"Last sync: {last_sync}")
st.divider()

# --- SECTION 2: BUSINESS INTELLIGENCE ---
st.subheader("Business Intelligence")

col1, col2 = st.columns(2)

with col1:
    top_accounts = merged.groupby('Name_acc')['Amount'].sum().reset_index()
    top_accounts = top_accounts.sort_values('Amount', ascending=False).head(10)
    top_accounts.columns = ['Account', 'Total Revenue']
    fig1 = px.bar(
        top_accounts,
        x='Total Revenue',
        y='Account',
        orientation='h',
        title='Top 10 Accounts by Revenue (CAD)',
        color='Total Revenue',
        color_continuous_scale='Blues'
    )
    fig1.update_layout(showlegend=False, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    revenue_by_service = opportunities.groupby('Description')['Amount'].sum().reset_index()
    revenue_by_service.columns = ['Service Type', 'Total Revenue']
    fig2 = px.pie(
        revenue_by_service,
        values='Total Revenue',
        names='Service Type',
        title='Revenue by Service Type'
    )
    st.plotly_chart(fig2, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    opportunities['CloseDate'] = pd.to_datetime(opportunities['CloseDate'])
    opportunities['Month'] = opportunities['CloseDate'].dt.strftime('%Y-%m')
    orders_by_month = opportunities.groupby('Month')['Amount'].sum().reset_index()
    orders_by_month.columns = ['Month', 'Total Revenue']
    fig3 = px.line(
        orders_by_month,
        x='Month',
        y='Total Revenue',
        title='Revenue Trend by Month',
        markers=True
    )
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    accounts_by_city = accounts.groupby('BillingCity').size().reset_index()
    accounts_by_city.columns = ['City', 'Count']
    fig4 = px.bar(
        accounts_by_city,
        x='City',
        y='Count',
        title='Accounts by City',
        color='Count',
        color_continuous_scale='Blues'
    )
    st.plotly_chart(fig4, use_container_width=True)

st.divider()

# --- SECTION 3: ACCOUNT EXPLORER ---
st.subheader("Account Explorer")

col1, col2 = st.columns(2)
with col1:
    city_filter = st.multiselect(
        "Filter by City",
        options=sorted(accounts['BillingCity'].dropna().unique()),
        default=[]
    )
with col2:
    service_filter = st.multiselect(
        "Filter by Service Type",
        options=sorted(opportunities['Description'].dropna().unique()),
        default=[]
    )

filtered = merged.copy()
if city_filter:
    filtered = filtered[filtered['BillingCity'].isin(city_filter)]
if service_filter:
    filtered = filtered[filtered['Description'].isin(service_filter)]

display_df = filtered[['Name_acc', 'BillingCity', 'Name_opp', 'Amount', 'Description', 'CloseDate']].copy()
display_df.columns = ['Account', 'City', 'Opportunity', 'Amount (CAD)', 'Service Type', 'Close Date']
display_df['Amount (CAD)'] = display_df['Amount (CAD)'].apply(lambda x: f"${x:,.0f}")

st.dataframe(display_df, use_container_width=True, hide_index=True)
st.divider()

# --- SECTION 4: AI ASSISTANT ---
st.subheader("🤖 AI Sales Assistant")
st.markdown("Ask questions about your accounts and opportunities in plain English.")

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

def build_data_context():
    accounts_summary = accounts[['Name', 'BillingCity', 'BillingCountry']].to_string(index=False)
    opps_summary = merged[['Name_acc', 'Name_opp', 'Amount', 'Description', 'CloseDate']].copy()
    opps_summary.columns = ['Account', 'Opportunity', 'Amount', 'Service Type', 'Close Date']
    opps_summary = opps_summary.to_string(index=False)

    total_pipeline = opportunities['Amount'].sum()
    top_account = merged.groupby('Name_acc')['Amount'].sum().idxmax()

    return f"""
You are an AI sales assistant analyzing Salesforce CRM data for a tech consulting firm.

Here is the current data:

SUMMARY:
- Total Accounts: {len(accounts)}
- Total Opportunities: {len(opportunities)}
- Total Pipeline Value: ${total_pipeline:,.0f} CAD
- Top Account by Revenue: {top_account}

ACCOUNTS:
{accounts_summary}

OPPORTUNITIES:
{opps_summary}

Answer the user's question based on this data. Be concise and specific.
Format numbers as currency where appropriate.
If asked for a list, use bullet points.
If the question cannot be answered from the data provided, say so clearly.
"""

if prompt := st.chat_input("Ask me anything about your accounts and pipeline..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                client = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))

                response = client.messages.create(
                    model="claude-opus-4-5",
                    max_tokens=1024,
                    system=build_data_context(),
                    messages=[
                        {"role": m["role"], "content": m["content"]}
                        for m in st.session_state.messages
                    ]
                )

                assistant_message = response.content[0].text
                st.markdown(assistant_message)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": assistant_message
                })

            except Exception as e:
                st.error(f"AI assistant error: {e}")