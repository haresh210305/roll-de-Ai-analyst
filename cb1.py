import streamlit as st
from openai import AzureOpenAI
import os
from dotenv import load_dotenv
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Page config must be the first Streamlit command
st.set_page_config(
    page_title="Roll'd SQL Assistant",
    page_icon="🍜",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load .env file
load_dotenv()

# Azure OpenAI Config
client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_KEY"),
    api_version="2023-07-01-preview",
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
)

DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")

# Example schema (replace with your full schema)
DATABASE_SCHEMA = """

###Table: abacus_3pl_daily_summary
  - sales_date: date
  - store_name: string
  - payment_type: string
  - net_sales: decimal(9,2)
  - txns: int

###Table: abacus_cash_card_daily_summary
  - sales_date: date
  - store_name: string
  - payment_type: string
  - net_sales: decimal(9,2)
  - txns: int

###Table: abacus_daily_data
  - store_name: string
  - device_id: string
  - invoice_number: string
  - sales_datetime: timestamp
  - customer_name: string
  - customer_email: string
  - customer_phone: string
  - invoice_discount: decimal(9,2)
  - item_name: string
  - item_code: string
  - category: string
  - amount: decimal(9,2)
  - amount_excl_tax: decimal(9,2)
  - tax: decimal(9,2)
  - discount: decimal(9,2)
  - quantity: decimal(9,2)
  - unit_price: decimal(9,2)
  - variant: string
  - variant_code: string
  - variant_amount: decimal(9,2)
  - variant_amount_excl_tax: decimal(9,2)
  - variant_tax: decimal(9,2)
  - variant_quantity: decimal(9,2)
  - variant_unit_price: decimal(9,2)
  - payment_type: string
  - order_type: string
  - delivery_fee: decimal(9,2)

Table: abacus_daily_data_variances
  - store_name: string
  - device_id: string
  - invoice_number: string
  - sales_datetime: timestamp
  - customer_name: string
  - customer_email: string
  - customer_phone: string
  - invoice_discount: decimal(9,2)
  - item_name: string
  - item_code: string
  - category: string
  - amount: decimal(9,2)
  - amount_excl_tax: decimal(9,2)
  - tax: decimal(9,2)
  - discount: decimal(9,2)
  - quantity: decimal(9,2)
  - unit_price: decimal(9,2)
  - variant: string
  - variant_code: string
  - variant_amount: decimal(9,2)
  - variant_amount_excl_tax: decimal(9,2)
  - variant_tax: decimal(9,2)
  - variant_quantity: decimal(9,2)
  - variant_unit_price: decimal(9,2)
  - payment_type: string
  - order_type: string
  - delivery_fee: decimal(9,2)

Table: abacus_daily_summary
  - start_date: date
  - end_date: date
  - store_name: string
  - discount: decimal(9,2)
  - refund: decimal(9,2)
  - net_sales: decimal(9,2)
  - gross_sales: decimal(9,2)
  - txns: int
  - avg_net_sales: decimal(9,2)
  - avg_gross_sales: decimal(9,2)

Table: abacus_daily_summary_backup
  - start_date: date
  - end_date: date
  - store_name: string
  - discount: decimal(9,2)
  - refund: decimal(9,2)
  - net_sales: decimal(9,2)
  - gross_sales: decimal(9,2)
  - txns: int
  - avg_net_sales: decimal(9,2)
  - avg_gross_sales: decimal(9,2)

Table: abacus_daily_summary_variances
  - todays_date: date
  - refresh_date: date
  - store_name: string
  - variance: decimal(10,2)
  - net_sales_new: decimal(10,2)
  - net_sales_old: decimal(10,2)

Table: abacus_daily_variant_quantity
  - sales_datetime: timestamp
  - variant: string
  - quantity: decimal(9,2)

Table: abacus_grocery_daily_summary
  - sales_date: date
  - store_name: string
  - net_sales: decimal(9,2)
  - txns: int

Table: abacus_item_weekly_summary
  - week_commencing: date
  - store_name: string
  - category: string
  - item_name: string
  - quantity: decimal(19,2)
  - amount_excl_tax: decimal(19,2)
  - payment_type: string

Table: abacus_item_weekly_summary_backup
  - week_commencing: date
  - store_name: string
  - category: string
  - item_name: string
  - quantity: decimal(9,2)
  - amount_excl_tax: decimal(9,2)

Table: abacus_riam_daily_summary
  - sales_date: date
  - store_name: string
  - units_sold: int
  - txns: int

Table: abacus_voids
  - store_name: string
  - device_id: string
  - invoice_number: string
  - sales_datetime: timestamp
  - amount: decimal(9,2)
  - amount_excl_tax: decimal(9,2)
  - tax: decimal(9,2)
  - paid_amount: decimal(9,2)
  - void_amount: decimal(9,2)
  - void_by: string
  - void_reason: string

Table: abacus_weekly_variants
  - week_commencing: date
  - store_name: string
  - variant: string
  - quantity: bigint
  - payment_type: string

Table: abacus_weekly_variants_backup
  - week_commencing: date
  - store_name: string
  - variant: string
  - quantity: bigint

Table: actual_3pl_daily_summary
  - sales_date: date
  - store_name: string
  - net_sales: decimal(9,2)

Table: app_web_closed_stores
  - store_name: string
  - sales_datetime: timestamp

Table: app_web_daily_summary
  - sales_date: date
  - store_name: string
  - net_sales: decimal(9,2)
  - txns: int
  - order_type: string

Table: avg_order_time
  - sales_date: date
  - store_name: string
  - avg_order_time: float

Table: catering_daily_summary
  - sales_date: date
  - store_name: string
  - amount: decimal(9,2)
  - amount_excl_tax: decimal(9,2)
  - txns: int

Table: chart_of_accounts
  - sub_account: string
  - account: string

Table: copy_abacus_daily_summary
  - start_date: date
  - end_date: date
  - store_name: string
  - discount: decimal(9,2)
  - refund: decimal(9,2)
  - net_sales: decimal(9,2)
  - gross_sales: decimal(9,2)
  - txns: int
  - avg_net_sales: decimal(9,2)
  - avg_gross_sales: decimal(9,2)

Table: daily_targets
  - sales_date: date
  - store_name: string
  - sales_target: decimal(9,2)

Table: deliveroo_daily_data
  - deliveroo_name: string
  - order_id: string
  - sales_date: date
  - order_status: string
  - sales_incl_tax: decimal(9,2)

Table: doordash_daily_data
  - doordash_name: string
  - order_id: string
  - sales_date: date
  - order_status: string
  - sales_incl_tax: decimal(9,2)
  - sales_tax: decimal(9,2)

Table: franchisee_accounts
  - franchisee_account: string
  - sub_account: string

Table: hms_host_daily_summary
  - sales_date: date
  - store_name: string
  - net_sales: decimal(9,2)
  - txns: int

Table: master
  - sales_date: date
  - store_name: string
  - net_sales: decimal(9,2)
  - gross_sales: decimal(9,2)
  - txns: int

Table: master_copy
  - sales_date: date
  - store_name: string
  - net_sales: decimal(9,2)
  - gross_sales: decimal(9,2)
  - txns: int

Table: master_variance
  - same_day_last_week: string
  - sales_date: string
  - aiport: string
  - xpath_aiport_no: bigint
  - net_sales: string
  - gross_sales: string
  - txns: string
  - melbourne_datetime: timestamp
  - today_net_sales: double
  - today_gross_sales: double
  - today_txns: int
  - variance_net_sales: double
  - variance_gross_sales: double
  - variance_txns: double

Table: menulog_daily_data
  - menulog_name: string
  - order_id: string
  - sales_date: date
  - order_status: string
  - sales_excl_tax: decimal(9,2)

Table: order_of_the_day
  - order_date: date
  - store_name: string

Table: order_up_daily_data
  - order_id: int
  - cart_id: int
  - device: string
  - order_date: timestamp
  - customer: string
  - customer_type: string
  - sub_total: decimal(9,2)
  - discount: decimal(9,2)
  - fees: decimal(9,2)
  - total: decimal(9,2)
  - discount_code: string
  - discount_type: string
  - method: string
  - payment: string
  - printed: string
  - business_name: string
  - store_name: string

Table: pfd_data
  - customer_num: int
  - customer_name: string
  - order_num: string
  - order_date: date
  - product_code: int
  - product_name: string
  - quantity: decimal(9,2)
  - price: decimal(9,2)
  - price_tax: decimal(9,2)
  - total_cost: decimal(9,2)
  - total_cost_incl_tax: decimal(9,2)
  - dc_num: int
  - dc_name: string

Table: pfd_reference
  - reference: string
  - store_name: string

Table: pl_data
  - sales_month: date
  - sub_account: string
  - balance: decimal(9,2)
  - store_name: string

Table: retail_calendar
  - sales_date: date
  - sales_month: date
  - week_commencing: date
  - financial_year: string

Table: ro_abacus_daily_data
  - store_name: string
  - device_id: string
  - invoice_number: string
  - sales_datetime: timestamp
  - customer_name: string
  - customer_email: string
  - customer_phone: string
  - invoice_discount: decimal(9,2)
  - item_name: string
  - item_code: string
  - category: string
  - amount: decimal(9,2)
  - amount_excl_tax: decimal(9,2)
  - tax: decimal(9,2)
  - discount: decimal(9,2)
  - quantity: decimal(9,2)
  - unit_price: decimal(9,2)
  - variant: string
  - variant_code: string
  - variant_amount: decimal(9,2)
  - variant_amount_excl_tax: decimal(9,2)
  - variant_tax: decimal(9,2)
  - variant_quantity: decimal(9,2)
  - variant_unit_price: decimal(9,2)
  - payment_type: string
  - order_type: string
  - delivery_fee: decimal(9,2)

Table: ro_avg_order_time
  - sales_date: date
  - store_name: string
  - avg_order_time: float

Table: ro_master
  - sales_date: date
  - store_name: string
  - net_sales: decimal(9,2)
  - gross_sales: decimal(9,2)
  - txns: int

Table: ro_store_info
  - store_id: string
  - store_name: string
  - type: string
  - state: string
  - region: string
  - open_date: date
  - close_date: date
  - status: string
  - menu_tier: int

Table: rolld_prop_daily_data
  - sales_date: date
  - state_id: string
  - location_id: int
  - location_name: string
  - item_id: int
  - item_name: string
  - units_sold: int
  - unit_cost: decimal(9,3)
  - invoice_cost: decimal(9,2)

Table: ssp_daily_summary
  - store_name: string
  - sales_date: date
  - gross_sales: decimal(9,2)
  - net_sales: decimal(9,2)
  - txns: int
  - avg_net_sales: decimal(9,2)
  - item_count: int
  - avg_item_count: decimal(9,1)
  - avg_item_net_value: decimal(9,2)

Table: st_bernadette_primary_school_canteen
  - start_date: date
  - end_date: date
  - store_name: string
  - discount: decimal(9,2)
  - refund: decimal(9,2)
  - net_sales: decimal(9,2)
  - gross_sales: decimal(9,2)
  - txns: int
  - avg_net_sales: decimal(9,2)
  - avg_gross_sales: decimal(9,2)

Table: store_3pl_reference
  - reference: string
  - store_name: string

Table: store_info
  - store_id: string
  - store_name: string
  - type: string
  - state: string
  - region: string
  - open_date: date
  - close_date: date
  - status: string
  - menu_tier: int

Table: supplier_weekly_summary
  - week_commencing: date
  - store_name: string
  - product_name: string
  - quantity: decimal(9,2)
  - price: decimal(9,2)
  - total_cost: decimal(9,2)
  - supplier_name: string

Table: test
  - start_date: date
  - end_date: date
  - store_name: string
  - discount: decimal(9,2)
  - refund: decimal(9,2)
  - net_sales: decimal(9,2)
  - gross_sales: decimal(9,2)
  - txns: int
  - avg_net_sales: decimal(9,2)
  - avg_gross_sales: decimal(9,2)

Table: test1_school_canteen
  - start_date: date
  - end_date: date
  - store_name: string
  - discount: decimal(9,2)
  - refund: decimal(9,2)
  - net_sales: decimal(9,2)
  - gross_sales: decimal(9,2)
  - txns: int
  - avg_net_sales: decimal(9,2)
  - avg_gross_sales: decimal(9,2)

Table: tip_top_data
  - store_num: string
  - store_name: string
  - product_code: string
  - product_name: string
  - quantity: decimal(9,2)
  - price: decimal(9,2)
  - total_cost: decimal(9,2)
  - week_commencing: date

Table: tip_top_reference
  - reference: string
  - store_name: string

Table: uber_eats_daily_data
  - uber_name: string
  - order_id: string
  - sales_date: date
  - order_status: string
  - sales_excl_tax: decimal(9,2)
  - sales_tax: decimal(9,2)
  - discount_excl_tax: decimal(9,2)
  - discount_tax: decimal(9,2)

Table: veg_product_reference
  - reference: string
  - product_name: string

Table: veg_supplier_fnq
  - store_name: string
  - state: string
  - order_num: string
  - order_date: date
  - product_code: string
  - product_name: string
  - quantity: decimal(9,2)
  - price: decimal(9,2)
  - total_cost: decimal(9,2)

Table: veg_supplier_nsw_act
  - week_commencing: date
  - store_name: string
  - pid: int
  - product_code: string
  - product_name: string
  - quantity: decimal(9,2)
  - price: decimal(9,2)
  - total_cost: decimal(9,2)

###Table: veg_supplier_qld
  - order_date: date
  - store_name: string
  - product_code: string
  - product_name: string
  - quantity: decimal(9,2)
  - price: decimal(9,2)
  - total_cost: decimal(9,2)

###Table: veg_supplier_reference
  - reference: string
  - store_name: string

###Table: veg_supplier_sa
  - week_commencing: date
  - store_name: string
  - product_name: string
  - quantity: decimal(9,2)
  - price: decimal(9,2)
  - total_cost: decimal(9,2)

###Table: veg_supplier_vic
  - week_commencing: date
  - store_name: string
  - store_code: int
  - product_code: string
  - product_name: string
  - quantity: decimal(9,2)
  - price: decimal(9,2)
  - total_cost: decimal(9,2)

###Table: veg_supplier_wa
  - order_date: date
  - order_num: string
  - store_name: string
  - product_code: string
  - product_name: string
  - unit: string
  - quantity: decimal(9,2)
  - price: decimal(9,2)
  - total_cost: decimal(9,2)



"""

# Prompt
SYSTEM_PROMPT = f"""
[Your existing system prompt remains exactly the same]
"""

# Custom CSS for enhanced professional UI
st.markdown("""
    <style>
    :root {
        --primary: #E31937;
        --primary-dark: #B8142C;
        --secondary: #2C3E50;
        --light: #F8F9FA;
        --dark: #212529;
        --gray: #6C757D;
        --success: #28A745;
    }
    
    .main {
        background-color: #ffffff;
    }
    .stApp {
        max-width: 1400px;
        margin: 0 auto;
        padding-top: 1rem;
    }
    .header {
        background: linear-gradient(135deg, var(--primary) 0%, var(--primary-dark) 100%);
        padding: 1.5rem;
        border-radius: 0 0 10px 10px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        color: white;
    }
    .title-container {
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    .title-text {
        color: white;
        font-size: 2.5rem;
        font-weight: 700;
        margin: 0;
        text-shadow: 1px 1px 3px rgba(0,0,0,0.2);
    }
    .subtitle-text {
        color: rgba(255,255,255,0.9);
        font-size: 1.2rem;
        margin: 0.5rem 0 0;
        font-weight: 400;
    }
    .logo-container {
        display: flex;
        align-items: center;
    }
    .logo-img {
        height: 60px;
        margin-right: 1rem;
    }
    .stButton>button {
        background-color: var(--primary);
        color: white;
        border-radius: 8px;
        padding: 0.75rem 1.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
        border: none;
        font-size: 1rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stButton>button:hover {
        background-color: var(--primary-dark);
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
    }
    .query-box {
        background-color: white;
        padding: 1.5rem;
        border-radius: 10px;
        border: 1px solid #E9ECEF;
        margin-bottom: 1.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .result-box {
        background-color: white;
        padding: 1.5rem;
        border-radius: 10px;
        border: 1px solid #E9ECEF;
        margin-top: 1rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .info-box {
        background-color: #F8F9FA;
        padding: 1.25rem;
        border-radius: 8px;
        border-left: 5px solid var(--primary);
        margin-bottom: 1.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .tab-content {
        padding: 1rem 0;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.5rem;
        padding: 0.5rem 0;
    }
    .stTabs [data-baseweb="tab"] {
        height: 45px;
        white-space: nowrap;
        background-color: #F8F9FA;
        border-radius: 8px;
        padding: 0 1.5rem;
        font-weight: 500;
        transition: all 0.3s ease;
        border: 1px solid #E9ECEF;
        margin-right: 0.5rem;
    }
    .stTabs [aria-selected="true"] {
        background-color: var(--primary);
        color: white;
        border-color: var(--primary);
    }
    .stTabs [aria-selected="false"]:hover {
        background-color: #E9ECEF;
    }
    .copy-button {
        background-color: var(--primary);
        color: white;
        border: none;
        padding: 0.5rem 1rem;
        border-radius: 6px;
        cursor: pointer;
        font-size: 0.9rem;
        font-weight: 500;
        transition: all 0.3s ease;
        display: flex;
        align-items: center;
        gap: 0.5rem;
        margin-top: 1rem;
    }
    .copy-button:hover {
        background-color: var(--primary-dark);
        transform: translateY(-1px);
    }
    .sidebar .sidebar-content {
        background-color: #F8F9FA;
        padding: 1.5rem;
        border-right: 1px solid #E9ECEF;
    }
    .quick-tip {
        background-color: white;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 1rem;
        border: 1px solid #E9ECEF;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .quick-tip h3 {
        color: var(--primary);
        margin-top: 0;
        font-size: 1.1rem;
    }
    .footer {
        text-align: center;
        color: var(--gray);
        padding: 1.5rem;
        margin-top: 2rem;
        border-top: 1px solid #E9ECEF;
    }
    .divider {
        border: 0;
        height: 1px;
        background: linear-gradient(to right, transparent, #E9ECEF, transparent);
        margin: 1.5rem 0;
    }
    .quick-query-btn {
        width: 100%;
        margin-bottom: 0.5rem;
        background-color: white;
        color: var(--dark);
        border: 1px solid #E9ECEF;
        transition: all 0.3s ease;
    }
    .quick-query-btn:hover {
        background-color: #F8F9FA;
        border-color: var(--primary);
        color: var(--primary);
    }
    .stTextArea textarea {
        min-height: 120px;
    }
    .stRadio > div {
        flex-direction: row;
        gap: 1rem;
    }
    .stRadio [role="radiogroup"] {
        gap: 1rem;
    }
    .stRadio [role="radio"] {
        margin-right: 0.5rem;
    }
    </style>
    """, unsafe_allow_html=True)

# Header with logo
st.markdown("""
    <div class="header">
        <div class="title-container">
            <div>
                <h1 class="title-text">Roll'd SQL Assistant</h1>
                <p class="subtitle-text">Transform business questions into actionable insights</p>
            </div>
            <div class="logo-container">
                <img src="D:\rolde chatbot\logo rolde.webp" class="logo-img">
            </div>
        </div> 
    </div>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("### Navigation")
    st.markdown("---")
    
    # Query Type Selection
    query_type = st.radio(
        "Select Query Type:",
        ["SQL Query", "Data Visualization"],
        index=0,
        key="query_type"
    )
    
    st.markdown("---")
    st.markdown("### Quick Tips")
    with st.container():
        st.markdown("""
        <div class="quick-tip">
            <h3>💡 Asking Effective Questions</h3>
            <ul style="padding-left: 1.2rem; margin-bottom: 0;">
                <li>Be specific about time periods</li>
                <li>Mention store names if relevant</li>
                <li>Focus on one question at a time</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown("### Common Queries")
    if st.button("📊 Daily Sales by Store", key="daily_sales", use_container_width=True, type="secondary"):
        st.session_state.user_question = "Show me the daily sales for all stores in the last 7 days"
    if st.button("🏆 Top Selling Items", key="top_items", use_container_width=True, type="secondary"):
        st.session_state.user_question = "What are the top 10 selling items this month?"
    if st.button("📈 Store Performance", key="store_perf", use_container_width=True, type="secondary"):
        st.session_state.user_question = "Compare store performance by net sales for the current month"
    if st.button("🛒 Inventory Analysis", key="inventory", use_container_width=True, type="secondary"):
        st.session_state.user_question = "Show me inventory movement by product category last quarter"

# Main content container
with st.container():
    # Info box
    st.markdown("""
        <div class="info-box">
            <div style="display: flex; align-items: center; gap: 1rem;">
                <div style="font-size: 1.5rem;">🔍</div>
                <div>
                    <h3 style="margin: 0 0 0.5rem 0; color: var(--secondary);">How to use this tool</h3>
                    <p style="margin: 0;">Ask natural language questions about sales, inventory, or store performance. The assistant will generate SQL queries and visualizations using the Roll'd sales database.</p>
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)

    # Query input
    st.markdown('<div class="query-box">', unsafe_allow_html=True)
    user_question = st.text_area(
        "Enter your business question:",
        value=st.session_state.get('user_question', ''),
        height=150,
        placeholder="Example: What were the total sales for each store last month?",
        key="user_question_input"
    )
    st.markdown('</div>', unsafe_allow_html=True)

    # Generate button centered
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        generate_button = st.button(
            "Generate Insights", 
            use_container_width=True,
            key="generate_button",
            type="primary"
        )

    if generate_button:
        if not user_question.strip():
            st.warning("Please enter a question to generate insights.")
        else:
            with st.spinner("Analyzing your question and generating insights..."):
                # [Rest of your existing processing logic remains exactly the same]
                # Only the UI components are being modified, not the functionality
                
                # First, perform table analysis
                table_analysis = ""
                relevant_tables = []
                for table in DATABASE_SCHEMA.splitlines():
                    if user_question.lower() in table.lower():
                        relevant_tables.append(table)
                        table_analysis += f"- {table}\n"
                
                # If no relevant tables found, show warning
                if "No relevant tables found in schema" in table_analysis:
                    st.warning("""
                    ⚠️ No relevant tables found in the schema to answer this question.
                    Please rephrase your question using data from the available tables.
                    """)
                    st.markdown("### Available Tables:")
                    st.markdown(DATABASE_SCHEMA)
                else:
                    # Now generate the actual query
                    visualization_prompt = f"""Generate both SQL query and Python visualization code using Plotly. 
                    The visualization code should be Databricks-friendly and include clear comments.
                    
                    Based on the table analysis:
                    {table_analysis}
                    
                    IMPORTANT: 
                    - Only use the tables identified above
                    - Only use columns that exist in these tables
                    - Do not assume any relationships not shown in the schema
                    - Always start the SQL query with 'USE sales;'
                    
                    Format the response as follows:

                    ```sql
                    USE sales;
                    [SQL QUERY HERE]
                    ```

                    ```python
                    # Databricks-friendly visualization code
                    [PYTHON CODE HERE]
                    ```"""

                    prompt = f"""
You are a helpful assistant that converts business questions into SQL queries and Python visualization code.
IMPORTANT: Only use tables and columns from the schema below. Do not assume or create any tables or columns.

Schema:
{DATABASE_SCHEMA}

Question:
{user_question}

Table Analysis:
{table_analysis}

{visualization_prompt if query_type == 'Data Visualization' else 'Generate SQL query only. IMPORTANT: Only use the tables identified in the analysis above. Always start the SQL query with USE sales;'}:"""

                    response = client.chat.completions.create(
                        model=DEPLOYMENT_NAME,
                        messages=[
                            {
                                "role": "system", 
                                "content": """You are an expert data analyst who writes correct and efficient SQL queries and creates beautiful visualizations.
                                IMPORTANT: 
                                - Only use tables and columns from the provided schema
                                - Do not assume or create any tables or columns
                                - Validate all table and column names against the schema
                                - Always start SQL queries with 'USE sales;'
                                - For visualizations, always use Plotly and include clear comments
                                - The code should be ready to run in Databricks
                                - Always include proper error handling and data validation"""
                            },
                            {"role": "user", "content": prompt}
                        ],
                        temperature=0,
                        max_tokens=1000
                    )

                    result = response.choices[0].message.content.strip()
                    
                    # Display results in tabs
                    tab1, tab2, tab3 = st.tabs(["🧐 Table Analysis", "💻 SQL Query", "📊 Visualization"])
                    
                    with tab1:
                        st.markdown('<div class="result-box">', unsafe_allow_html=True)
                        st.markdown("### Relevant Tables Identified")
                        st.markdown("These tables contain data relevant to your question:")
                        st.markdown(table_analysis)
                        st.markdown('</div>', unsafe_allow_html=True)
                    
                    with tab2:
                        st.markdown('<div class="result-box">', unsafe_allow_html=True)
                        st.markdown("### Generated SQL Query")
                        sql_code = result.split("```python")[0].replace("```sql", "").strip()
                        # Ensure USE sales; is at the beginning
                        if not sql_code.strip().startswith("USE sales;"):
                            sql_code = "USE sales;\n" + sql_code
                        st.code(sql_code, language="sql")
                        
                        st.markdown("""
                        <div style="margin-top: 1.5rem;">
                            <h4>How to use this query:</h4>
                            <ol>
                                <li>Copy the SQL query above</li>
                                <li>Paste it into a Databricks notebook cell</li>
                                <li>Run the cell to execute the query</li>
                                <li>Results will be displayed automatically</li>
                            </ol>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        st.markdown('</div>', unsafe_allow_html=True)
                    
                    if query_type == "Data Visualization":
                        with tab3:
                            st.markdown('<div class="result-box">', unsafe_allow_html=True)
                            st.markdown("### Generated Visualization Code")
                            
                            # Extract Python code if it exists, otherwise generate a default visualization
                            if "```python" in result:
                                python_code = result.split("```python")[1].split("```")[0].strip()
                            else:
                                python_code = """
# Databricks-friendly visualization code
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

try:
    # First, set the database context
    spark.sql("USE sales")
    
    # Main query to get the data
    query = '''
    SELECT 
        store_name, 
        SUM(net_sales) AS total_net_sales
    FROM 
        abacus_daily_summary
    WHERE 
        start_date >= DATE_TRUNC('month', CURRENT_DATE)
        AND end_date <= LAST_DAY(CURRENT_DATE)
    GROUP BY 
        store_name
    ORDER BY 
        total_net_sales DESC
    '''

    # Execute the query using spark.sql() and convert to pandas
    df = spark.sql(query)
    pdf = df.toPandas()

    # Create a bar chart using Plotly
    fig = px.bar(
        pdf, 
        x='store_name', 
        y='total_net_sales',
        title='Net Sales Performance of All Stores for the Current Month',
        labels={
            'store_name': 'Store Name',
            'total_net_sales': 'Total Net Sales'
        },
        template='plotly_white'
    )

    # Customize the layout
    fig.update_layout(
        xaxis_title='Store Name',
        yaxis_title='Total Net Sales',
        showlegend=False,
        height=600
    )

    # Show the plot
    fig.show()

except Exception as e:
    print(f"Error creating visualization: {str(e)}")
"""
                            
                            st.code(python_code, language="python")
                            
                            # Add Databricks-specific instructions
                            st.markdown("""
                            <div style="margin-top: 1.5rem;">
                                <h4>How to use this visualization code:</h4>
                                <ol>
                                    <li>Copy the Python code above</li>
                                    <li>Paste it into a Databricks notebook cell</li>
                                    <li>Run the cell to execute the code</li>
                                    <li>The visualization will be displayed automatically</li>
                                </ol>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # Add a copy button
                            st.markdown("""
                            <div style="margin-top: 1.5rem;">
                                <button class="copy-button" onclick="navigator.clipboard.writeText(document.querySelectorAll('pre code')[1].textContent)">
                                    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" viewBox="0 0 16 16">
                                        <path fill-rule="evenodd" d="M4 2a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V2zm2-1a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1V2a1 1 0 0 0-1-1H6zM2 5a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1v-1h1v1a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h1v1H2z"/>
                                    </svg>
                                    Copy Visualization Code
                                </button>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            st.markdown('</div>', unsafe_allow_html=True)

# Footer
st.markdown("""
    <div class="footer">
        <p>© 2025 Roll'd Analytics Platform | Version 1.0.0</p>
        <p style="font-size: 0.9rem; margin-top: 0.5rem;">For authorized use only. All data is confidential.</p>
    </div>
""", unsafe_allow_html=True)