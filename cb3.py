import streamlit as st
from openai import AzureOpenAI
import os
from dotenv import load_dotenv
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import time
import re

# Helper functions
def extract_code_from_response(response, language):
    """Extract code blocks of a specific language from the response"""
    if language == "sql":
        if "```sql" in response:
            return response.split("```sql")[1].split("```")[0].strip()
        return response.split("```")[0].strip()
    elif language == "python":
        if "```python" in response:
            return response.split("```python")[1].split("```")[0].strip()
        return ""
    return ""

def execute_sql_query(sql_query):
    """Execute SQL query in Databricks and return results as pandas DataFrame"""
    try:
        conn = connect_to_databricks()
        
        if not conn:
            return None, "Could not connect to Databricks. Please check your credentials."
        
        # Split the query into USE statement and actual query
        if "USE sales;" in sql_query:
            use_stmt, actual_query = sql_query.split("USE sales;")
            # Execute USE statement first
            with conn.cursor() as cursor:
                cursor.execute("USE sales;")
                
                # Set legacy time parser policy to handle date formats
                cursor.execute("SET spark.sql.legacy.timeParserPolicy=LEGACY;")
                
                # Replace problematic date patterns with Spark 3.0 compatible ones
                modified_query = actual_query.strip()
                
                # Fix date interval syntax
                modified_query = modified_query.replace("date('now', '-7 days')", "date_sub(current_date(), 7)")
                modified_query = modified_query.replace("date('now', '-30 days')", "date_sub(current_date(), 30)")
                modified_query = modified_query.replace("date('now', '-1 month')", "date_sub(current_date(), 30)")
                modified_query = modified_query.replace("date('now', '-3 months')", "date_sub(current_date(), 90)")
                modified_query = modified_query.replace("date('now', '-6 months')", "date_sub(current_date(), 180)")
                modified_query = modified_query.replace("date('now', '-1 year')", "date_sub(current_date(), 365)")
                
                # Fix date format patterns
                modified_query = modified_query.replace("'%Y-%m-01'", "'yyyy-MM-01'")
                modified_query = modified_query.replace("'%Y-%m-%d'", "'yyyy-MM-dd'")
                modified_query = modified_query.replace("'%Y-%m'", "'yyyy-MM'")
                modified_query = modified_query.replace("'%Y'", "'yyyy'")
                
                # Fix comparison operators
                modified_query = modified_query.replace("≥", ">=")
                modified_query = modified_query.replace("≤", "<=")
                
                # Execute the modified query
                cursor.execute(modified_query)
                result_df = cursor.fetchall_arrow().to_pandas()
                return result_df, None
        else:
            with conn.cursor() as cursor:
                # Set legacy time parser policy
                cursor.execute("SET spark.sql.legacy.timeParserPolicy=LEGACY;")
                
                # Replace problematic date patterns
                modified_query = sql_query
                
                # Fix date interval syntax
                modified_query = modified_query.replace("date('now', '-7 days')", "date_sub(current_date(), 7)")
                modified_query = modified_query.replace("date('now', '-30 days')", "date_sub(current_date(), 30)")
                modified_query = modified_query.replace("date('now', '-1 month')", "date_sub(current_date(), 30)")
                modified_query = modified_query.replace("date('now', '-3 months')", "date_sub(current_date(), 90)")
                modified_query = modified_query.replace("date('now', '-6 months')", "date_sub(current_date(), 180)")
                modified_query = modified_query.replace("date('now', '-1 year')", "date_sub(current_date(), 365)")
                
                # Fix date format patterns
                modified_query = modified_query.replace("'%Y-%m-01'", "'yyyy-MM-01'")
                modified_query = modified_query.replace("'%Y-%m-%d'", "'yyyy-MM-dd'")
                modified_query = modified_query.replace("'%Y-%m'", "'yyyy-MM'")
                modified_query = modified_query.replace("'%Y'", "'yyyy'")
                
                # Fix comparison operators
                modified_query = modified_query.replace("≥", ">=")
                modified_query = modified_query.replace("≤", "<=")
                
                cursor.execute(modified_query)
                result_df = cursor.fetchall_arrow().to_pandas()
                return result_df, None
    except Exception as e:
        import traceback
        return None, f"Error executing SQL: {str(e)}\n{traceback.format_exc()}"

def execute_visualization_code(python_code, data_df):
    """Execute visualization code and return the plotly figure"""
    try:
        # Create a sandbox environment with the dataframe already loaded
        namespace = {
            "pd": pd, 
            "px": px, 
            "go": go, 
            "df": data_df,  # The dataframe from the SQL query
            "fig": None
        }
        
        # If no visualization code provided, create a default visualization
        if not python_code or python_code.strip() == "":
            # Determine the best visualization type based on the data
            if len(data_df.columns) == 2:  # If we have exactly 2 columns
                x_col, y_col = data_df.columns
                if data_df[y_col].dtype in ['int64', 'float64']:  # If y-axis is numeric
                    if len(data_df) <= 10:  # If we have 10 or fewer categories
                        # Create a pie chart
                        fig = px.pie(
                            data_df,
                            names=x_col,
                            values=y_col,
                            title=f"{y_col} by {x_col}",
                            template="plotly_white"
                        )
                    else:
                        # Create a bar chart
                        fig = px.bar(
                            data_df,
                            x=x_col,
                            y=y_col,
                            title=f"{y_col} by {x_col}",
                            template="plotly_white"
                        )
                else:
                    # Create a bar chart for non-numeric y-axis
                    fig = px.bar(
                        data_df,
                        x=x_col,
                        y=y_col,
                        title=f"{y_col} by {x_col}",
                        template="plotly_white"
                    )
            else:
                # Create a line chart for time series or multiple columns
                fig = px.line(
                    data_df,
                    title="Data Visualization",
                    template="plotly_white"
                )
            
            # Customize the layout
            fig.update_layout(
                height=600,
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            return fig, None
        
        # If visualization code is provided, execute it
        modified_code = python_code.replace("fig.show()", "")
        
        # Remove any Databricks or pyspark imports or references
        modified_code = modified_code.replace("from pyspark.sql import *", "")
        modified_code = modified_code.replace("from pyspark import *", "")
        modified_code = modified_code.replace("import pyspark", "")
        modified_code = modified_code.replace("from databricks.koalas import *", "")
        modified_code = modified_code.replace("import databricks.koalas", "")
        modified_code = modified_code.replace("import koalas", "")
        
        # Convert any koalas DataFrame operations to pandas
        modified_code = modified_code.replace("koalas.DataFrame", "pd.DataFrame")
        modified_code = modified_code.replace("koalas.Series", "pd.Series")
        
        # Remove any direct database connection code
        modified_code = modified_code.replace("connection.execute", "df")
        modified_code = modified_code.replace("cursor.execute", "df")
        modified_code = modified_code.replace("pd.read_sql", "df")
        
        # Add final line to ensure fig is assigned
        if "fig = " not in modified_code:
            if "px." in modified_code:
                fig_creation_pattern = r"(px\.[a-zA-Z_]+\([^)]+\))"
                matches = re.findall(fig_creation_pattern, modified_code)
                if matches:
                    modified_code += "\nfig = " + matches[0]
            elif "go.Figure(" in modified_code:
                fig_creation_pattern = r"(go\.Figure\([^)]+\))"
                matches = re.findall(fig_creation_pattern, modified_code)
                if matches:
                    modified_code += "\nfig = " + matches[0]
        
        # Execute the code
        exec(modified_code, namespace)
        
        # Return the figure if it was created
        if namespace["fig"] is not None:
            return namespace["fig"], None
        else:
            return None, "No visualization figure was created"
    except Exception as e:
        import traceback
        return None, f"Error executing visualization: {str(e)}\n{traceback.format_exc()}"

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
@st.cache_resource
def get_openai_client():
    try:
        api_key = os.getenv("AZURE_OPENAI_KEY")
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        
        if not api_key or not endpoint:
            st.warning("Azure OpenAI credentials not configured properly.")
            return None
            
        return AzureOpenAI(
            api_key=api_key,
            api_version="2023-07-01-preview",
            azure_endpoint=endpoint
        )
    except Exception as e:
        st.error(f"Error connecting to Azure OpenAI: {str(e)}")
        return None

# Try to get the client, but allow the app to continue if it fails
client = get_openai_client()
DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4")
    
# Databricks SQL Connection
@st.cache_resource(ttl=600)
def connect_to_databricks():
    """Establish connection to Databricks SQL"""
    try:
        from databricks import sql
        
        # Get connection parameters from environment variables
        server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        http_path = os.getenv("DATABRICKS_HTTP_PATH")
        access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
        
        # Check if credentials are available
        if not server_hostname:
            st.error("DATABRICKS_SERVER_HOSTNAME is not set in environment variables")
            return None
        if not http_path:
            st.error("DATABRICKS_HTTP_PATH is not set in environment variables")
            return None
        if not access_token:
            st.error("DATABRICKS_ACCESS_TOKEN is not set in environment variables")
            return None
            
        # Create connection
        try:
            connection = sql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
                access_token=access_token
            )
            return connection
        except Exception as conn_error:
            st.error(f"Failed to connect to Databricks: {str(conn_error)}")
            st.info("Please check your Databricks credentials in the .env file")
            return None
    except ImportError:
        st.error("databricks-sql-connector package is not installed. Please install it using: pip install databricks-sql-connector")
        return None
    except Exception as e:
        st.error(f"Unexpected error connecting to Databricks: {str(e)}")
        return None

# Add a function to check connection status
def check_databricks_connection():
    """Check if Databricks connection is properly configured"""
    required_vars = [
        "DATABRICKS_SERVER_HOSTNAME",
        "DATABRICKS_HTTP_PATH",
        "DATABRICKS_ACCESS_TOKEN"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        st.error("Missing Databricks credentials:")
        for var in missing_vars:
            st.error(f"- {var} is not set")
        st.info("Please set these variables in your .env file")
        return False
    
    return True

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
SYSTEM_PROMPT = """You are an expert data analyst who writes correct and efficient SQL queries and creates beautiful visualizations.
IMPORTANT: 
- Only use tables that exist in the schema
- Validate all table and column names against the schema
- Always start SQL queries with 'USE sales;'
- For date formatting, use 'yyyy-MM-dd' instead of '%Y-%m-%d'
- For month formatting, use 'yyyy-MM' instead of '%Y-%m'
- For year formatting, use 'yyyy' instead of '%Y'
- For date intervals, use date_sub(current_date(), n) where n is the number of days
- Use >= instead of ≥ for greater than or equal to
- Use <= instead of ≤ for less than or equal to
- For visualizations:
  - Use only pandas and plotly
  - Work with the dataframe 'df' that contains the SQL query results
  - Do not try to connect to the database directly
  - Create visualizations using the data in the dataframe
- Do not use any Databricks-specific libraries
- Always include proper error handling and data validation"""

# Custom CSS for enhanced professional UI
st.markdown("""
    <style>
    :root {
        --primary: #10a37f;
        --primary-dark: #0d8c6d;
        --secondary: #2C3E50;
        --light: #F8F9FA;
        --dark: #212529;
        --gray: #6C757D;
        --background: #ffffff;
        --border: #e5e5e5;
    }

    .main {
        background-color: var(--background);
    }
    .stApp {
        max-width: 1000px;
        margin: 0 auto;
        padding-top: 1rem;
    }
    .header {
        background: var(--background);
        padding: 1rem 0;
        margin-bottom: 1.5rem;
        border-bottom: 1px solid var(--border);
    }
    .title-container {
        display: flex;
        align-items: center;
        justify-content: flex-start;
        max-width: 1000px;
        margin: 0 auto;
        padding: 0 1rem;
        gap: 2rem;
    }
    .logo-container {
        display: flex;
        align-items: center;
        min-width: 120px;
        border: 1px solid transparent; /* Debug border */
    }
    .logo-img {
        height: 40px;
        width: auto;
        object-fit: contain;
        display: block; /* Ensure image is displayed as block */
    }
    .title-text {
        color: var(--dark);
        font-size: 1.5rem;
        font-weight: 600;
        margin: 0;
    }
    .subtitle-text {
        color: var(--gray);
        font-size: 0.9rem;
        margin: 0.25rem 0 0;
        font-weight: 400;
    }
    .stButton>button {
        background-color: var(--primary);
        color: white;
        border-radius: 4px;
        padding: 0.5rem 1rem;
        font-weight: 500;
        transition: all 0.2s ease;
        border: none;
        font-size: 0.9rem;
    }
    .stButton>button:hover {
        background-color: var(--primary-dark);
    }
    .query-box {
        background-color: var(--background);
        padding: 1.25rem;
        border-radius: 6px;
        border: 1px solid var(--border);
        margin-bottom: 1.25rem;
    }
    .result-box {
        background-color: var(--background);
        padding: 1.25rem;
        border-radius: 6px;
        border: 1px solid var(--border);
        margin-top: 1rem;
    }
    .info-box {
        background-color: var(--background);
        padding: 1rem;
        border-radius: 6px;
        border: 1px solid var(--border);
        margin-bottom: 1.25rem;
    }
    .tab-content {
        padding: 0.75rem 0;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.5rem;
        padding: 0.5rem 0;
    }
    .stTabs [data-baseweb="tab"] {
        height: 36px;
        white-space: nowrap;
        background-color: var(--background);
        border-radius: 4px;
        padding: 0 1rem;
        font-weight: 500;
        transition: all 0.2s ease;
        border: 1px solid var(--border);
        margin-right: 0.5rem;
        font-size: 0.9rem;
    }
    .stTabs [aria-selected="true"] {
        background-color: var(--primary);
        color: white;
        border-color: var(--primary);
    }
    .stTabs [aria-selected="false"]:hover {
        background-color: var(--light);
    }
    .sidebar .sidebar-content {
        background-color: var(--background);
        padding: 1.25rem;
        border-right: 1px solid var(--border);
    }
    .quick-tip {
        background-color: var(--background);
        border-radius: 6px;
        padding: 0.75rem;
        margin-bottom: 0.75rem;
        border: 1px solid var(--border);
    }
    .quick-tip h3 {
        color: var(--primary);
        margin-top: 0;
        font-size: 0.9rem;
    }
    .footer {
        text-align: center;
        color: var(--gray);
        padding: 1rem;
        margin-top: 1.5rem;
        border-top: 1px solid var(--border);
        font-size: 0.8rem;
    }
    .stTextArea textarea {
        min-height: 100px;
        border-radius: 6px;
        border: 1px solid var(--border);
    }
    </style>
    """, unsafe_allow_html=True)

# Header with logo
st.markdown("""
    <div class="header">
        <div class="title-container">
            <div class="logo-container">
                <img src="rollde1.png" class="logo-img" alt="Roll'd Logo" 
                     onerror="this.onerror=null; console.log('Logo failed to load: rollde1.png'); this.style.display='none';"
                     onload="console.log('Logo loaded successfully')">
            </div>
            <div>
                <h1 class="title-text">Roll'd AI Analyst</h1>
                <p class="subtitle-text">Transform business questions into actionable insights</p>
            </div>
        </div> 
    </div>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("### Navigation")
    st.markdown("---")
    
    # Check Databricks connection status
    if all([os.getenv(key) for key in ["DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_ACCESS_TOKEN"]]):
        try:
            connection = connect_to_databricks()
            if connection:
                st.success("✅ Connected to Databricks")
            else:
                st.warning("⚠️ Databricks connection failed")
        except Exception as e:
            st.error(f"⚠️ Databricks connection error: {str(e)}")
    else:
        st.info("ℹ️ Databricks credentials not configured")
    
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

# Main content container
with st.container():
    # Info box
    st.markdown("""
        <div class="info-box">
            <div style="display: flex; align-items: center; gap: 1rem;">
                <div style="font-size: 1.2rem;">🔍</div>
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
        height=100,
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
            # Check Databricks connection first
            if not check_databricks_connection():
                st.error("Cannot proceed without proper Databricks connection. Please configure your credentials first.")
            else:
                with st.spinner("Analyzing your question and generating insights..."):
                    # First, perform table analysis
                    table_analysis = ""
                    relevant_tables = []
                    
                    # Define keywords for different types of queries
                    sales_keywords = ['sales', 'revenue', 'income', 'earnings', 'revenue']
                    item_keywords = ['item', 'product', 'menu', 'food', 'drink', 'quantity', 'items sold']
                    store_keywords = ['store', 'location', 'branch', 'outlet']
                    date_keywords = ['daily', 'weekly', 'monthly', 'yearly', 'date', 'period', 'time']
                    
                    # Analyze the question to determine relevant tables
                    question_lower = user_question.lower()
                    
                    # Check for sales-related queries
                    if any(keyword in question_lower for keyword in sales_keywords):
                        relevant_tables.extend([
                            "abacus_3pl_daily_summary",
                            "abacus_cash_card_daily_summary",
                            "abacus_grocery_daily_summary",
                            "app_web_daily_summary",
                            "hms_host_daily_summary",
                            "master",
                            "ro_master"
                        ])
                    
                    # Check for item-related queries
                    if any(keyword in question_lower for keyword in item_keywords):
                        relevant_tables.extend([
                            "abacus_daily_data",
                            "abacus_item_weekly_summary",
                            "abacus_weekly_variants",
                            "rolld_prop_daily_data"
                        ])
                    
                    # Check for store-related queries
                    if any(keyword in question_lower for keyword in store_keywords):
                        relevant_tables.extend([
                            "store_info",
                            "ro_store_info"
                        ])
                    
                    # Check for date-related queries
                    if any(keyword in question_lower for keyword in date_keywords):
                        relevant_tables.extend([
                            "retail_calendar"
                        ])
                    
                    # Remove duplicates and sort
                    relevant_tables = sorted(list(set(relevant_tables)))
                    
                    # Create table analysis
                    if relevant_tables:
                        table_analysis = "These tables contain data relevant to your question:\n\n"
                        for table in relevant_tables:
                            # Find the table definition in the schema
                            table_def = next((line for line in DATABASE_SCHEMA.splitlines() 
                                            if line.strip().startswith(f"###Table: {table}") or 
                                            line.strip().startswith(f"Table: {table}")), None)
                            if table_def:
                                table_analysis += f"- {table_def}\n"
                    else:
                        table_analysis = "No relevant tables found for your question. Please try rephrasing your question using the available tables."

                    # If no relevant tables found, show warning
                    if "No relevant tables found" in table_analysis:
                        st.warning("""
                        ⚠️ No relevant tables found in the schema to answer this question.
                        Please rephrase your question using the available tables.
                        """)
                        st.markdown("### Available Tables:")
                        st.markdown(DATABASE_SCHEMA)
                    else:
                        # Now generate the actual query
                        visualization_prompt = f"""Generate both SQL query and Python visualization code using Plotly. 
                        The visualization code should use pandas and plotly only, with clear comments.

                        Based on the table analysis:
                        {table_analysis}

                        IMPORTANT: 
                        - Only use tables that exist in the schema
                        - Validate all table and column names against the schema
                        - Always start the SQL query with 'USE sales;'
                        - For date formatting, use 'yyyy-MM-dd' instead of '%Y-%m-%d'
                        - For month formatting, use 'yyyy-MM' instead of '%Y-%m'
                        - For year formatting, use 'yyyy' instead of '%Y'
                        - For date intervals, use date_sub(current_date(), n) where n is the number of days
                        - Use >= instead of ≥ for greater than or equal to
                        - Use <= instead of ≤ for less than or equal to
                        - For visualizations:
                          - Use only pandas and plotly
                          - Work with the dataframe 'df' that contains the SQL query results
                          - Do not try to connect to the database directly
                          - Create visualizations using the data in the dataframe
                        - Do not use any Databricks-specific libraries

                        Format the response as follows:

                        ```sql
                        USE sales;
                        [SQL QUERY HERE]
                        ```

                        ```python
                        # Visualization code using pandas and plotly
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

{visualization_prompt}:"""

                        # Check if OpenAI client is available
                        if client:
                            try:
                                response = client.chat.completions.create(
                                    model=DEPLOYMENT_NAME,
                                    messages=[
                                        {
                                            "role": "system", 
                                            "content": SYSTEM_PROMPT
                                        },
                                        {"role": "user", "content": prompt}
                                    ],
                                    temperature=0,
                                    max_tokens=1000
                                )
                                
                                result = response.choices[0].message.content.strip()
                            except Exception as e:
                                st.error(f"Error generating response: {str(e)}")
                                result = """```sql
USE sales;
-- Sample query (OpenAI API error occurred)
SELECT 
    store_name, 
    SUM(net_sales) AS total_sales
FROM 
    abacus_daily_summary
GROUP BY 
    store_name
ORDER BY 
    total_sales DESC
LIMIT 10
```"""
                        else:
                            st.warning("Azure OpenAI client not available. Please check your API credentials.")
                            result = """```sql
USE sales;
-- Sample query (OpenAI API not configured)
SELECT 
    store_name, 
    SUM(net_sales) AS total_sales
FROM 
    abacus_daily_summary
GROUP BY 
    store_name
ORDER BY 
    total_sales DESC
LIMIT 10
```"""

                        # Extract code
                        sql_code = extract_code_from_response(result, "sql")
                        python_code = extract_code_from_response(result, "python")

                        if sql_code and not sql_code.strip().startswith("USE sales;"):
                            sql_code = "USE sales;\n" + sql_code

                        st.session_state.sql_query = sql_code
                        st.session_state.python_viz_code = python_code

                        # Execute SQL
                        if all([os.getenv(k) for k in ["DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_ACCESS_TOKEN"]]):
                            try:
                                query_results, error = execute_sql_query(sql_code)
                                if error:
                                    st.session_state.execution_error = error
                                    st.session_state.query_results = None
                                else:
                                    st.session_state.query_results = query_results
                                    st.session_state.execution_error = None

                                    # Visualization
                                    if python_code:
                                        fig, viz_error = execute_visualization_code(python_code, query_results)
                                        if viz_error:
                                            st.session_state.execution_error = viz_error
                                            st.session_state.visualization_fig = None
                                        else:
                                            st.session_state.visualization_fig = fig
                            except Exception as e:
                                import traceback
                                st.session_state.execution_error = f"Execution error: {str(e)}\n{traceback.format_exc()}"
                        else:
                            st.session_state.execution_error = "Databricks credentials not configured."

                        # Result tabs
                        if st.session_state.get("table_analysis") or st.session_state.get("sql_query"):
                            tabs = ["🧐 Table Analysis", "💻 SQL Query", "📊 Data Table", "📈 Visualization"]
                            selected_tabs = st.tabs(tabs)

                            with selected_tabs[0]:
                                st.markdown('<div class="result-box">', unsafe_allow_html=True)
                                st.markdown("### Relevant Tables Identified")
                                
                                if table_analysis:
                                    st.markdown("These tables contain data relevant to your question:")
                                    st.markdown(table_analysis)
                                    
                                    # Add table descriptions
                                    st.markdown("### Table Descriptions")
                                    for table in relevant_tables:
                                        table_name = table.split(':')[0].strip()
                                        st.markdown(f"**{table_name}**: Contains data about {table_name.lower().replace('_', ' ')}")
                                else:
                                    st.warning("No relevant tables found for your question. Please try rephrasing your question using the available tables.")
                                    st.markdown("### Available Tables:")
                                    st.markdown(DATABASE_SCHEMA)
                                
                                st.markdown('</div>', unsafe_allow_html=True)

                            with selected_tabs[1]:
                                st.markdown("### Generated SQL Query")
                                st.code(st.session_state.sql_query, language="sql")

                            with selected_tabs[2]:
                                if st.session_state.execution_error:
                                    st.error(st.session_state.execution_error)
                                elif st.session_state.get("query_results") is not None:
                                    st.success("Query executed successfully!")
                                    st.dataframe(st.session_state.query_results)
                                    csv = st.session_state.query_results.to_csv(index=False)
                                    st.download_button("Download as CSV", data=csv, file_name="query_results.csv", mime="text/csv")
                                else:
                                    st.info("Click 'Generate Insights' to run the query.")

                            with selected_tabs[3]:
                                st.markdown("### Data Visualization")
                                if st.session_state.execution_error:
                                    st.error(st.session_state.execution_error)
                                elif st.session_state.get("visualization_fig"):
                                    st.plotly_chart(st.session_state.visualization_fig, use_container_width=True)
                                else:
                                    st.info("No visualization generated.")
                                    st.markdown("### Generated Python Code")            
                                    st.code(st.session_state.python_viz_code, language="python")

# Footer
st.markdown("""
    <div class="footer">
        <p>© 2025 Roll'd Analytics Platform | Version 1.0.0</p>
        <p style="font-size: 0.9rem; margin-top: 0.5rem;">For authorized use only. All data is confidential.</p>
    </div>
""", unsafe_allow_html=True)

# Initialize session state for results
if 'query_results' not in st.session_state:
    st.session_state.query_results = None
if 'visualization_fig' not in st.session_state:
    st.session_state.visualization_fig = None
if 'table_analysis' not in st.session_state:
    st.session_state.table_analysis = ""
if 'sql_query' not in st.session_state:
    st.session_state.sql_query = ""
if 'python_viz_code' not in st.session_state:
    st.session_state.python_viz_code = ""
if 'execution_error' not in st.session_state:
    st.session_state.execution_error = None