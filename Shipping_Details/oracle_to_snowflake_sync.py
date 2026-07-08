import oracledb
import snowflake.connector
import pandas as pd
import numpy as np 

# ---------------Orcal Connector----------------
oracle_conn = oracledb.connect(
    user='system',
    password='InushkiK03',
    dsn='localhost:1521/XE'
)

oracle_query = """
SELECT
    PRODUCT_TYPE,
    SKU,
    AVAILABILITY,
    NUMBER_OF_PRODUCTS_SOLD,
    STOCK_LEVELS,
    LEAD_TIME,
    ORDER_QUANTITIES,
    SHIPPING_CARRIERS,
    SUPPLIER_NAME,
    TRANSPORTATION_MODES,
    ORDER_DATE,
    SHIPPING_DATE,
    PO_DATE,
    PO_APPROVE_DATE,
    INVOICE_DATE,
    GAN_DATE,
    ACTUAL_LEAD_TIME_DAYS,
    PLANNED_LEAD_TIME_DAYS,
    DELIVERY_STATUS,
    RECEIVED_QUANTITY,
    PENDING_DELIVERY_QTY,
    EXPECTED_GAN_DATE,
    DATA_QUALITY_FLAG,
    ETA_HOURS
FROM SUPPLY_CHAIN_STAGE
"""

df= pd.read_sql(oracle_query, oracle_conn)

# Convert Oracle date/timestamp columns to string format for Snowflake
date_columns = [
    "ORDER_DATE",
    "SHIPPING_DATE",
    "PO_DATE",
    "PO_APPROVE_DATE",
    "INVOICE_DATE",
    "GAN_DATE",
    "EXPECTED_GAN_DATE"
]

for col in date_columns:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

# Convert dataframe to object type
df = df.astype(object)

# Replace all NaN / NaT / missing values with None
df = df.replace({np.nan: None})
df = df.where(pd.notnull(df), None)

data = []

for row in df.itertuples(index=False, name=None):
    clean_row = []
    for value in row:
        if pd.isna(value):
            clean_row.append(None)
        else:
            clean_row.append(value)
    data.append(tuple(clean_row))


# ---------------Snowflake Connector----------------
snowflake_conn = snowflake.connector.connect(
    user='KandyGirls',
    password='KandyGirls12345',
    account='OUUPYYP-PG24859',
    warehouse='COMPUTE_WH',
    database='TEST_DB',
    schema='PUBLIC',
    role='ACCOUNTADMIN'
)

cursor = snowflake_conn.cursor()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS SUPPLY_CHAIN_STAGE (
    PRODUCT_TYPE STRING,
    SKU STRING,
    AVAILABILITY NUMBER,
    NUMBER_OF_PRODUCTS_SOLD NUMBER,
    STOCK_LEVELS NUMBER,
    LEAD_TIME NUMBER,
    ORDER_QUANTITIES NUMBER,
    SHIPPING_CARRIERS STRING,
    SUPPLIER_NAME STRING,
    TRANSPORTATION_MODES STRING,
    ORDER_DATE DATE,
    SHIPPING_DATE DATE,
    PO_DATE DATE,
    PO_APPROVE_DATE DATE,
    INVOICE_DATE DATE,
    GAN_DATE DATE,
    ACTUAL_LEAD_TIME_DAYS NUMBER,
    PLANNED_LEAD_TIME_DAYS NUMBER,
    DELIVERY_STATUS STRING,
    RECEIVED_QUANTITY NUMBER,
    PENDING_DELIVERY_QTY NUMBER,
    EXPECTED_GAN_DATE DATE,
    DATA_QUALITY_FLAG STRING,
    ETA_HOURS NUMBER
)
""")

# Delete and Refresh
cursor.execute("DELETE FROM TEST_DB.PUBLIC.SUPPLY_CHAIN_STAGE")

insert_sql = """
INSERT INTO TEST_DB.PUBLIC.SUPPLY_CHAIN_STAGE (
    PRODUCT_TYPE, SKU, AVAILABILITY, NUMBER_OF_PRODUCTS_SOLD, STOCK_LEVELS,
    LEAD_TIME, ORDER_QUANTITIES, SHIPPING_CARRIERS, SUPPLIER_NAME,
    TRANSPORTATION_MODES, ORDER_DATE, SHIPPING_DATE, PO_DATE,
    PO_APPROVE_DATE, INVOICE_DATE, GAN_DATE, ACTUAL_LEAD_TIME_DAYS,
    PLANNED_LEAD_TIME_DAYS, DELIVERY_STATUS, RECEIVED_QUANTITY,
    PENDING_DELIVERY_QTY, EXPECTED_GAN_DATE, DATA_QUALITY_FLAG, ETA_HOURS
)
VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s
)
"""

data = [tuple(row) for row in df.itertuples(index=False, name=None)]
cursor.executemany(insert_sql, data)

snowflake_conn.commit()

cursor.close()
snowflake_conn.close()
oracle_conn.close()

print("Oracle data synced to Snowflake successfully.")