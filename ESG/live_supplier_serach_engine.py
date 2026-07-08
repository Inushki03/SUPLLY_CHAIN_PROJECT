import os
import time
import requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cursor = conn.cursor()

print("Live Supplier Search Engine Started...")

while True:

    cursor.execute("""
        SELECT SEARCH_ID, SUPPLIER_NAME
        FROM LIVE_SUPPLIER_SEARCH_QUEUE
        WHERE SEARCH_STATUS = 'PENDING'
        ORDER BY CREATED_AT
        LIMIT 1
    """)

    row = cursor.fetchone()

    if row:

        search_id = row[0]
        supplier_name = row[1]

        print(f"Processing supplier: {supplier_name}")

        url = "https://newsapi.org/v2/everything"

        params = {
            "q": f'"{supplier_name}" AND (ESG OR sustainability OR compliance OR environmental OR governance OR emissions OR violation OR penalty OR lawsuit)',
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 5,
            "apiKey": NEWS_API_KEY
        }

        response = requests.get(url, params=params)

        data = response.json()

        articles = data.get("articles", [])

        for article in articles:

            title = article.get("title", "")
            description = article.get("description", "")
            source_url = article.get("url", "")
            published_date = article.get("publishedAt", "")[:10]

            cursor.execute("""
                INSERT INTO SUPPLIER_OSINT_RESULTS
                (
                    SEARCH_ID,
                    SUPPLIER_NAME,
                    SOURCE_URL,
                    SOURCE_TYPE,
                    TITLE,
                    CONTENT,
                    PUBLISHED_DATE
                )
                VALUES
                (
                    %s,
                    %s,
                    %s,
                    'News',
                    %s,
                    %s,
                    TO_DATE(%s)
                )
            """,
            (
                search_id,
                supplier_name,
                source_url,
                title,
                description,
                published_date
            ))

        cursor.execute(f"""
            UPDATE LIVE_SUPPLIER_SEARCH_QUEUE
            SET SEARCH_STATUS = 'COMPLETED'
            WHERE SEARCH_ID = {search_id}
        """)

        cursor.execute(f"""
    INSERT INTO SUPPLIER_OSINT_ANALYSIS
    SELECT
        SEARCH_ID,
        SUPPLIER_NAME,
        SOURCE_TYPE,
        TITLE,
        PUBLISHED_DATE,

        CASE
            WHEN LOWER(CONTENT) LIKE '%violation%'
              OR LOWER(CONTENT) LIKE '%penalty%'
              OR LOWER(CONTENT) LIKE '%lawsuit%'
              OR LOWER(CONTENT) LIKE '%sanction%'
              OR LOWER(CONTENT) LIKE '%concern%'
            THEN 'Negative'
            ELSE 'Positive'
        END,

        CASE
            WHEN LOWER(CONTENT) LIKE '%violation%'
              OR LOWER(CONTENT) LIKE '%penalty%'
              OR LOWER(CONTENT) LIKE '%sanction%'
            THEN 'High'

            WHEN LOWER(CONTENT) LIKE '%concern%'
              OR LOWER(CONTENT) LIKE '%risk%'
              OR LOWER(CONTENT) LIKE '%waste%'
              OR LOWER(CONTENT) LIKE '%lawsuit%'
            THEN 'Medium'

            ELSE 'Low'
        END,

        CASE
            WHEN LOWER(CONTENT) LIKE '%violation%'
              OR LOWER(CONTENT) LIKE '%penalty%'
              OR LOWER(CONTENT) LIKE '%sanction%'
            THEN 85

            WHEN LOWER(CONTENT) LIKE '%concern%'
              OR LOWER(CONTENT) LIKE '%risk%'
              OR LOWER(CONTENT) LIKE '%waste%'
              OR LOWER(CONTENT) LIKE '%lawsuit%'
            THEN 55

            ELSE 20
        END,

        CURRENT_TIMESTAMP()

    FROM SUPPLIER_OSINT_RESULTS
    WHERE SEARCH_ID = {search_id}
""")

        conn.commit()

        print(f"Completed supplier: {supplier_name}")

    else:
        print("No pending suppliers...")

    time.sleep(1)