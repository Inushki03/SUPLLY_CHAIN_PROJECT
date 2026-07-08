import os
import requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")

supplier_name = input("Enter supplier name: ")

# -------------------------
# NEWS API SEARCH
# -------------------------

url = "https://newsapi.org/v2/everything"

params = {
   "q": f'"{supplier_name}" AND (ESG OR sustainability OR compliance OR environmental OR governance OR emissions OR violation OR penalty OR lawsuit)',
    "language": "en",
    "sortBy": "publishedAt",
    "pageSize": 5,
    "apiKey": NEWS_API_KEY
}

response = requests.get(url, params=params)

print("News API Status:", response.status_code)

data = response.json()

articles = data.get("articles", [])

print("Articles found:", len(articles))

# -------------------------
# SNOWFLAKE CONNECTION
# -------------------------

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cursor = conn.cursor()

# -------------------------
# INSERT INTO SNOWFLAKE
# -------------------------

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
            1,
            %s,
            %s,
            'News',
            %s,
            %s,
            TO_DATE(%s)
        )
    """,
    (
        supplier_name,
        source_url,
        title,
        description,
        published_date
    ))

conn.commit()

print("Data inserted into Snowflake successfully.")

cursor.close()
conn.close()