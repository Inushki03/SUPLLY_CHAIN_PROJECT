import asyncio
import json
import os
from datetime import datetime
import ssl
import certifi
import re

import websockets
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY")
if not AISSTREAM_API_KEY:
    raise ValueError("AISSTREAM_API_KEY is missing. Check your .env file.")

conn = snowflake.connector.connect(
    user=os.getenv("snowflake_user"),
    password=os.getenv("snowflake_password"),
    account=os.getenv("snowflake_account"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cursor=conn.cursor()

def insert_into_snowflake(data):
    sql="""
        Insert into LIVE_SHIPPING_DATA(MMSI, VESSELNAME, LATITUDE, LONGATIDUE, SPEED,COURSE,HEADING,NAV_STATUS,MESSAGE_TIME)
        values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    value=(
        data["mmsi"],
        data["vesselname"],
        data["latitude"],
        data["longitude"],
        data["speed"],  
        data["course"],
        data["heading"],
        data["nav_status"],
        data["message_time"]
    )

    cursor.execute(sql, value)
    conn.commit()


def insert_static_to_snowflake(data):
    sql="""
        Insert into VESSEL_STATIC_VOYAGE_DATA(
            MMSI,
            IMO_NUMBER,
            VESSEL_NAME,
            CALL_SIGN,
            SHIP_TYPE,
            DESTINATION,
            ETA_MONTH,
            ETA_DAY,
            ETA_HOUR,
            ETA_MIN,
            MAX_DRAUGHT,
            DIMENSION_A,
            DIMENSION_B,
            DIMENSION_C,
            DIMENION_D,
            MESSAGE_TIME
        ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
    values=(
        data["mmsi"],
        data["imo_number"],
        data["vesselname"],
        data["call_sign"],
        data["ship_type"],
        data["destination"],
        data["eta_month"],
        data["eta_day"],
        data["eta_hour"],
        data["eta_min"],
        data["max_draught"],
        data["dimension_a"],
        data["dimension_b"],
        data["dimension_c"],
        data["dimension_d"],
        data["message_time"]
       
    )

    cursor.execute(sql, values)
    conn.commit()
    
    

def clean_timestamp(ts):
    if ts is None:
        return datetime.utcnow()

    ts = str(ts)

    # Example:
    # 2026-05-20 12:19:16.496810734 +0000 UTC

    # Remove UTC text
    ts = ts.replace(" UTC", "")

    # Remove timezone like +0000
    ts = re.sub(r"\s[+-]\d{4}", "", ts)

    # Get date, time, and decimal seconds safely
    match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?:\.(\d+))?", ts)

    if not match:
        return datetime.utcnow()

    base_time = match.group(1)
    fraction = match.group(2) or "0"

    # Keep only first 6 digits
    fraction = fraction[:6].ljust(6, "0")

    clean_ts = f"{base_time}.{fraction}"

    return datetime.strptime(clean_ts, "%Y-%m-%d %H:%M:%S.%f")



async def connect_aisstream():
    ssl_context = ssl._create_unverified_context()
    async with websockets.connect("wss://stream.aisstream.io/v0/stream",ssl=ssl_context,ping_interval=60,ping_timeout=60) as websocket:

        subscribe_message = {
            "APIKey": AISSTREAM_API_KEY,

            # Area around Sri Lanka
            "BoundingBoxes": [
                [[1.0, 103.0], [2.0, 104.5]]
            ],

            "FilterMessageTypes": ["PositionReport","ShipStaticData","StaticDataReport"]
        }

        await websocket.send(json.dumps(subscribe_message))

        print("Connected to AISStream...")
        print("Waiting for vessel data...")

        while True:
            message_json = await websocket.recv()
            message = json.loads(message_json)

            message_type = message.get("MessageType")

            if message_type == "PositionReport":
                position=message.get("Message",{}).get("PositionReport",{})
                metadata = message.get("MetaData", {})
            
                data = {
                    "mmsi": str(metadata.get("MMSI")),
                    "vesselname": metadata.get("ShipName"),
                    "latitude": position.get("Latitude"),
                    "longitude": position.get("Longitude"),
                    "speed": position.get("Sog"),
                    "course": position.get("Cog"),
                    "heading": position.get("TrueHeading"),
                    "nav_status": str(position.get("NavigationalStatus")),
                    "message_time": clean_timestamp(metadata.get("time_utc"))
                }

                print("Successfully received position report for MMSI:", data["mmsi"])
                insert_into_snowflake(data)
            
            elif message_type =="ShipStaticData":
                static=message.get("Message",{}).get("ShipStaticData",{})
                metadata = message.get("MetaData", {})

                eta=static.get("Eta",{})
                dimensions=static.get("Dimensions",{})

                data={
                    "mmsi":str(metadata.get("MMSI") or static.get("UserID")),
                    "imo_number":str(static.get("ImoNumber")),
                    "vesselname":static.get("ShipName"),
                    "call_sign":static.get("CallSign"),
                    "ship_type":str(static.get("ShipType")),
                    "destination":static.get("Destination"),
                    "eta_month":eta.get("Month"),
                    "eta_day":eta.get("Day"),
                    "eta_hour":eta.get("Hour"),
                    "eta_min":eta.get("Min"),
                    "max_draught":static.get("MaxDraught"),
                    "dimension_a":dimensions.get("A"),
                    "dimension_b":dimensions.get("B"),
                    "dimension_c":dimensions.get("C"),
                    "dimension_d":dimensions.get("D"),
                    "message_time":clean_timestamp(metadata.get("time_utc"))
                }
    
                print("Successfully received static data for MMSI:", data["mmsi"])
                insert_static_to_snowflake(data)

    
asyncio.run(connect_aisstream())