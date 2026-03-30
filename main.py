"""
main.py
    - Runs the PMPS analytics process based on messages received from Kafka.
    - Receives messages containing S3 file paths, parses them using parseS3object,
      then passes them to the appropriate processor (ACT or AIR) for analysis.
      Results are stored in the SQL database.

Processing steps:
    1. Connect to Kafka server and subscribe to the configured topic.
    2. Receive JSON messages containing S3 paths.
    3. Parse S3 paths and run data processing.
    4. Handle errors for invalid messages or unknown formats.
"""

import json
import os
import datetime
from kafka import KafkaConsumer
from S3Processor import S3Processor
from ACT_processor import ActProcessor
from AIR_processor import AirProcessor
from OracleProcessor import OracleProcessor
from parse_S3_object import parseS3object

print("PMPS analytics")

# Initialize S3 and SQL processors
s3 = S3Processor()
sql = OracleProcessor()

# Build ACT processor: S3 -> KMeans -> SQL
act_proc = ActProcessor(s3=s3, sql_processor=sql)

# Build AIR processor: S3 -> KMeans -> SQL
air_proc = AirProcessor(s3=s3, sql_processor=sql)

# Fetch configuration from environment variables
KAFKA_SERVER = os.getenv("KAFKA_SERVER").split(",")
KAFKA_ANALYTICS_TOPIC = os.getenv("KAFKA_ANALYTICS_TOPIC")
KAFKA_ANALYTICS_GROUP_ID = os.getenv("KAFKA_ANALYTICS_GROUP_ID")
KAFKA_NOTIFICATIONS_TOPIC = os.getenv("KAFKA_NOTIFICATIONS_TOPIC")

# Initialize Kafka consumer
consumer = KafkaConsumer(
    KAFKA_ANALYTICS_TOPIC,
    group_id=KAFKA_ANALYTICS_GROUP_ID,
    bootstrap_servers=KAFKA_SERVER
)

print(
    f"Listening on Kafka topic: {KAFKA_ANALYTICS_TOPIC}, "
    f"group_id={KAFKA_ANALYTICS_GROUP_ID}, bootstrap_servers={KAFKA_SERVER}"
)


# ---TEST---
"""
#s3_object = 'ACT/dn=KCSDVK113335V01MM11/date=2025-10-02/12.parquet'
s3_object = 'AIR/dn=KCSDVK11HP1VI1KKP01/date=2026-03-22/10.parquet'
if not s3_object:
    print(f"[ERROR] Missing 's3_object' field in message: {msg_json}")
else:
    try:
        # Parse S3 path to extract device and datetime info
        info = parseS3object(s3_object)

        # Run processing for the detected telegram type
        if info["telegram"] == "ACT":
            act_proc.process_s3_data(info["device"], info["datetime"])
        elif info["telegram"] == "AIR":
            air_proc.process_s3_data(info["device"], info["datetime"])
    except ValueError as ve:
        print(f"[ERROR] Failed to parse S3 path: {ve}")
"""
# ----------

# Main Kafka message loop
for message in consumer:
    try:
        # Decode and parse Kafka message as JSON
        msg_value = message.value.decode("utf-8")
        msg_json = json.loads(msg_value)
    except Exception as e:
        print(f"[ERROR] Failed to decode/parse message: {e}, raw={message.value!r}")
        continue

    # Extract S3 object path from Kafka message
    s3_object = msg_json.get("s3_object")  # Example: 'AIR/dn=KCSDVK11HP1VI1KKP01/date=2025-03-12/08.parquet'
    if not s3_object:
        print(f"[ERROR] Missing 's3_object' field in message: {msg_json}")
        continue

    try:
        # Parse S3 path to extract device and datetime info
        info = parseS3object(s3_object)

        # Route message to the appropriate processor based on telegram type
        if info["telegram"] == "ACT":
            act_proc.process_s3_data(info["device"], info["datetime"])
        elif info["telegram"] == "AIR":
            air_proc.process_s3_data(info["device"], info["datetime"])
        else:
            print(f"[WARNING] Unknown telegram type: {info['telegram']}")
    except ValueError as ve:
        print(f"[ERROR] Failed to parse S3 path: {ve}")
        continue

# ---------------------------
# Optional SQL debug section
# ---------------------------

# sql.print_all_tables()
