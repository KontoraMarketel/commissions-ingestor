import os
import json
import asyncio
import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from fetch_commissions import fetch_commissions
from storage import upload_to_minio

logging.basicConfig(level=logging.INFO)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CONSUMER_TOPIC = os.getenv("KAFKA_INGEST_COMMISSIONS_TASKS_TOPIC")
PRODUCER_TOPIC = os.getenv("KAFKA_STG_COMMISSIONS_TOPIC")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")


async def handle_message(msg: bytes):
    try:
        task_id = msg["task_id"]
        api_token = msg["wb_token"]
        init_date = msg["init_date"]

        logging.info(f"Start processing task {task_id}")

        data = await fetch_commissions(api_token)
        filename = "commissions.json"
        prefix = f"{init_date}/{task_id}/"
        minio_key = prefix + filename

        await upload_to_minio(
            endpoint_url=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            bucket=MINIO_BUCKET,
            data=data,
            key=minio_key,
        )

        logging.info(f"Task {task_id} completed successfully.")

        return {
            "task_id": task_id,
            "load_date": init_date,
            "minio_key": minio_key
        }

    except Exception as e:
        logging.error(f"Error processing message: {e}")


async def main():
    consumer = AIOKafkaConsumer(
        CONSUMER_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="commission-ingestors",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    await consumer.start()
    try:
        async for msg in consumer:
            await handle_message(msg.value)
            await producer.send(PRODUCER_TOPIC, value=msg.value)
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
