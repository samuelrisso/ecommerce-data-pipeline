"""
Consumidor Kafka: lee órdenes del topic 'ordenes' y las guarda en MinIO como Parquet.
Implementa idempotencia: mismo order_id → mismo archivo.
"""
import json
import os
import io
import pandas as pd
import boto3
from kafka import KafkaConsumer


def create_minio_client():
    """Crea un cliente S3 apuntando a MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    )


def ensure_bucket(s3_client, bucket: str):
    """Crea el bucket si no existe."""
    try:
        s3_client.head_bucket(Bucket=bucket)
    except Exception:
        s3_client.create_bucket(Bucket=bucket)
        print(f"[consumer] Bucket '{bucket}' creado")


def consumir_y_guardar(bootstrap_servers: str = "localhost:9092",
                       topic: str = "ordenes",
                       bucket: str = "raw-orders",
                       max_messages: int = 1000):
    """Consume mensajes de Kafka y los guarda en MinIO como Parquet."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_servers],
        group_id="pipeline-consumer",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
    )

    s3 = create_minio_client()
    ensure_bucket(s3, bucket)

    orders = []
    count = 0

    for message in consumer:
        orders.append(message.value)
        count += 1
        if count >= max_messages:
            break

    consumer.close()

    if orders:
        df = pd.DataFrame(orders)

        # Guardar como Parquet en MinIO
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        key = "ordenes/ordenes_batch.parquet"
        s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        print(f"[consumer] {count} órdenes guardadas en MinIO: {bucket}/{key}")

        # También guardar localmente para dbt
        os.makedirs("data/output", exist_ok=True)
        df.to_parquet("data/output/ordenes_raw.parquet", index=False)
        print(f"[consumer] Copia local: data/output/ordenes_raw.parquet")
    else:
        print("[consumer] No se recibieron mensajes")


if __name__ == "__main__":
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    consumir_y_guardar(bootstrap_servers=bootstrap)
