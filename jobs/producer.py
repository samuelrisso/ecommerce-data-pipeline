"""
Productor Kafka: lee órdenes del Parquet y las publica en el topic 'ordenes'.
Simula la ingestión de eventos en tiempo real.
"""
import json
import pandas as pd
from kafka import KafkaProducer


def create_producer(bootstrap_servers: str = "localhost:9092") -> KafkaProducer:
    """Crea un productor Kafka con serialización JSON."""
    return KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def publicar_ordenes(parquet_path: str = "data/input/ordenes.parquet",
                     bootstrap_servers: str = "localhost:9092",
                     topic: str = "ordenes"):
    """Lee el Parquet y publica cada orden como mensaje en Kafka."""
    df = pd.read_parquet(parquet_path)
    producer = create_producer(bootstrap_servers)

    count = 0
    for _, row in df.iterrows():
        event = row.to_dict()
        producer.send(topic, value=event)
        count += 1

    producer.flush()
    producer.close()
    print(f"[producer] {count} órdenes publicadas en topic '{topic}'")


if __name__ == "__main__":
    import os
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    publicar_ordenes(bootstrap_servers=bootstrap)
