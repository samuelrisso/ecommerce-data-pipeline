"""
Servidor de métricas para Prometheus.
Expone métricas del pipeline en el puerto 8000 vía /metrics.
"""
import time
import os
import pandas as pd
from prometheus_client import start_http_server, Gauge, Counter

# Métricas
RECORDS_PROCESSED = Gauge("pipeline_records_processed", "Total de registros procesados")
ORDERS_TOTAL = Counter("pipeline_orders_total", "Órdenes totales ingestadas")
REVENUE_TOTAL = Gauge("pipeline_revenue_total", "Ingreso total calculado")
ERRORS_TOTAL = Counter("pipeline_errors_total", "Errores en el pipeline")
LAST_RUN_DURATION = Gauge("pipeline_last_run_seconds", "Duración del último run en segundos")


def update_metrics(parquet_path: str = "data/output/ordenes_raw.parquet"):
    """Lee el Parquet de output y actualiza las métricas."""
    try:
        if not os.path.exists(parquet_path):
            return

        df = pd.read_parquet(parquet_path)
        RECORDS_PROCESSED.set(len(df))

        if "cantidad" in df.columns and "precio_unitario" in df.columns:
            revenue = (df["cantidad"] * df["precio_unitario"]).sum()
            REVENUE_TOTAL.set(round(revenue, 2))

    except Exception as e:
        ERRORS_TOTAL.inc()
        print(f"[metrics] Error actualizando métricas: {e}")


def main():
    interval = int(os.getenv("METRICS_INTERVAL", "30"))
    port = int(os.getenv("METRICS_PORT", "8000"))

    start_http_server(port)
    print(f"[metrics] Servidor de métricas en puerto {port}")

    while True:
        update_metrics()
        time.sleep(interval)


if __name__ == "__main__":
    main()
