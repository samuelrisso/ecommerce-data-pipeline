# E-Commerce Data Pipeline

Pipeline de datos end-to-end para análisis de órdenes de un e-commerce.

## Arquitectura

```
[Generador] → [Kafka] → [Consumidor] → [MinIO] → [dbt/DuckDB] → [Métricas]
  (Python)     (topic)     (Python)      (S3)       (SQL)        (Prometheus)
                                                       ↑
                                              [Airflow orquesta]
```

## Stack

| Componente | Herramienta | Propósito |
|------------|-------------|-----------|
| Ingestión | Apache Kafka | Cola de mensajes para eventos de órdenes |
| Storage | MinIO | Data lake local (S3-compatible) |
| Transformación | dbt + DuckDB | Modelos SQL: staging → limpieza → métricas |
| Orquestación | Apache Airflow | DAG diario automatizado |
| Observabilidad | Prometheus + Grafana | Métricas y dashboards |
| Testing | pytest + dbt test | Calidad de datos y lógica |
| CI/CD | GitHub Actions | Tests automáticos en cada push |

## Quick Start

```bash
# 1. Clonar
git clone https://github.com/samuelrisso/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline

# 2. Levantar servicios
cd infra && docker compose up -d

# 3. Ejecutar pipeline
docker exec pipeline python jobs/run_pipeline.py

# 4. Ver métricas
# Grafana: http://localhost:3000 (admin/admin)
# Prometheus: http://localhost:9090
# MinIO: http://localhost:9001 (minioadmin/minioadmin)
```

## Estructura del proyecto

```
.
├── infra/                  Docker Compose + Dockerfile
├── jobs/                   Scripts Python del pipeline
│   ├── generar_datos.py    Generador de órdenes sintéticas
│   ├── producer.py         Productor Kafka
│   ├── consumer.py         Consumidor Kafka → MinIO
│   ├── metrics_server.py   Servidor Prometheus
│   └── run_pipeline.py     Orquestador local
├── dbt/                    Proyecto dbt
│   ├── models/             Modelos SQL
│   ├── seeds/              Datos de referencia
│   └── profiles.yml        Conexión DuckDB
├── airflow/dags/           DAG de Airflow
├── observability/          Config Prometheus/Grafana
├── tests/                  Tests automatizados (pytest)
├── docs/                   Runbook y documentación
└── .github/workflows/      CI/CD pipeline
```

## Justificación técnica

Se evaluó incluir Apache Spark para procesamiento distribuido, pero dado que el volumen de datos del caso de uso (miles de órdenes diarias) no justifica el overhead de un cluster, se optó por **DuckDB** como motor analítico. DuckDB resuelve el caso con mejor performance en datasets de este tamaño y sin complejidad operativa adicional.

## Autor

Samuel Risso — [GitHub](https://github.com/samuelrisso)

Proyecto final para Data Engineering — CoderHouse (101990)
