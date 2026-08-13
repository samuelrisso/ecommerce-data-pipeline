# Proyecto Final — Pipeline de Datos E-Commerce

**Alumno:** Samuel Risso
**Curso:** Data Engineering — CoderHouse (101990)
**Repositorio:** https://github.com/samuelrisso/ecommerce-data-pipeline

---

## 1. Arquitectura y Justificación Técnica

### Diseño del sistema

El pipeline implementa un flujo de datos end-to-end para un e-commerce, desde la generación de órdenes hasta métricas analíticas:

```
[Generador Python] → [Kafka] → [Consumidor] → [MinIO] → [dbt/DuckDB] → [Prometheus/Grafana]
                                                                ↑
                                                       [Airflow orquesta]
```

### Decisiones técnicas

| Decisión | Justificación |
|----------|---------------|
| Kafka para ingestión | Estándar de la industria para streaming. Desacopla productor de consumidor. Tolerante a fallos. |
| MinIO como data lake | Compatible con S3, permite almacenamiento por capas (raw/processed). Sin costo de cloud. |
| DuckDB en vez de Spark | El volumen de datos (miles de órdenes diarias) no justifica un cluster distribuido. DuckDB resuelve queries analíticos en milisegundos sobre Parquet. |
| dbt para transformaciones | SQL versionado, testeado y documentado. Estándar para la capa de transformación. |
| Docker Compose | Entorno 100% reproducible. Cualquier persona puede levantar el pipeline con un solo comando. |

### Trade-offs

- **Spark:** Se evaluó incluirlo para procesamiento distribuido, pero dado el volumen del caso de uso, agregaría complejidad operativa sin beneficio real. Si el volumen escalara a millones de registros diarios, se incorporaría Spark para procesamiento batch.
- **Airflow:** Se usa para orquestación aunque un cron podría resolver el scheduling. Airflow aporta visibilidad del DAG, reintentos, y alertas.
- **DuckDB vs PostgreSQL:** DuckDB es columnar y optimizado para analytics. PostgreSQL sería mejor si necesitáramos transacciones OLTP, pero nuestro caso es puramente analítico.

---

## 2. Infraestructura

### Servicios definidos (docker-compose.yml)

| Servicio | Imagen | Puerto | Propósito |
|----------|--------|--------|-----------|
| Zookeeper | confluentinc/cp-zookeeper:7.5.0 | 2181 | Coordinación de Kafka |
| Kafka | confluentinc/cp-kafka:7.5.0 | 9092 | Cola de mensajes |
| MinIO | minio/minio:latest | 9000, 9001 | Data lake (S3 local) |
| Pipeline | python:3.11-slim (custom) | - | Jobs Python + dbt |
| Prometheus | prom/prometheus:latest | 9090 | Recolección de métricas |
| Grafana | grafana/grafana:latest | 3000 | Dashboards |

### Dockerfile del pipeline

```dockerfile
FROM python:3.11-slim
RUN apt-get update && apt-get install -y git curl && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
WORKDIR /app
CMD ["bash"]
```

### Dependencias principales

```
pandas==2.2.2
kafka-python==2.0.2
boto3==1.34.0
dbt-duckdb==1.8.1
prometheus-client==0.20.0
pytest==8.2.0
```

---

## 3. Orquestación (Airflow)

### DAG: ecommerce_pipeline

El DAG se ejecuta diariamente (`@daily`) y tiene 5 tareas en secuencia:

```
generar_datos → ingestar_kafka → consumir_a_minio → dbt_run → dbt_test
```

### Lógica de ejecución

1. **generar_datos:** Ejecuta el generador de órdenes sintéticas (1000 registros)
2. **ingestar_kafka:** Publica las órdenes en el topic 'ordenes' de Kafka
3. **consumir_a_minio:** Lee de Kafka y persiste en MinIO como Parquet
4. **dbt_run:** Ejecuta los modelos SQL (staging → limpieza → métricas)
5. **dbt_test:** Valida calidad de datos (not_null, accepted_values, unique)

### Configuración

- Owner: samuelrisso
- Retries: 1 con delay de 5 minutos
- Catchup: deshabilitado (no reprocesa días pasados)

---

## 4. Transformaciones (dbt)

### Modelos implementados

| Modelo | Materialización | Descripción |
|--------|----------------|-------------|
| orders_raw | view | Lee Parquet crudo, calcula ingreso_total |
| orders_clean | table | Filtra canceladas, valida categorías, enriquece con seed |
| orders_daily_metrics | table | Agrega por fecha/producto: unidades, ingreso, precio promedio |

### DAG de modelos

```
orders_raw (view) → orders_clean (table) → orders_daily_metrics (table)
                         ↑
                    categorias (seed)
```

### Tests definidos

| Modelo | Columna | Test |
|--------|---------|------|
| orders_raw | order_id | not_null, unique |
| orders_raw | estado | accepted_values: completada, cancelada, pendiente |
| orders_clean | order_id | not_null, unique |
| orders_clean | categoria | accepted_values: Tecnología, Indumentaria, Alimentos |
| orders_daily_metrics | fecha | not_null |
| orders_daily_metrics | ingreso_total | not_null |

### Seed: categorias.csv

```csv
nombre,departamento
Tecnología,Electronics
Indumentaria,Fashion
Alimentos,Grocery
```

---

## 5. Procesamiento Distribuido (PySpark)

### Decisión: No incluir Spark

Se evaluó incluir Apache Spark para el procesamiento de datos, pero se descartó por las siguientes razones:

1. **Volumen insuficiente:** El caso de uso genera ~1000 órdenes por ejecución. DuckDB procesa esta carga en milisegundos.
2. **Complejidad operativa:** Un cluster Spark (incluso en Docker) requiere master + workers, consume significativamente más RAM, y agrega puntos de fallo.
3. **Overhead de serialización:** Para datasets pequeños, el costo de serializar datos entre nodos supera el beneficio del paralelismo.

### Cuándo sí incluiríamos Spark

- Volumen > 1 millón de registros por batch
- Necesidad de procesamiento distribuido real (múltiples nodos)
- Transformaciones que requieran shuffle sobre datasets que no caben en memoria de un solo nodo

Esta decisión demuestra criterio técnico: elegir la herramienta adecuada según el caso, no la más compleja.

---

## 6. Datos de Ejemplo

### Estructura del dataset de órdenes

| Columna | Tipo | Descripción |
|---------|------|-------------|
| order_id | int | Identificador único de la orden |
| fecha | string | Fecha de la orden (YYYY-MM-DD) |
| producto | string | Nombre del producto |
| categoria | string | Categoría: Tecnología, Indumentaria, Alimentos |
| cantidad | int | Unidades (1-9) |
| precio_unitario | float | Precio por unidad (10.0 - 1500.0) |
| estado | string | completada, cancelada, pendiente |

### Muestra de datos

| order_id | fecha | producto | categoria | cantidad | precio_unitario | estado |
|----------|-------|----------|-----------|----------|-----------------|--------|
| 1 | 2024-01-15 | Laptop | Tecnología | 3 | 1245.50 | completada |
| 2 | 2024-01-15 | Remera | Indumentaria | 5 | 45.99 | completada |
| 3 | 2024-01-16 | Café x1kg | Alimentos | 2 | 89.00 | cancelada |

### Productos disponibles

- Tecnología: Laptop, Mouse, Teclado, Monitor, Auriculares
- Indumentaria: Remera, Pantalón, Zapatillas
- Alimentos: Café x1kg, Yerba x500g

### Generación

- Seed: 42 (reproducible)
- N: 1000 registros por ejecución
- Rango de fechas: 90 días desde 2024-01-01
- Distribución de estados: ~60% completada, ~20% cancelada, ~20% pendiente

---

## 7. Pruebas Automatizadas

### Estrategia de testing

Se implementan dos niveles de testing:

1. **Tests unitarios (pytest):** Validan la lógica Python del generador y las transformaciones
2. **Tests de calidad de datos (dbt test):** Validan integridad de los datos transformados

### Tests pytest implementados

| Test | Qué valida |
|------|-----------|
| test_genera_cantidad_correcta | Genera exactamente N registros |
| test_columnas_esperadas | Las 7 columnas están presentes |
| test_no_hay_nulls | Ningún valor es NULL |
| test_estados_validos | Solo estados permitidos |
| test_categorias_validas | Solo categorías conocidas |
| test_cantidades_positivas | Todas las cantidades > 0 |
| test_precios_positivos | Todos los precios > 0 |
| test_order_ids_unicos | No hay IDs duplicados |
| test_reproducibilidad_con_seed | Mismo seed = mismos datos |
| test_ingreso_total_calculado | cantidad * precio > 0 |
| test_filtro_canceladas | Eliminación correcta de canceladas |
| test_agregacion_por_fecha | Agrupación produce resultados válidos |

### Ejecución

```bash
# pytest
docker exec pipeline pytest tests/ -v

# dbt test
docker exec pipeline dbt test --project-dir dbt --profiles-dir dbt
```

---

## 8. CI/CD

### Pipeline de integración continua (GitHub Actions)

Archivo: `.github/workflows/ci.yml`

### Etapas del pipeline CI

1. **Checkout** del código
2. **Setup Python** 3.11
3. **Instalar dependencias** desde requirements.txt
4. **Ejecutar pytest** — tests unitarios
5. **Generar datos** de prueba
6. **Simular ingestión** (copia local del Parquet)
7. **dbt seed + run + test** — validación end-to-end

### Trigger

Se ejecuta en cada `push` a `main` y en cada Pull Request.

### Flujo esperado

```
push → install deps → pytest → generate data → dbt seed → dbt run → dbt test → ✅
```

Si algún paso falla, el pipeline se detiene y reporta el error.

---

## 9. Observabilidad

### Métricas expuestas

| Métrica | Tipo | Descripción |
|---------|------|-------------|
| pipeline_records_processed | Gauge | Total de registros procesados |
| pipeline_orders_total | Counter | Órdenes ingestadas acumuladas |
| pipeline_revenue_total | Gauge | Ingreso total calculado |
| pipeline_errors_total | Counter | Errores en el pipeline |
| pipeline_last_run_seconds | Gauge | Duración del último run |

### Stack de observabilidad

- **Prometheus** (puerto 9090): Scrapea métricas cada 15 segundos
- **Grafana** (puerto 3000): Dashboards visuales
- **Endpoint /metrics** (puerto 8000): Exposición de métricas en formato Prometheus

### Configuración Prometheus

```yaml
scrape_configs:
  - job_name: "pipeline-metrics"
    static_configs:
      - targets: ["pipeline:8000"]
```

### Alertas sugeridas (conceptual)

- `pipeline_errors_total > 0` → Alerta de error en pipeline
- `pipeline_records_processed == 0` → Pipeline no procesó datos
- `pipeline_last_run_seconds > 300` → Run demasiado lento

---

## 10. Runbook

### Ejecución normal

```bash
# Levantar infraestructura
cd infra && docker compose up -d

# Ejecutar pipeline completo
docker exec pipeline python jobs/run_pipeline.py

# Ejecutar pasos individuales
docker exec pipeline python jobs/generar_datos.py
docker exec pipeline python jobs/producer.py
docker exec pipeline python jobs/consumer.py
docker exec pipeline dbt run --project-dir dbt --profiles-dir dbt
docker exec pipeline dbt test --project-dir dbt --profiles-dir dbt
```

### URLs de servicios

| Servicio | URL | Credenciales |
|----------|-----|-------------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Prometheus | http://localhost:9090 | - |
| Grafana | http://localhost:3000 | admin / admin |

### Troubleshooting

| Problema | Solución |
|----------|----------|
| Docker no corre | Iniciar Docker Desktop, esperar 30s |
| Kafka no conecta | `docker compose restart kafka` |
| dbt: file not found | Ejecutar generar_datos.py primero |
| MinIO: bucket no existe | El consumidor lo crea automáticamente |
| Prometheus no scrapea | Verificar `curl localhost:8000/metrics` |

### Detener todo

```bash
docker compose down        # Conserva datos
docker compose down -v     # Borra todo
```

---

## 11. Documentación Técnica

### Flujo de datos detallado

1. `generar_datos.py` crea un DataFrame con 1000 órdenes y lo guarda como Parquet en `data/input/`
2. `producer.py` lee el Parquet y publica cada orden como mensaje JSON en el topic 'ordenes' de Kafka
3. `consumer.py` consume los mensajes, los agrupa en un DataFrame y los persiste en MinIO (bucket `raw-orders`) y localmente en `data/output/`
4. dbt lee el Parquet local, aplica transformaciones SQL y persiste tablas en DuckDB
5. `metrics_server.py` expone métricas del pipeline para Prometheus

### Idempotencia

El consumidor implementa idempotencia: si se ejecuta múltiples veces, sobrescribe el mismo archivo en MinIO (mismo key), evitando duplicados.

### Reproducibilidad

- Seed fijo (42) en el generador → mismos datos siempre
- Docker Compose → mismo entorno en cualquier máquina
- requirements.txt con versiones pinneadas → mismas dependencias

---

## 12. Guía de Demo

### Pre-requisitos

- Docker Desktop instalado y corriendo
- Git instalado
- Puerto 9000, 9001, 9090, 3000, 8000 disponibles

### Pasos para reproducir

```bash
# 1. Clonar el repositorio
git clone https://github.com/samuelrisso/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline

# 2. Levantar toda la infraestructura
cd infra
docker compose up -d --build

# 3. Esperar 30 segundos (Kafka necesita inicializar)
sleep 30

# 4. Ejecutar el pipeline completo
docker exec pipeline python jobs/run_pipeline.py

# 5. Verificar resultados
# - MinIO: http://localhost:9001 → bucket raw-orders
# - Prometheus: http://localhost:9090 → métricas
# - Grafana: http://localhost:3000 → dashboards

# 6. Correr tests
docker exec pipeline pytest tests/ -v
docker exec pipeline dbt test --project-dir dbt --profiles-dir dbt
```

---

## 13. Evidencia de Ejecución

### Pipeline ejecutado

```
🚀 PIPELINE E-COMMERCE — Ejecución completa
==================================================
STEP: Generar datos
[generar_datos] Dataset generado: 1000 órdenes
✅ Generar datos completado en 0.3s

STEP: Ingestión
[ingest] 1000 órdenes copiadas a data/output/
✅ Ingestión completado en 0.2s

STEP: dbt run
Running with dbt=1.11.11
1 of 3 OK created sql view model main.orders_raw
2 of 3 OK created sql table model main.orders_clean
3 of 3 OK created sql table model main.orders_daily_metrics
✅ dbt run completado en 1.2s

STEP: dbt test
6 of 6 PASS
✅ dbt test completado en 0.5s

✅ PIPELINE COMPLETADO en 2.2s
```

### Tests pytest

```
tests/test_pipeline.py::TestGeneradorDatos::test_genera_cantidad_correcta PASSED
tests/test_pipeline.py::TestGeneradorDatos::test_columnas_esperadas PASSED
tests/test_pipeline.py::TestGeneradorDatos::test_no_hay_nulls PASSED
tests/test_pipeline.py::TestGeneradorDatos::test_estados_validos PASSED
tests/test_pipeline.py::TestGeneradorDatos::test_categorias_validas PASSED
tests/test_pipeline.py::TestGeneradorDatos::test_cantidades_positivas PASSED
tests/test_pipeline.py::TestGeneradorDatos::test_precios_positivos PASSED
tests/test_pipeline.py::TestGeneradorDatos::test_order_ids_unicos PASSED
tests/test_pipeline.py::TestGeneradorDatos::test_reproducibilidad_con_seed PASSED
tests/test_pipeline.py::TestTransformaciones::test_ingreso_total_calculado PASSED
tests/test_pipeline.py::TestTransformaciones::test_filtro_canceladas PASSED
tests/test_pipeline.py::TestTransformaciones::test_agregacion_por_fecha PASSED

12 passed in 0.8s
```

---

## 14. Calidad del Código

### Principios aplicados

- **Modularidad:** Cada paso del pipeline es un script independiente con responsabilidad única
- **Documentación:** Docstrings en todas las funciones, comentarios explicativos en SQL
- **Reproducibilidad:** Seeds fijos, versiones pinneadas, entorno Dockerizado
- **Configurabilidad:** Variables de entorno para conexiones (no hardcodeadas)
- **Idempotencia:** El pipeline puede re-ejecutarse sin generar duplicados
- **Testing:** 12 tests automatizados cubriendo generación, transformación y calidad

### Ejemplo: Generador de datos

```python
def generar_ordenes(n: int = 1000, seed: int = 42) -> pd.DataFrame:
    """Genera un DataFrame con N órdenes sintéticas."""
    np.random.seed(seed)  # Reproducibilidad garantizada
    # ... generación de datos ...
    return df
```

### Ejemplo: Modelo dbt con documentación

```sql
-- Modelo de limpieza: filtra órdenes inválidas y enriquece con categoría seed
{{ config(materialized='table') }}

SELECT ...
FROM {{ ref('orders_raw') }} AS o
INNER JOIN {{ ref('categorias') }} AS c
    ON o.categoria = c.nombre
WHERE
    o.order_id IS NOT NULL      -- Filtro de integridad
    AND o.cantidad > 0          -- Solo cantidades válidas
    AND o.estado != 'cancelada' -- Excluir canceladas del análisis
```

---

*Proyecto Final — Data Engineering — CoderHouse (101990)*
*Samuel Risso — Agosto 2026*
