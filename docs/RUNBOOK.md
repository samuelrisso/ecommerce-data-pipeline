# Runbook — Pipeline E-Commerce

## Ejecución normal

### Levantar infraestructura
```bash
cd infra
docker compose up -d
```

### Verificar servicios
```bash
docker compose ps
# Todos deben estar en estado "running"
```

### Ejecutar pipeline completo
```bash
docker exec pipeline python jobs/run_pipeline.py
```

### Ejecutar pasos individuales
```bash
# 1. Generar datos
docker exec pipeline python jobs/generar_datos.py

# 2. Producir a Kafka
docker exec pipeline python jobs/producer.py

# 3. Consumir y guardar en MinIO
docker exec pipeline python jobs/consumer.py

# 4. Transformar con dbt
docker exec pipeline dbt run --project-dir dbt --profiles-dir dbt

# 5. Testear calidad
docker exec pipeline dbt test --project-dir dbt --profiles-dir dbt
```

## Monitoreo

| Servicio | URL | Credenciales |
|----------|-----|-------------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Prometheus | http://localhost:9090 | - |
| Grafana | http://localhost:3000 | admin / admin |
| Métricas pipeline | http://localhost:8000/metrics | - |

## Troubleshooting

### Kafka no conecta
```bash
# Verificar que Kafka y Zookeeper están corriendo
docker compose logs kafka
# Reiniciar si es necesario
docker compose restart kafka
```

### dbt falla con "file not found"
```bash
# Verificar que existe el parquet de input
docker exec pipeline ls -la data/output/ordenes_raw.parquet
# Si no existe, ejecutar el paso de ingestión primero
docker exec pipeline python jobs/generar_datos.py
```

### MinIO: bucket no existe
```bash
# El consumidor lo crea automáticamente
# Si falla, crear manualmente desde la UI: http://localhost:9001
```

### Prometheus no scrappea métricas
```bash
# Verificar que el servidor de métricas está corriendo
curl http://localhost:8000/metrics
# Si no responde, levantar el servidor
docker exec -d pipeline python jobs/metrics_server.py
```

## Detener todo
```bash
cd infra
docker compose down        # Conserva datos
docker compose down -v     # Borra todo (incluyendo volúmenes)
```

## Contacto
- Autor: Samuel Risso
- Repo: https://github.com/samuelrisso/ecommerce-data-pipeline
