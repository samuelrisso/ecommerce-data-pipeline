-- Modelo staging: lee las órdenes crudas desde Parquet
-- Materializado como view para no duplicar datos

SELECT
    order_id,
    fecha,
    producto,
    categoria,
    cantidad,
    precio_unitario,
    estado,
    ROUND(cantidad * precio_unitario, 2) AS ingreso_total
FROM read_parquet('/app/data/output/ordenes_raw.parquet')
