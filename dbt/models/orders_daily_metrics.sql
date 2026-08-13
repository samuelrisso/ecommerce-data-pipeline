-- Modelo de métricas diarias: ventas agregadas por fecha, producto y categoría
{{ config(materialized='table') }}

SELECT
    fecha,
    producto,
    categoria,
    departamento,
    COUNT(*)                        AS total_ordenes,
    SUM(cantidad)                   AS unidades_vendidas,
    ROUND(SUM(ingreso_total), 2)    AS ingreso_total,
    ROUND(AVG(precio_unitario), 2)  AS precio_promedio
FROM {{ ref('orders_clean') }}
GROUP BY
    fecha,
    producto,
    categoria,
    departamento
