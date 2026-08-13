-- Modelo de limpieza: filtra órdenes inválidas y enriquece con categoría seed
{{ config(materialized='table') }}

SELECT
    o.order_id,
    o.fecha,
    o.producto,
    o.categoria,
    c.departamento,
    o.cantidad,
    o.precio_unitario,
    o.ingreso_total,
    o.estado
FROM {{ ref('orders_raw') }} AS o
INNER JOIN {{ ref('categorias') }} AS c
    ON o.categoria = c.nombre
WHERE
    o.order_id IS NOT NULL
    AND o.cantidad > 0
    AND o.precio_unitario > 0
    AND o.estado != 'cancelada'
