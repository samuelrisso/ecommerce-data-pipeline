"""
Generador de datos sintéticos de órdenes de e-commerce.
Simula órdenes con: order_id, producto, categoria, cantidad, precio, fecha, estado.
"""
import pandas as pd
import numpy as np
import os
from datetime import date, timedelta


def generar_ordenes(n: int = 1000, seed: int = 42) -> pd.DataFrame:
    """Genera un DataFrame con N órdenes sintéticas."""
    np.random.seed(seed)

    productos = [
        ("Laptop", "Tecnología"),
        ("Mouse", "Tecnología"),
        ("Teclado", "Tecnología"),
        ("Monitor", "Tecnología"),
        ("Auriculares", "Tecnología"),
        ("Remera", "Indumentaria"),
        ("Pantalón", "Indumentaria"),
        ("Zapatillas", "Indumentaria"),
        ("Café x1kg", "Alimentos"),
        ("Yerba x500g", "Alimentos"),
    ]

    estados = ["completada", "completada", "completada", "cancelada", "pendiente"]

    fechas = [
        str(date(2024, 1, 1) + timedelta(days=int(d)))
        for d in np.random.randint(0, 90, n)
    ]

    producto_idx = np.random.randint(0, len(productos), n)

    df = pd.DataFrame({
        "order_id": range(1, n + 1),
        "fecha": fechas,
        "producto": [productos[i][0] for i in producto_idx],
        "categoria": [productos[i][1] for i in producto_idx],
        "cantidad": np.random.randint(1, 10, n),
        "precio_unitario": np.random.uniform(10.0, 1500.0, n).round(2),
        "estado": [estados[np.random.randint(0, len(estados))] for _ in range(n)],
    })

    return df


def main():
    df = generar_ordenes(1000)

    os.makedirs("data/input", exist_ok=True)
    df.to_parquet("data/input/ordenes.parquet", index=False)

    print(f"[generar_datos] Dataset generado: {len(df)} órdenes")
    print(f"[generar_datos] Columnas: {list(df.columns)}")
    print(f"[generar_datos] Estados: {df['estado'].value_counts().to_dict()}")
    print(f"[generar_datos] Guardado en: data/input/ordenes.parquet")


if __name__ == "__main__":
    main()
