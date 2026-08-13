"""
Tests automatizados del pipeline e-commerce.
Valida generación de datos, transformaciones y calidad.
"""
import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from jobs.generar_datos import generar_ordenes


class TestGeneradorDatos:
    """Tests para el generador de datos sintéticos."""

    def test_genera_cantidad_correcta(self):
        df = generar_ordenes(100)
        assert len(df) == 100

    def test_columnas_esperadas(self):
        df = generar_ordenes(10)
        expected_cols = ["order_id", "fecha", "producto", "categoria",
                        "cantidad", "precio_unitario", "estado"]
        assert list(df.columns) == expected_cols

    def test_no_hay_nulls(self):
        df = generar_ordenes(500)
        assert df.isnull().sum().sum() == 0

    def test_estados_validos(self):
        df = generar_ordenes(500)
        estados_validos = {"completada", "cancelada", "pendiente"}
        assert set(df["estado"].unique()).issubset(estados_validos)

    def test_categorias_validas(self):
        df = generar_ordenes(500)
        categorias_validas = {"Tecnología", "Indumentaria", "Alimentos"}
        assert set(df["categoria"].unique()) == categorias_validas

    def test_cantidades_positivas(self):
        df = generar_ordenes(500)
        assert (df["cantidad"] > 0).all()

    def test_precios_positivos(self):
        df = generar_ordenes(500)
        assert (df["precio_unitario"] > 0).all()

    def test_order_ids_unicos(self):
        df = generar_ordenes(500)
        assert df["order_id"].is_unique

    def test_reproducibilidad_con_seed(self):
        df1 = generar_ordenes(100, seed=42)
        df2 = generar_ordenes(100, seed=42)
        pd.testing.assert_frame_equal(df1, df2)


class TestTransformaciones:
    """Tests para las transformaciones de datos."""

    def test_ingreso_total_calculado(self):
        df = generar_ordenes(100)
        df["ingreso_total"] = (df["cantidad"] * df["precio_unitario"]).round(2)
        assert "ingreso_total" in df.columns
        assert (df["ingreso_total"] > 0).all()

    def test_filtro_canceladas(self):
        df = generar_ordenes(500)
        df_clean = df[df["estado"] != "cancelada"]
        assert "cancelada" not in df_clean["estado"].values

    def test_agregacion_por_fecha(self):
        df = generar_ordenes(500)
        df["ingreso_total"] = (df["cantidad"] * df["precio_unitario"]).round(2)
        df_clean = df[df["estado"] != "cancelada"]
        agg = df_clean.groupby(["fecha", "producto"]).agg(
            total_ordenes=("order_id", "count"),
            ingreso_total=("ingreso_total", "sum"),
        ).reset_index()
        assert len(agg) > 0
        assert (agg["total_ordenes"] > 0).all()
