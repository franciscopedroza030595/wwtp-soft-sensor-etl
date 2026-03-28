"""
Pruebas del Pipeline ETL - Soft Sensor WWTP

Pruebas unitarias, de integración y de flujo completo
para validar el correcto funcionamiento del pipeline.
"""

import unittest
import pandas as pd
import numpy as np
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.helpers import load_config
from src.etl_pipeline import extract, transform, load


class TestExtraction(unittest.TestCase):
    """Pruebas unitarias para la fase de extracción."""

    def setUp(self):
        self.config = load_config('config/pipeline_config.yaml')
        self.filepath = self.config['paths']['raw_data']

    def test_file_exists(self):
        self.assertTrue(os.path.exists(self.filepath), "Archivo de datos no encontrado")

    def test_extract_returns_dataframe(self):
        df, meta = extract(self.filepath)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)

    def test_extract_column_count(self):
        df, meta = extract(self.filepath)
        self.assertEqual(len(df.columns), 31, "Se esperan 31 columnas en datos crudos")

    def test_extract_row_count(self):
        df, meta = extract(self.filepath)
        self.assertGreater(len(df), 100000, "Se esperan más de 100k registros")


class TestTransformation(unittest.TestCase):
    """Pruebas unitarias para la fase de transformación."""

    def setUp(self):
        self.config = load_config('config/pipeline_config.yaml')
        df, self.metadata = extract(self.config['paths']['raw_data'])
        self.df_transformed, self.metadata = transform(df, self.config, self.metadata)

    def test_no_basin6_do(self):
        self.assertNotIn('do_basin6_mg_l', self.df_transformed.columns,
                         "do_basin6_mg_l debería estar eliminada")

    def test_no_negative_airflow(self):
        airflow_cols = [c for c in self.df_transformed.columns if 'airflow' in c]
        for col in airflow_cols:
            min_val = self.df_transformed[col].min()
            self.assertGreaterEqual(min_val, 0, f"{col} tiene valores negativos")

    def test_timestamp_sorted(self):
        ts = self.df_transformed['timestamp']
        self.assertTrue(ts.is_monotonic_increasing, "Timestamps no están ordenados")

    def test_no_duplicate_timestamps(self):
        dups = self.df_transformed['timestamp'].duplicated().sum()
        self.assertEqual(dups, 0, "Existen timestamps duplicados")

    def test_feature_engineering_columns(self):
        expected = ['hour', 'day_of_week', 'month', 'is_weekend', 'do_avg_mg_l',
                    'nh4_removal_efficiency']
        for col in expected:
            self.assertIn(col, self.df_transformed.columns, f"Falta columna: {col}")

    def test_completeness_above_99(self):
        total = self.df_transformed.shape[0] * self.df_transformed.shape[1]
        nulls = self.df_transformed.isnull().sum().sum()
        completeness = (1 - nulls / total) * 100
        self.assertGreater(completeness, 99.0, "Completitud menor al 99%")

    def test_ph_range(self):
        ph = self.df_transformed['effluent_ph'].dropna()
        self.assertTrue(ph.min() >= 4.0 and ph.max() <= 10.0,
                        "pH fuera de rango válido (4-10)")

    def test_do_range(self):
        do = self.df_transformed['do_avg_mg_l'].dropna()
        self.assertTrue(do.min() >= 0 and do.max() <= 15.0,
                        "DO promedio fuera de rango (0-15)")


class TestFullPipeline(unittest.TestCase):
    """Prueba de integración del flujo completo del ETL."""

    def test_full_pipeline(self):
        config = load_config('config/pipeline_config.yaml')
        df_raw, metadata = extract(config['paths']['raw_data'])
        df_clean, metadata = transform(df_raw, config, metadata)
        metadata = load(df_clean, metadata, config)

        self.assertTrue(os.path.exists(config['paths']['processed_data']))
        self.assertTrue(os.path.exists(config['paths']['compressed_data']))
        self.assertTrue(os.path.exists(config['paths']['statistics']))
        self.assertTrue(os.path.exists(config['paths']['metadata']))

        self.assertEqual(metadata['carga']['columnas_finales'], len(df_clean.columns))


if __name__ == '__main__':
    unittest.main(verbosity=2)
