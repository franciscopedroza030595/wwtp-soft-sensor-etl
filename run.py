"""
Script de ejecución principal del pipeline ETL.

Uso:
    python run.py                              # Pipeline completo
    python run.py --step extract               # Solo extracción
    python run.py --step transform             # Solo transformación
    python run.py --step load                  # Pipeline completo con carga
    python run.py --config config/other.yaml   # Config alternativa
"""

import argparse
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.helpers import load_config, validate_file_exists
from src.etl_pipeline import extract, transform, load


def main():
    parser = argparse.ArgumentParser(description='Pipeline ETL — Soft Sensor WWTP')
    parser.add_argument('--step', type=str, default='all',
                        choices=['all', 'extract', 'transform', 'load'],
                        help='Paso del pipeline a ejecutar (default: all)')
    parser.add_argument('--config', type=str, default='config/pipeline_config.yaml',
                        help='Ruta al archivo de configuración YAML')
    args = parser.parse_args()

    print("\n" + "▓" * 60)
    print("  PIPELINE ETL — SOFT SENSOR WWTP")
    print("  Proceso de Aireación — Datos SCADA")
    print("▓" * 60)

    config = load_config(args.config)
    filepath = config['paths']['raw_data']
    validate_file_exists(filepath)

    if args.step in ('all', 'extract'):
        df_raw, metadata = extract(filepath)
        if args.step == 'extract':
            print("\n✓ Extracción completada.\n")
            return

    if args.step in ('all', 'transform'):
        if args.step == 'transform':
            df_raw, metadata = extract(filepath)
        df_clean, metadata = transform(df_raw, config, metadata)
        if args.step == 'transform':
            print("\n✓ Transformación completada.\n")
            return

    if args.step in ('all', 'load'):
        if args.step == 'load':
            df_raw, metadata = extract(filepath)
            df_clean, metadata = transform(df_raw, config, metadata)
        metadata = load(df_clean, metadata, config)

    print(f"\n{'='*60}")
    print(f"  RESUMEN DEL PIPELINE ETL")
    print(f"{'='*60}")
    print(f"  Fuente:            {metadata['archivo_fuente']}")
    print(f"  Filas extraídas:   {metadata['filas_extraidas']:,}")
    print(f"  Filas finales:     {metadata['carga']['filas_finales']:,}")
    print(f"  Columnas finales:  {metadata['carga']['columnas_finales']}")
    print(f"  Completitud:       {metadata['carga']['completitud_pct']}%")
    print(f"  CSV:               {metadata['carga']['csv_size_mb']} MB")
    print(f"  Gzip:              {metadata['carga']['gz_size_mb']} MB")
    print(f"{'='*60}")
    print("\n✓ Pipeline ETL completado exitosamente.\n")


if __name__ == '__main__':
    main()
