"""
Pipeline ETL — Soft Sensor WWTP
Módulo principal: extract(), transform(), load()
Produce el mismo resultado que el notebook de Google Colab.
"""

import pandas as pd
import numpy as np
import os
import json
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.helpers import log_step, load_config


def extract(filepath):
    """Lee el CSV del SCADA e inspecciona los datos crudos."""
    log_step("FASE 1: EXTRACCIÓN", f"Fuente: {filepath}")

    df = pd.read_csv(filepath)
    metadata = {
        'archivo_fuente': os.path.basename(filepath),
        'fecha_extraccion': datetime.now().isoformat(),
        'filas_extraidas': len(df),
        'columnas_extraidas': len(df.columns),
        'tamano_mb': round(df.memory_usage(deep=True).sum() / 1e6, 2),
    }

    print(f"  Filas:    {len(df):,}")
    print(f"  Columnas: {len(df.columns)}")
    print(f"  Memoria:  {metadata['tamano_mb']} MB")

    print(f"\n  Tipos de datos:")
    for col in df.columns:
        print(f"    {col[:50]:50s} | {str(df[col].dtype)}")

    print(f"\n  Valores nulos:")
    null_counts = df.isnull().sum()
    for col in df.columns:
        n = null_counts[col]
        if n > 0:
            print(f"    {col[:50]:50s} | {n:6d} ({n/len(df)*100:.3f}%)")
    total = null_counts.sum()
    print(f"    {'TOTAL':50s} | {total:6d}")
    metadata['total_nulos_crudos'] = int(total)

    return df, metadata


def transform(df, config, metadata):
    """Aplica las 7 transformaciones idénticas al notebook Colab."""
    log_step("FASE 2: TRANSFORMACIÓN")
    transform_log = {}
    col_rename = config['column_rename']
    valid_ranges = config['valid_ranges']
    t_cfg = config['transformation']
    f_cfg = config['features']
    limit = t_cfg['interpolation_limit']

    # Paso 1: Timestamps
    print("\n  [1] Estandarización de timestamps...")
    dt_col = config['extraction']['datetime_column']
    df[dt_col] = pd.to_datetime(df[dt_col], format=config['extraction']['datetime_format'])
    df = df.dropna(subset=[dt_col]).sort_values(dt_col).reset_index(drop=True)
    print(f"      Rango: {df[dt_col].min()} → {df[dt_col].max()}")
    print(f"      Duración: {(df[dt_col].max() - df[dt_col].min()).days} días")

    # Paso 2: Renombramiento
    print("\n  [2] Renombramiento de columnas...")
    df = df.rename(columns=col_rename)
    print(f"      {len(col_rename)} columnas renombradas")

    # Paso 3: Eliminación de columnas sin información
    print("\n  [3] Eliminación de columnas sin información...")
    for col in t_cfg['columns_to_drop']:
        if col in df.columns:
            zero_pct = (df[col] == 0).sum() / len(df) * 100
            print(f"      {col}: {zero_pct:.1f}% ceros → ELIMINADA")
            df = df.drop(columns=[col])

    # Paso 4: Interpolación de valores faltantes
    print("\n  [4] Interpolación de valores faltantes...")
    null_before = df.isnull().sum().sum()
    print(f"      NaN antes: {null_before:,}")
    for col in df.select_dtypes(include=[np.number]).columns:
        is_null = df[col].isnull()
        if is_null.sum() == 0:
            continue
        groups = is_null.ne(is_null.shift()).cumsum()
        null_groups = df[is_null].groupby(groups[is_null]).size()
        small = (null_groups <= limit).sum()
        large = (null_groups > limit).sum()
        df[col] = df[col].interpolate(method=t_cfg['interpolation_method'], limit=limit)
        r = df[col].isnull().sum()
        if small > 0 or large > 0:
            print(f"      {col[:40]:40s} | gaps≤30min: {small:3d}, gaps>30min: {large}, NaN rest: {r}")
    null_after = df.isnull().sum().sum()
    print(f"      Interpolados: {null_before - null_after:,}")
    transform_log['interpolados'] = int(null_before - null_after)

    # Paso 5: Detección y tratamiento de outliers
    print("\n  [5] Detección de outliers...")
    for col, bounds in valid_ranges.items():
        if col not in df.columns:
            continue
        vmin, vmax = bounds[0], bounds[1]
        mask = (df[col] < vmin) | (df[col] > vmax)
        n = mask.sum()
        if n > 0:
            df.loc[mask, col] = np.nan
            print(f"      {col[:40]:40s} | {n:6d} outliers → NaN")

    print("\n      Flujos de aire negativos (convención SCADA):")
    airflow_cols = [c for c in df.columns if 'airflow' in c]
    for col in airflow_cols:
        neg = (df[col] < 0).sum()
        if neg > 0:
            df.loc[df[col] < 0, col] = df.loc[df[col] < 0, col].abs()
            print(f"      {col[:40]:40s} | {neg:6d} negativos → abs()")

    # Re-interpolar NaN creados por outliers
    for col in df.select_dtypes(include=[np.number]).columns:
        df[col] = df[col].interpolate(method='linear', limit=limit)

    # Paso 6: Validación temporal
    print("\n  [6] Validación temporal...")
    diffs = df['timestamp'].diff()
    gaps = diffs[diffs > pd.Timedelta(minutes=7)]
    print(f"      Gaps > 7 min: {len(gaps)}")
    if len(gaps) > 0:
        for idx in gaps.index:
            print(f"        {df['timestamp'].iloc[idx-1]} → {df['timestamp'].iloc[idx]} ({diffs.iloc[idx]})")
    dups = df['timestamp'].duplicated().sum()
    if dups > 0:
        df = df.drop_duplicates(subset='timestamp', keep='first')
        print(f"      Duplicados eliminados: {dups}")
    transform_log['gaps'] = len(gaps)
    transform_log['duplicados'] = int(dups)

    # Paso 7: Feature engineering
    print("\n  [7] Feature engineering...")
    cols_before = len(df.columns)

    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['month'] = df['timestamp'].dt.month
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    print("      ✓ Temporales: hour, day_of_week, month, is_weekend")

    do_cols = [c for c in df.columns if c.startswith('do_basin') and c.endswith('mg_l')]
    df['do_avg_mg_l'] = df[do_cols].mean(axis=1)
    print(f"      ✓ do_avg_mg_l: promedio de {len(do_cols)} basins")

    for i in range(1, 7):
        c16 = f'airflow_basin{i}_cells1_6_scfm'
        c710 = f'airflow_basin{i}_cells7_10_scfm'
        if c16 in df.columns and c710 in df.columns:
            df[f'airflow_basin{i}_total_scfm'] = df[c16] + df[c710]
    print("      ✓ airflow_basinX_total_scfm")

    mask = (df['influent_ammonium_mg_l'] > 0.1) & df['effluent_ammonium_mg_l'].notna()
    df['nh4_removal_efficiency'] = np.nan
    df.loc[mask, 'nh4_removal_efficiency'] = (
        (df.loc[mask, 'influent_ammonium_mg_l'] - df.loc[mask, 'effluent_ammonium_mg_l'])
        / df.loc[mask, 'influent_ammonium_mg_l'] * 100)
    print("      ✓ nh4_removal_efficiency (%)")

    window = f_cfg['rolling_window']
    min_p = f_cfg['rolling_min_periods']
    for var in f_cfg['key_variables']:
        if var in df.columns:
            df[f'{var}_delta'] = df[var].diff()
            df[f'{var}_rolling_mean_1h'] = df[var].rolling(window, min_periods=min_p).mean()
            df[f'{var}_rolling_std_1h'] = df[var].rolling(window, min_periods=min_p).std()
    print("      ✓ Deltas y rolling stats (1h)")

    new_features = len(df.columns) - cols_before
    print(f"      → {new_features} nuevas características")
    transform_log['features'] = new_features

    # Validación final
    total_cells = df.shape[0] * df.shape[1]
    total_nulls = df.isnull().sum().sum()
    completeness = round((1 - total_nulls / total_cells) * 100, 2)
    print(f"\n  Validación final: {df.shape} — Completitud: {completeness}%")

    transform_log['shape_final'] = list(df.shape)
    transform_log['completitud_pct'] = completeness
    metadata['transformacion'] = transform_log
    return df, metadata


def load(df, metadata, config):
    """Almacena el dataset procesado y los metadatos."""
    log_step("FASE 3: CARGA")
    paths = config['paths']

    os.makedirs(os.path.dirname(paths['processed_data']), exist_ok=True)
    os.makedirs(os.path.dirname(paths['statistics']), exist_ok=True)

    df.to_csv(paths['processed_data'], index=False)
    csv_size = os.path.getsize(paths['processed_data']) / 1e6
    print(f"  CSV:          {paths['processed_data']} ({csv_size:.1f} MB)")

    df.to_csv(paths['compressed_data'], index=False, compression='gzip')
    gz_size = os.path.getsize(paths['compressed_data']) / 1e6
    print(f"  Gzip:         {paths['compressed_data']} ({gz_size:.1f} MB)")

    stats = df.describe(include='all').T
    stats['null_count'] = df.isnull().sum()
    stats['null_pct'] = (df.isnull().sum() / len(df) * 100).round(3)
    stats.to_csv(paths['statistics'])
    print(f"  Estadísticas: {paths['statistics']}")

    metadata['carga'] = {
        'filas_finales': len(df),
        'columnas_finales': len(df.columns),
        'csv_size_mb': round(csv_size, 2),
        'gz_size_mb': round(gz_size, 2),
        'completitud_pct': metadata['transformacion']['completitud_pct'],
        'fecha_carga': datetime.now().isoformat(),
    }
    with open(paths['metadata'], 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
    print(f"  Metadatos:    {paths['metadata']}")

    return metadata
