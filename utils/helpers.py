"""
Funciones auxiliares del pipeline ETL.
Conectores, transformaciones comunes y utilidades de logging.
"""

import yaml
import os


def load_config(config_path='config/pipeline_config.yaml'):
    """Carga la configuración del pipeline desde el archivo YAML."""
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config


def log_step(name, detail=""):
    """Imprime un encabezado formateado para cada fase del pipeline."""
    print(f"\n{'='*60}")
    print(f"  {name}")
    if detail:
        print(f"  {detail}")
    print(f"{'='*60}")


def validate_file_exists(filepath):
    """Verifica que el archivo de datos exista antes de procesarlo."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"Archivo no encontrado: {filepath}\n"
            f"Verifique la ruta en config/pipeline_config.yaml"
        )
    return True
