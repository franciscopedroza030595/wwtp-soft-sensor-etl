# Pipeline ETL — Soft Sensor para Planta de Tratamiento de Aguas Residuales

Pipeline de **Extracción, Transformación y Carga (ETL)** de datos históricos del proceso de aireación de una planta de tratamiento de aguas residuales (WWTP), orientado a la preparación de datos para el desarrollo de sensores por software (soft sensors).

> **Curso:** ETL — Semestre 2026-1  
> **Línea:** Ciencia de Datos e IA  
> **Alcance:** Exclusivamente proceso ETL. La construcción de modelos predictivos queda como trabajo futuro.

---


## 1. Contexto del Problema

Las plantas de tratamiento de aguas residuales (WWTP) operan procesos biológicos complejos donde el **proceso de aireación** consume entre el 40% y el 60% de la energía total. La operación eficiente depende del monitoreo en tiempo real de variables críticas como oxígeno disuelto (DO), nitrógeno amoniacal (NH₃), pH y sólidos suspendidos (SS).

Los **sensores físicos** que capturan estas variables enfrentan degradación por bioincrustación, deriva instrumental, fallos mecánicos y altos costos de mantenimiento. Los **soft sensors** (sensores por software) son modelos predictivos que estiman variables difíciles de medir a partir de variables fácilmente medibles (Chen et al., 2026; Haimi et al., 2013).

Sin embargo, los datos de sistemas **SCADA** industriales presentan problemas de calidad que deben resolverse antes de cualquier modelado: sensores inactivos, convenciones de señal no documentadas, gaps temporales y valores fuera de rango. Este proyecto aborda esa fase fundamental.

---


## 2. Dataset

| Característica | Valor |
|---|---|
| **Archivo** | `AmmoniaModelDataWitB1C7AmmoniumHuman.csv` |
| **Fuente** | Base de datos SQL del sistema SCADA — WWTP en Carolina del Norte, EE.UU. |
| **Registros** | 105,409 filas × 31 columnas |
| **Frecuencia** | 1 registro cada 5 minutos (288/día) |
| **Período** | 1 Nov 2023 — 1 Nov 2024 (366 días) |
| **Tamaño** | 35.33 MB |
| **Formato** | CSV con valores en unidades de ingeniería (mg/L, PSI, SCFM, GPM, MGD, °F, °C) |
| **Nulos nativos** | 1,456 (0.045% del total de celdas) |

### Categorías de variables

| Categoría | Variables | Unidades | Descripción |
|---|---|---|---|
| **Calidad efluente** | NH₃, PO₄, SS, pH, Temp | mg/L, pH, °F | Variables objetivo para soft sensors |
| **Oxígeno disuelto** | DO Basin 1-5 (Cell 5) | mg/L | Indicador de intensidad del proceso biológico |
| **Flujos de aire** | Airflow Basins 1-6 (Cells 1-6, 7-10) | SCFM | 12 señales de aireación |
| **Flujos de proceso** | Influente total, RAS, MLR | MGD, GPM | Flujos hidráulicos del sistema |
| **Calidad influente** | NH₃ distribución | mg/L | Carga orgánica de entrada |
| **Operativas** | Presión, Temp cabezal de aire | PSI, °F | Condiciones del sistema de aire |
| **Intermedias** | NH₃ Basin 1 Cell 7 | mg/L | Estado intermedio del tratamiento |

---
## 3. Arquitectura del Pipeline

```
               EXTRACCIÓN                    TRANSFORMACIÓN                         CARGA
               ══════════                    ══════════════                         ═════
               CSV (SCADA)  ──────►  1. Estandarización de timestamps    ──────►  CSV procesado
               105,409 × 31          2. Renombramiento de columnas               CSV.gz comprimido
               35.33 MB              3. Eliminación sensor inactivo              Estadísticas
                                     4. Interpolación (gaps ≤ 30 min)            Metadatos JSON
                                     5. Outliers + corrección negativos
                                     6. Validación temporal
                                     7. Feature engineering (+24 cols)
                                     ════════════════════════════════
                                     Resultado: 105,397 × 54
                                     Completitud: 99.96%
```

### Problemas de calidad resueltos

| Problema | Detalle | Solución |
|---|---|---|
| Sensor inactivo | DO Basin 6: 99.9% ceros durante 366 días | Columna eliminada |
| Flujos negativos | Basin 6 (89.1%), Basin 4 (30.7%) | Valor absoluto (convención SCADA) |
| Valores nulos | 1,456 nativos distribuidos entre columnas | Interpolación lineal (límite 30 min) |
| Gap temporal | 1h 05min por cambio de horario (DST) | Documentado, sin acción |
| Duplicados | 12 timestamps duplicados | Eliminados (conserva primer registro) |

---

## 4. Estructura del Repositorio

```
wwtp-soft-sensor-etl/
│
├── config/                                 Archivos de configuración: conexiones,
│   └── pipeline_config.yaml                parámetros del proyecto (rutas, rangos
│                                           válidos, mapeo de columnas, config de
│                                           transformación e ingeniería de features)
│
├── src/                                    Código principal del ETL: scripts y
│   ├── __init__.py                         pipelines organizados por flujo
│   └── etl_pipeline.py                     Funciones extract(), transform(), load()
│
├── utils/                                  Funciones auxiliares: conectores,
│   ├── __init__.py                         transformaciones, alertas, generadores
│   └── helpers.py                          load_config(), log_step(), validaciones
│
├── test/                                   Pruebas: unitarias, integración, pruebas
│   ├── __init__.py                         del flujo completo del ETL
│   └── test_pipeline.py                    9 tests (rangos, tipos, nulos, flujo E→T→L)
│
├── dwh_version/                            Versionamiento de esquemas del destino
│   └── schema_v1.0.0.yaml                  de carga (columnas, tipos, unidades,
│                                           changelog)
│
├── data/
│   ├── raw/                                CSV original del SCADA
│   └── processed/                          CSV procesado + gzip
│
├── output/                                 Resultados del pipeline
│   ├── data_statistics.csv                 Estadísticas descriptivas
│   ├── pipeline_metadata.json              Metadatos con trazabilidad
│   ├── series_temporales.png               Grafico de series temporales
│   ├── distribuciones.png.                 grafico de distribuciones
│   └── correlacion.png                     Matriz de correlacion
│
├── docs/                                   Documentación académica
│   ├── Entregable_2.pdf                   Problema, Indagación y Datos
│   └── Entregable_3.pdf                   Solución Final
│
├── run.py                                  Script de ejecución principal. Permite
│                                           correr flujos con argumentos desde CLI
│                                           Ej: python run.py --step=extract
│
├── requirements.txt                        Dependencias Python
├── README.md                               Este archivo
└── ETL_Pipeline_WWTP_Soft_Sensor.ipynb     Notebook para Google Colab
```

---

## 5. Ejecución

El pipeline puede ejecutarse de dos formas: desde la **línea de comandos** (CLI) usando la estructura modular del repositorio, o desde **Google Colab** usando el notebook autocontenido.

### 5.1 Ejecución desde CLI (Python local)

#### Paso 1: Clonar el repositorio

```bash
git clone https://github.com/[usuario]/wwtp-soft-sensor-etl.git
cd wwtp-soft-sensor-etl
```

#### Paso 2: Instalar dependencias

```bash
pip install -r requirements.txt
```

Las dependencias son: `pandas`, `numpy`, `matplotlib`, `seaborn`, `pyyaml`.

#### Paso 3: Colocar los datos

Copiar el archivo CSV original en la carpeta de datos crudos:

```bash
cp /ruta/al/AmmoniaModelDataWitB1C7AmmoniumHuman.csv data/raw/
```

> **Nota:** La ruta del archivo se configura en `config/pipeline_config.yaml` bajo `paths.raw_data`. Si se ubica en otra carpeta, modificar esa línea.

#### Paso 4: Ejecutar el pipeline

```bash
# Pipeline completo (extract → transform → load)
python run.py

# Solo extracción e inspección de datos crudos
python run.py --step extract

# Extracción + transformación (sin guardar archivos)
python run.py --step transform

# Pipeline completo con carga de archivos
python run.py --step load

# Usar un archivo de configuración alternativo
python run.py --config config/otra_planta.yaml
```

#### Paso 5: Verificar resultados

Los archivos generados se encuentran en:

```
data/processed/wwtp_aeration_processed.csv       ← Dataset procesado (~70 MB)
data/processed/wwtp_aeration_processed.csv.gz     ← Versión comprimida (~27 MB)
output/data_statistics.csv                        ← Estadísticas descriptivas
output/pipeline_metadata.json                     ← Metadatos del pipeline
```

#### Ejecutar pruebas

```bash
python -m pytest test/ -v
```

Las pruebas validan: existencia del archivo, dimensiones, eliminación de Basin 6 DO, ausencia de negativos en airflow, orden temporal, ausencia de duplicados, columnas de feature engineering, completitud >99%, y rangos válidos de pH y DO.

---

### 5.2 Ejecución desde Google Colab

Colab tiene todo el código inline con visualizaciones (series temporales, histogramas, correlación) — es la versión interactiva para presentación.   

El notebook `ETL_Pipeline_WWTP_Soft_Sensor.ipynb` es **autocontenido** — no requiere la estructura de carpetas del repositorio ni el archivo `run.py`. Incluye todo el código, las visualizaciones y la carga de archivos.

#### Paso 1: Abrir el notebook

Abrir `ETL_Pipeline_WWTP_Soft_Sensor.ipynb` directamente en Google Colab desde GitHub o subiendo el archivo.

#### Paso 2: Subir los datos

**Opción A — Google Drive (recomendado para archivos grandes):**

1. Subir `AmmoniaModelDataWitB1C7AmmoniumHuman.csv` a Google Drive.
2. Ejecutar la celda de montaje de Drive:
   ```python
   from google.colab import drive
   drive.mount('/content/drive')
   ```
3. Ajustar la variable `FILEPATH` en la celda de configuración:
   ```python
   FILEPATH = '/content/drive/MyDrive/AmmoniaModelDataWitB1C7AmmoniumHuman.csv'
   ```

**Opción B — Subida directa a Colab:**

1. En el panel izquierdo de Colab, hacer clic en el ícono de archivos y subir el CSV.
2. Ajustar la variable:
   ```python
   FILEPATH = '/content/AmmoniaModelDataWitB1C7AmmoniumHuman.csv'
   ```

#### Paso 3: Ejecutar todas las celdas

Ejecutar las celdas secuencialmente (`Runtime → Run all`). El pipeline completo tarda aproximadamente 30 segundos y genera:

- Series temporales, histogramas y matriz de correlación (visualización inline)
- Archivos CSV, gzip, estadísticas y metadatos en `/content/output/`

#### Paso 4: Descargar resultados

La última celda del notebook descarga automáticamente los archivos generados:

```python
from google.colab import files
files.download(f'{OUTPUT_DIR}/wwtp_aeration_processed.csv.gz')
files.download(f'{OUTPUT_DIR}/data_statistics.csv')
files.download(f'{OUTPUT_DIR}/pipeline_metadata.json')
```

---
