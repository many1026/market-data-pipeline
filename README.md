# Pipeline Context — OHLCV Market Data · Tree Search Partners

## ¿Qué es esto?

Pipeline de descarga, validación y subida de datos OHLCV horarios para un universo de **297 small/mid-cap US equities** (~$100M–$2B market cap). Los datos vienen de **Databento** y se almacenan en **GCS** para consumo del equipo quant.

---

## Infraestructura

| Componente | Detalle |
|------------|---------|
| VM | `mbp10-downloader` · GCP us-central1-a · e2-standard-4 (4 vCPU, 16 GB RAM) |
| OS | Debian 12 (bookworm) |
| Python | 3.11 |
| Storage local | 200 GB SSD (persistent disk) |
| Storage remoto | GCS bucket `tree-search-partner-data` |
| Proyecto GCP | `tree-search-partner` |

---

## Archivos del pipeline

```
pipeline/
├── config.py        # Parámetros globales: venues, schemas, paths, thresholds
├── downloader.py    # Descarga paralela desde Databento API
├── validator.py     # 9 checks de calidad sobre los parquets (ABORT/WARNING/PASS)
├── uploader.py      # Subida a GCS + carga opcional a BigQuery
├── main.py          # CLI orquestador: download / validate / upload
└── tickers.txt      # Universo de 297 tickers
```

---

## Datos descargados

- **Schema:** `ohlcv-1h` (barras OHLCV de 1 hora)
- **Venue:** 15 lit exchanges NMS vía `EDGX_PITCH` (maker-taker, alta liquidez)
- **Periodo:** 2022-01-01 → 2025-12-31
- **Universo:** 297 tickers small/mid-cap
- **Estructura local:**
```
cache/ohlcv-1h/{SYMBOL}/{VENUE}/{YYYY-MM}.parquet
```
- **Columnas:** `open, high, low, close, volume` · índice `DatetimeIndex` en `America/New_York`
- **Sesión filtrada:** 09:30–16:00 ET

---

## Dependencias instaladas en la VM

```bash
pip3 install pandas numpy pyarrow pytz tqdm python-dotenv \
             databento google-cloud-storage google-cloud-bigquery \
             --break-system-packages
```

---

## Modificaciones al código original

### `config.py`
Se agregaron al final dos variables que faltaban y que `validator.py` necesita importar:

```python
# OHLCV data directory
OHLCV_DATA_DIR = CACHE_DIR / "ohlcv_1h"

# Required columns for ohlcv-1h schema (check D)
REQUIRED_COLS["ohlcv-1h"] = ["open", "high", "low", "close", "volume"]
```

### `validator.py`
Se agregaron al final dos funciones de compatibilidad con `main.py`:

```python
# 1. Alias directo
validate_all = validate_all_ohlcv

# 2. Wrapper que entiende la estructura cache/{schema}/{symbol}/{venue}/{YYYY-MM}.parquet
#    main.py llama validate_all(schema=..., symbol_filter=...)
#    pero validate_all_ohlcv no acepta `schema` como argumento
def validate_all(schema, symbol_filter=None):
    # Consolida todos los chunks de un símbolo, concatena y valida
    ...
```

---

## Checks de validación (validator.py)

| Check | Severidad | Descripción |
|-------|-----------|-------------|
| A | ABORT | `median(close) > 10,000` → precio en fixed-point sin convertir |
| B | ABORT | OHLC ≤ 0 o volumen negativo |
| C | ABORT | Timestamps no monotónicamente crecientes |
| D | ABORT | Columnas requeridas ausentes |
| J | ABORT | Relaciones OHLC imposibles (`high < low`, etc.) |
| E | INFO | Filtro de sesión 09:30–16:00 ET |
| F | WARNING | Timestamps duplicados |
| H | WARNING | VWAP diario shift > 30% (posible corporate event) |
| I | WARNING | Barras horarias faltantes dentro de un día |

---

## Cómo correr el pipeline

```bash
# Conectarse a la VM
gcloud compute ssh mbp10-downloader --zone=us-central1-a --project=tree-search-partner

cd ~/work/pipeline

# 1. Descargar datos
python3 main.py download --mode ohlcv

# 2. Validar
python3 main.py validate --mode ohlcv

# 3. Subir a GCS (sin BigQuery)
python3 main.py upload --mode ohlcv --no-bq

# Opciones útiles
python3 main.py validate --mode ohlcv --symbol AAPL        # un solo ticker
python3 main.py validate --mode ohlcv --fix-quarantine      # re-descarga quarantined
python3 main.py upload   --mode ohlcv --overwrite           # fuerza re-upload

# Correr en background (recomendado para runs largos)
nohup python3 main.py validate --mode ohlcv > logs/validate_run.log 2>&1 &
tail -f logs/validate_run.log
```

---

## Resultado de validación (Abril 2026)

```
Validation complete: 297 passed, 0 warned, 0 quarantined / 297 total
```

Todos los tickers pasaron los 9 checks sin errores ni warnings.

---

## Variables de entorno requeridas

Crear archivo `pipeline/.env`:

```env
DATABENTO_API_KEY=db-xxxxxxxxxxxx
GCS_BUCKET=tree-search-partner-data
GCP_PROJECT=tree-search-partner
BQ_DATASET=market_data          # opcional
```

---

## Cómo bajar los archivos del pipeline a local

```bash
# Desde tu terminal local (no la VM)
gcloud compute scp \
  mbp10-downloader:~/work/pipeline/config.py \
  mbp10-downloader:~/work/pipeline/downloader.py \
  mbp10-downloader:~/work/pipeline/main.py \
  mbp10-downloader:~/work/pipeline/tickers.txt \
  mbp10-downloader:~/work/pipeline/uploader.py \
  mbp10-downloader:~/work/pipeline/validator.py \
  ~/Desktop/pipeline-local/ \
  --zone=us-central1-a \
  --project=tree-search-partner
```

> ⚠️ No incluir `cache/` ni `logs/` — el cache son varios GB de parquets.

---

## Acceso a los datos desde Python

```python
import pandas as pd

df = pd.read_parquet(
    "gs://tree-search-partner-data/raw/ohlcv-1h/APOG/",
    storage_options={"token": "google_default"},
).sort_index()
```

Ver `manual_acceso_datos.md` para guía completa de onboarding del equipo.

---

*Última actualización: Abril 2026 · Emilio · DE1 @ Tree Search Partners*
