"""
ETL - Carga y actualización de FACT_MOVIMIENTOS_CMP (TFM SGP CMP).
Versión refactorizada siguiendo los estándares de código del proyecto en español.
"""

import sys
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


import logging
import re
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from sqlalchemy import Engine, text

from python.common.db_connection import get_db_engine
from python.common.utils import (
    build_hash_id,
    clean_str,
    clean_upper,
    drop_table_safe,
    ensure_dir,
    execute_sql_with_retry,
    get_logger,
    normalize_amount,
    normalize_date,
    normalize_numeric_code,
    normalize_razon_social,
    normalize_spaces_text,
    to_sql_with_retry,
)

# -------------------------
# Configuración
# -------------------------
INPUT_DIRS: List[str] = [
    r"C:\Users\camil\Ministerio de Hacienda\Capacitación Cuentas Maestras - RBancos\Pagadoras"
]
OUTPUT_DIR: str = r"c:\Users\camil\Documents\TFM-SGP-CuentasMaestras\output"
DB_SCHEMA: str = "sgp"
TXT_ENCODING: str = "latin-1"
TXT_DELIMITER: str = ";"
YEARS_TO_PROCESS: List[int] = [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
FILE_PREFIX: str = "CMH145"

logger = get_logger("etl_movimientos_cmp")
engine = get_db_engine()
db_cache: Dict[str, Set[str]] = {}


def get_cached_table(db_engine: Engine, table_name: str, column_name: str) -> Set[str]:
    """Recupera y almacena en caché valores de una columna específica de una tabla de la base de datos."""
    if table_name not in db_cache:
        try:
            with db_engine.begin() as conn:
                df_table = pd.read_sql(text(f"SELECT {column_name} FROM {table_name}"), conn)
                db_cache[table_name] = set(df_table.iloc[:, 0].astype(str).str.strip().dropna().tolist())
        except Exception as e:
            logger.warning(f"No fue posible cargar caché para {table_name}: {e}")
            db_cache[table_name] = set()
    return db_cache[table_name]


# -------------------------
# Estructuras de Layout
# -------------------------
FIELD_NAMES = {
    "CMH145CMPA": {
        "Registro tipo 3": [
            "TIPO_DE_REGISTRO",
            "CONSECUTIVO_DE_REGISTRO",
            "CODIGO_DEPARTAMENTO_ENTIDAD_TITULAR_DE_LA_CUENTA_MAESTRA_PAGADORA",
            "CODIGO_MUNICIPIO_ENTIDAD_TITULAR_DE_LA_CUENTA_MAESTRA_PAGADORA",
            "TIPO_DE_CUENTA_MAESTRA_PAGADORA",
            "NUMERO_DE_CUENTA_MAESTRA_PAGADORA_SGP",
            "FECHA_DE_MOVIMIENTO",
            "TIPO_DE_REGISTRO_DE_MOVIMIENTO",
            "DESCRIPCION_DEL_MOVIMIENTO",
            "VALOR_DEL_REGISTRO_DE_DETALLE",
            "TIPO_DE_IDENTIFICACION_DEL_BENEFICIARIO",
            "NUMERO_DE_IDENTIFICACION_DEL_BENEFICIARIO",
            "NOMBRE_O_RAZON_SOCIAL_DEL_BENEFICIARIO",
            "CODIGO_DEL_SERVICIO",
            "NUMERO_DEL_CODIGO_UNICO_DE_SERVICIO_CUS_O_NO_DE_CODIGO_DE_SERVICIO",
        ]
    }
}

FIELD_NAMES_2023 = [
    "CONSECUTIVO", "NUMERO_DE_NIT_BANCO_TITULAR_CON_DV", "CODIGO_ACH_BANCO_TITULAR", "ANIO_REPORTE", "MES_REPORTE",
    "CODIGO_SECTOR", "TIPO_TITULAR", "CODIGO_DEPARTAMENTO_ENTIDAD_TITULAR_CMP", "CODIGO_MUNICIPIO_ENTIDAD_TITULAR_CMP",
    "NUMERO_IDENTIFICACION_TITULAR_CMP", "DV_TITULAR_CMP", "NOMBRE_ENTIDAD_TITULAR_CMP", "NOMBRE_RESGUARDO_INDIGENA",
    "TIPO_DE_CUENTA_MAESTRA_PARA_PAGOS_ELECTRONICOS", "NOMENCLATURA_CUENTA_MAESTRA_PARA_PAGOS_ELECTRONICOS",
    "NUMERO_CUENTA_MAESTRA_PARA_PAGOS_ELECTRONICOS", "NUMERO_CUENTA_MAESTRA_PRINCIPAL", "TIPO_BANCARIO_CUENTA_MAESTRA_PARA_PAGOS_ELECTRONICOS",
    "TIPO_IDENTIFICACION_BENEFICIARIO", "NUMERO_IDENTIFICACION_BENEFICIARIO", "DV_BENEFICIARIO", "NOMBRE_O_RAZON_SOCIAL_BENEFICIARIO",
    "CODIGO_DEL_SERVICIO", "CUS_O_CODIGO_DE_SERVICIO", "FECHA_MOVIMIENTO", "TIPO_DE_REGISTRO_DE_MOVIMIENTO", "DESCRIPCION_DEL_MOVIMIENTO",
    "VALOR_DEL_REGISTRO_DE_DETALLE",
]


# -------------------------
# Detección y Lectura
# -------------------------
def detect_layout(path_txt: str) -> str:
    """Detecta el diseño de registro del archivo basándose en su contenido."""
    try:
        with open(path_txt, "r", encoding=TXT_ENCODING, errors="replace") as f:
            first = next((line.strip() for line in f if line.strip()), None)
    except Exception as e:
        logger.error(f"Error detectando layout en {path_txt}: {e}")
        return "UNKNOWN"

    if not first:
        return "UNKNOWN"

    parts = first.split(TXT_DELIMITER)
    if len(parts) >= 2 and clean_str(parts[0]) in {"1", "2", "3", "4", "5", "6"}:
        return "CURRENT_RECORD_LAYOUT"
    if len(parts) >= 25:
        return "LAYOUT_2023"
    return "UNKNOWN"


def list_txt_files(input_dirs: List[str], file_prefix: Optional[str] = None) -> List[Path]:
    """Lista todos los archivos .txt en los directorios dados."""
    files = []
    for folder in input_dirs:
        base = Path(folder)
        if not base.exists():
            continue
        valid_files = [p for p in base.iterdir() if p.is_file() and p.suffix.lower() == ".txt"]
        if file_prefix:
            valid_files = [p for p in valid_files if p.name.upper().startswith(file_prefix.upper())]
        files.extend(valid_files)
    return sorted(files)


def parse_txt_type3_current(path_txt: str) -> pd.DataFrame:
    """Procesa archivos con el diseño tradicional (registros de tipo 3)."""
    cols = FIELD_NAMES["CMH145CMPA"]["Registro tipo 3"]
    rows = []
    with open(path_txt, "r", encoding=TXT_ENCODING, errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(TXT_DELIMITER)
            if clean_str(parts[0]) != "3":
                continue
            if len(parts) < len(cols):
                parts += [None] * (len(cols) - len(parts))
            rows.append(parts[: len(cols)])

    df = pd.DataFrame(rows, columns=cols)
    if not df.empty:
        df["NOMBRE_ARCHIVO_PLANO"] = Path(path_txt).stem
        df["LAYOUT_ORIGEN"] = "CURRENT_RECORD_LAYOUT"
    return df


def parse_txt_layout_2023(path_txt: str) -> pd.DataFrame:
    """Procesa archivos con el diseño de registro de 2023."""
    rows = []
    with open(path_txt, "r", encoding=TXT_ENCODING, errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(TXT_DELIMITER)
            if len(parts) < len(FIELD_NAMES_2023):
                parts += [None] * (len(FIELD_NAMES_2023) - len(parts))
            rows.append(parts[: len(FIELD_NAMES_2023)])

    df = pd.DataFrame(rows, columns=FIELD_NAMES_2023)
    if not df.empty:
        df["NOMBRE_ARCHIVO_PLANO"] = Path(path_txt).stem
        df["LAYOUT_ORIGEN"] = "LAYOUT_2023"
    return df


# -------------------------
# Preparación de Datos
# -------------------------
def prepare_movements(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Homogeniza los datos brutos al formato estándar de movimientos para CMP."""
    target_cols = [
        "ID_MOV_CMP", "TIPO_CMP", "NUMERO_CMP", "NUMERO_CM_PRINCIPAL", "FECHA_MOVIMIENTO", "TIPO_MOVIMIENTO",
        "DESCRIPCION_MOVIMIENTO", "VALOR", "TIPO_ID_BENEFICIARIO", "ID_BENEFICIARIO",
        "DV", "RAZON_SOCIAL", "CODIGO_SERVICIO", "CUS",
        "NOMBRE_ARCHIVO_PLANO", "CONSECUTIVO_FILA"
    ]

    if df_raw.empty:
        return pd.DataFrame(columns=target_cols)

    frames = []

    # Diseño actual (Tipo 3)
    df_cur = df_raw[df_raw["LAYOUT_ORIGEN"] == "CURRENT_RECORD_LAYOUT"].copy()
    if not df_cur.empty:
        mapping = {
            "CONSECUTIVO_DE_REGISTRO": "CONSECUTIVO_FILA",
            "TIPO_DE_CUENTA_MAESTRA_PAGADORA": "TIPO_CMP",
            "NUMERO_DE_CUENTA_MAESTRA_PAGADORA_SGP": "NUMERO_CMP",
            "FECHA_DE_MOVIMIENTO": "FECHA_MOVIMIENTO",
            "TIPO_DE_REGISTRO_DE_MOVIMIENTO": "TIPO_MOVIMIENTO",
            "DESCRIPCION_DEL_MOVIMIENTO": "DESCRIPCION_MOVIMIENTO",
            "VALOR_DEL_REGISTRO_DE_DETALLE": "VALOR",
            "TIPO_DE_IDENTIFICACION_DEL_BENEFICIARIO": "TIPO_ID_BENEFICIARIO",
            "NUMERO_DE_IDENTIFICACION_DEL_BENEFICIARIO": "ID_BENEFICIARIO",
            "NOMBRE_O_RAZON_SOCIAL_DEL_BENEFICIARIO": "RAZON_SOCIAL",
            "CODIGO_DEL_SERVICIO": "CODIGO_SERVICIO",
            "NUMERO_DEL_CODIGO_UNICO_DE_SERVICIO_CUS_O_NO_DE_CODIGO_DE_SERVICIO": "CUS",
        }
        df_cur = df_cur.rename(columns=mapping)
        df_cur["NUMERO_CM_PRINCIPAL"] = None
        df_cur["DV"] = None
        frames.append(df_cur)

    # Diseño 2023
    df_23 = df_raw[df_raw["LAYOUT_ORIGEN"] == "LAYOUT_2023"].copy()
    if not df_23.empty:
        mapping = {
            "CONSECUTIVO": "CONSECUTIVO_FILA",
            "TIPO_DE_CUENTA_MAESTRA_PARA_PAGOS_ELECTRONICOS": "TIPO_CMP",
            "NUMERO_CUENTA_MAESTRA_PARA_PAGOS_ELECTRONICOS": "NUMERO_CMP",
            "NUMERO_CUENTA_MAESTRA_PRINCIPAL": "NUMERO_CM_PRINCIPAL",
            "FECHA_MOVIMIENTO": "FECHA_MOVIMIENTO",
            "TIPO_DE_REGISTRO_DE_MOVIMIENTO": "TIPO_MOVIMIENTO",
            "DESCRIPCION_DEL_MOVIMIENTO": "DESCRIPCION_MOVIMIENTO",
            "VALOR_DEL_REGISTRO_DE_DETALLE": "VALOR",
            "TIPO_IDENTIFICACION_BENEFICIARIO": "TIPO_ID_BENEFICIARIO",
            "NUMERO_IDENTIFICACION_BENEFICIARIO": "ID_BENEFICIARIO",
            "DV_BENEFICIARIO": "DV",
            "NOMBRE_O_RAZON_SOCIAL_BENEFICIARIO": "RAZON_SOCIAL",
            "CODIGO_DEL_SERVICIO": "CODIGO_SERVICIO",
            "CUS_O_CODIGO_DE_SERVICIO": "CUS",
        }
        df_23 = df_23.rename(columns=mapping)
        frames.append(df_23)

    df = pd.concat(frames, ignore_index=True)

    # Normalización
    df["RAZON_SOCIAL"] = df["RAZON_SOCIAL"].map(normalize_razon_social)
    df["VALOR"] = normalize_amount(df["VALOR"])
    df["FECHA_MOVIMIENTO"] = normalize_date(df["FECHA_MOVIMIENTO"])
    df["ID_BENEFICIARIO"] = normalize_numeric_code(df["ID_BENEFICIARIO"])
    df["DV"] = normalize_numeric_code(df["DV"], width=1)
    df["TIPO_ID_BENEFICIARIO"] = df["TIPO_ID_BENEFICIARIO"].map(clean_upper)

    df["ID_MOV_CMP"] = [
        build_hash_id([a, c])
        for a, c in zip(df["NOMBRE_ARCHIVO_PLANO"], df["CONSECUTIVO_FILA"])
    ]

    return df[target_cols].copy()


# -------------------------
# Lógica UPSERT
# -------------------------
def upsert_fact_movimientos(df: pd.DataFrame):
    """Actualiza o inserta movimientos para CMP usando MERGE."""
    if df.empty:
        return
    stg_name = f"STG_MOV_CMP_{uuid.uuid4().hex[:8]}"
    cols_db = [c for c in df.columns if c not in ["NOMBRE_ARCHIVO_PLANO", "CONSECUTIVO_FILA"]]
    to_sql_with_retry(df[cols_db], engine, stg_name, "dbo", logger=logger)

    merge_sql = f"""
    MERGE INTO {DB_SCHEMA}.FACT_MOVIMIENTOS_CMP AS t
    USING dbo.{stg_name} AS s
      ON t.ID_MOV_CMP = s.ID_MOV_CMP
    WHEN MATCHED THEN
      UPDATE SET t.VALOR = s.VALOR, t.FECHA_MOVIMIENTO = s.FECHA_MOVIMIENTO
    WHEN NOT MATCHED THEN
      INSERT ({", ".join(cols_db)})
      VALUES ({", ".join([f"s.{c}" for c in cols_db])});
    """
    try:
        execute_sql_with_retry(engine, merge_sql, "merge_movimientos_cmp", logger=logger)
    finally:
        drop_table_safe(engine, f"dbo.{stg_name}", logger=logger)


# -------------------------
# Proceso ETL Principal
# -------------------------
def run_etl():
    """Punto de entrada para la ejecución principal del ETL de CMP."""
    ensure_dir(OUTPUT_DIR)
    logger.info("Iniciando ETL Fact Movimientos CMP")

    files = list_txt_files(INPUT_DIRS, FILE_PREFIX)
    if not files:
        logger.error("No se encontraron archivos CMP.")
        return

    for fp in files:
        logger.info(f"Procesando CMP: {fp.name}")
        layout = detect_layout(str(fp))
        if layout == "CURRENT_RECORD_LAYOUT":
            df_raw = parse_txt_type3_current(str(fp))
        elif layout == "LAYOUT_2023":
            df_raw = parse_txt_layout_2023(str(fp))
        else:
            continue

        if df_raw.empty:
            continue

        df_mov = prepare_movements(df_raw)
        upsert_fact_movimientos(df_mov)

    logger.info("Finalizado ETL Fact Movimientos CMP")


if __name__ == "__main__":
    run_etl()
