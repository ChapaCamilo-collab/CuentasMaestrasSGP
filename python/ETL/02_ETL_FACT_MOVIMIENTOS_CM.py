"""
ETL - Carga y actualización de DIM_BENEFICIARIOS y FACT_MOVIMIENTOS_CM.
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
    r"C:\Users\camil\Ministerio de Hacienda\Capacitación Cuentas Maestras - RBancos\Prinicipales",
    r"C:\Users\camil\Ministerio de Hacienda\Capacitación Cuentas Maestras - RBancos\Resguardos Indigenas",
]
OUTPUT_DIR: str = r"c:\Users\camil\Documents\TFM-SGP-CuentasMaestras\output"
DB_SCHEMA: str = "sgp"
TXT_ENCODING: str = "latin-1"
TXT_DELIMITER: str = ";"
YEARS_TO_PROCESS: List[int] = [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
FILE_PREFIX: str = "CMH145"

logger = get_logger("etl_movimientos_cm")
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
    "CMH145CMPG": {
        "Registro tipo 4": [
            "TIPO_DE_REGISTRO",
            "CONSECUTIVO_DE_REGISTRO_DENTRO_DEL_ARCHIVO",
            "CODIGO_DEPARTAMENTO_ENTIDAD_TITULAR_DE_LA_CUENTA_MAESTRA_O_DEL_LUGAR_DE_LA_APERTURA_DE_LA_CUENTA_MAESTRA",
            "CODIGO_MUNICIPIO_ENTIDAD_TITULAR_DE_LA_CUENTA_MAESTRA_O_DEL_LUGAR_DE_APERTURA_DE_LA_CUENTA_MAESTRA",
            "TIPO_DE_CUENTA_MAESTRA",
            "NUMERO_DE_CUENTA_MAESTRA",
            "FECHA_DEL_MOVIMIENTO",
            "TIPO_DE_REGISTRO_DE_MOVIMIENTO",
            "DESCRIPCION_DEL_MOVIMIENTO",
            "VALOR_DEL_REGISTRO_DE_DETALLE",
            "TIPO_IDENTIFICACION_DEL_BENEFICIARIO_DEL_EGRESO_O_DE_LA_FUENTE_DE_INGRESO",
            "NUMERO_DE_IDENTIFICACION_DEL_BENEFICIARIO_DEL_EGRESO_O_DE_LA_FUENTE_DE_INGRESO",
            "DIGITO_DE_VERIFICACION_DEL_NIT_DEL_BENEFICIARIO_O_DE_LA_FUENTE_DE_INGRESO",
            "NOMBRE_O_RAZON_SOCIAL_DEL_BENEFICIARIO_DEL_EGRESO_O_DE_LA_FUENTE_DE_INGRESO",
            "CODIGO_DE_LA_ENTIDAD_BANCARIA_DE_LA_CUENTA_DEL_BENEFICIARIO_O_DE_LA_FUENTE_DE_INGRESO",
            "NUMERO_DE_CUENTA_BANCARIA_DEL_BENEFICIARIO_O_DE_LA_FUENTE_DE_INGRESO",
            "TIPO_DE_CUENTA_BANCARIA_DEL_BENEFICIARIO_O_DE_LA_FUENTE_DE_INGRESO",
        ]
    }
}

FIELD_NAMES_2023 = [
    "SECUENCIA", "NIT_ENTIDAD_REPORTE", "CODIGO_REPORTE", "ANIO", "MES", "SECTOR_ORIGEN",
    "TIPO_TITULAR", "COD_DEPARTAMENTO", "COD_MUNICIPIO", "NIT_TITULAR", "DV_TITULAR",
    "RAZON_SOCIAL_TITULAR", "CAMPO_VACIO_1", "TIPO_CM", "NOMBRE_CUENTA", "NUMERO_CM",
    "TIPO_CUENTA_CM", "TIPO_ID_BENEFICIARIO", "ID_BENEFICIARIO", "DV_BENEFICIARIO",
    "RAZON_SOCIAL_BENEFICIARIO", "CODIGO_BANCO", "CAMPO_2023_1", "CUENTA_BENEFICIARIO",
    "TIPO_CUENTA_BENEFICIARIO", "CAMPO_2023_2", "FECHA_MOVIMIENTO", "TIPO_MOVIMIENTO",
    "CAMPO_2023_3", "VALOR",
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
    """Lista todos los archivos .txt en los directorios dados con un prefijo opcional."""
    files = []
    for folder in input_dirs:
        base = Path(folder)
        if not base.exists():
            logger.warning(f"Ruta no encontrada: {folder}")
            continue
        valid_files = [p for p in base.iterdir() if p.is_file() and p.suffix.lower() == ".txt"]
        if file_prefix:
            valid_files = [p for p in valid_files if p.name.upper().startswith(file_prefix.upper())]
        files.extend(valid_files)
    return sorted(files)


def parse_txt_type4_current(path_txt: str) -> pd.DataFrame:
    """Procesa archivos con el diseño tradicional (registros de tipo 4)."""
    cols = FIELD_NAMES["CMH145CMPG"]["Registro tipo 4"]
    rows = []
    with open(path_txt, "r", encoding=TXT_ENCODING, errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(TXT_DELIMITER)
            if clean_str(parts[0]) != "4":
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
    """Homogeniza los datos brutos al formato estándar de movimientos."""
    target_cols = [
        "ID_MOV_CM", "TIPO_CM", "NUMERO_CM", "FECHA_MOVIMIENTO", "TIPO_MOVIMIENTO",
        "DESCRIPCION_MOVIMIENTO", "VALOR", "TIPO_ID_BENEFICIARIO", "ID_BENEFICIARIO",
        "DV", "RAZON_SOCIAL", "CODIGO_BANCO", "CUENTA_BENEFICIARIO", "TIPO_CUENTA_BENEFICIARIO",
        "NOMBRE_ARCHIVO_PLANO", "CONSECUTIVO_FILA"
    ]

    if df_raw.empty:
        return pd.DataFrame(columns=target_cols)

    frames = []

    # Diseño actual (Tipo 4)
    df_cur = df_raw[df_raw["LAYOUT_ORIGEN"] == "CURRENT_RECORD_LAYOUT"].copy()
    if not df_cur.empty:
        mapping = {
            "CONSECUTIVO_DE_REGISTRO_DENTRO_DEL_ARCHIVO": "CONSECUTIVO_FILA",
            "TIPO_DE_CUENTA_MAESTRA": "TIPO_CM",
            "NUMERO_DE_CUENTA_MAESTRA": "NUMERO_CM",
            "FECHA_DEL_MOVIMIENTO": "FECHA_MOVIMIENTO",
            "TIPO_DE_REGISTRO_DE_MOVIMIENTO": "TIPO_MOVIMIENTO",
            "DESCRIPCION_DEL_MOVIMIENTO": "DESCRIPCION_MOVIMIENTO",
            "VALOR_DEL_REGISTRO_DE_DETALLE": "VALOR",
            "TIPO_IDENTIFICACION_DEL_BENEFICIARIO_DEL_EGRESO_O_DE_LA_FUENTE_DE_INGRESO": "TIPO_ID_BENEFICIARIO",
            "NUMERO_DE_IDENTIFICACION_DEL_BENEFICIARIO_DEL_EGRESO_O_DE_LA_FUENTE_DE_INGRESO": "ID_BENEFICIARIO",
            "DIGITO_DE_VERIFICACION_DEL_NIT_DEL_BENEFICIARIO_O_DE_LA_FUENTE_DE_INGRESO": "DV",
            "NOMBRE_O_RAZON_SOCIAL_DEL_BENEFICIARIO_DEL_EGRESO_O_DE_LA_FUENTE_DE_INGRESO": "RAZON_SOCIAL",
            "CODIGO_DE_LA_ENTIDAD_BANCARIA_DE_LA_CUENTA_DEL_BENEFICIARIO_O_DE_LA_FUENTE_DE_INGRESO": "CODIGO_BANCO",
            "NUMERO_DE_CUENTA_BANCARIA_DEL_BENEFICIARIO_O_DE_LA_FUENTE_DE_INGRESO": "CUENTA_BENEFICIARIO",
            "TIPO_DE_CUENTA_BANCARIA_DEL_BENEFICIARIO_O_DE_LA_FUENTE_DE_INGRESO": "TIPO_CUENTA_BENEFICIARIO",
        }
        df_cur = df_cur.rename(columns=mapping)
        frames.append(df_cur)

    # Diseño 2023
    df_23 = df_raw[df_raw["LAYOUT_ORIGEN"] == "LAYOUT_2023"].copy()
    if not df_23.empty:
        mapping = {
            "SECUENCIA": "CONSECUTIVO_FILA",
            "TIPO_CM": "TIPO_CM",
            "NUMERO_CM": "NUMERO_CM",
            "FECHA_MOVIMIENTO": "FECHA_MOVIMIENTO",
            "TIPO_MOVIMIENTO": "TIPO_MOVIMIENTO",
            "RAZON_SOCIAL_BENEFICIARIO": "RAZON_SOCIAL",
            "DV_BENEFICIARIO": "DV",
        }
        df_23 = df_23.rename(columns=mapping)
        frames.append(df_23)

    df = pd.concat(frames, ignore_index=True)

    # Normalización
    df["RAZON_SOCIAL"] = df["RAZON_SOCIAL"].map(normalize_razon_social)
    df["VALOR"] = normalize_amount(df["VALOR"])
    df["FECHA_MOVIMIENTO"] = normalize_date(df["FECHA_MOVIMIENTO"])
    df["ID_BENEFICIARIO"] = normalize_numeric_code(df["ID_BENEFICIARIO"])
    df["CODIGO_BANCO"] = normalize_numeric_code(df["CODIGO_BANCO"], width=4)
    df["TIPO_ID_BENEFICIARIO"] = df["TIPO_ID_BENEFICIARIO"].map(clean_upper)
    df["CUENTA_BENEFICIARIO"] = normalize_numeric_code(df["CUENTA_BENEFICIARIO"])

    df["ID_MOV_CM"] = [
        build_hash_id([a, c])
        for a, c in zip(df["NOMBRE_ARCHIVO_PLANO"], df["CONSECUTIVO_FILA"])
    ]

    return df[target_cols].copy()


def prepare_beneficiarios(df_mov: pd.DataFrame) -> pd.DataFrame:
    """Extrae beneficiarios únicos a partir de los movimientos."""
    cols = ["TIPO_ID_BENEFICIARIO", "ID_BENEFICIARIO", "DV", "RAZON_SOCIAL", "CODIGO_BANCO", "CUENTA_BENEFICIARIO", "TIPO_CUENTA_BENEFICIARIO"]
    df = df_mov[cols].drop_duplicates().copy()
    return df


# -------------------------
# Validaciones
# -------------------------
def validate_movements(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Validación básica de movimientos."""
    mask_err = df["ID_MOV_CM"].isna() | df["VALOR"].isna()
    return df[~mask_err].copy(), df[mask_err].copy()


# -------------------------
# Lógica UPSERT
# -------------------------
def upsert_dim_beneficiarios(df: pd.DataFrame):
    """Actualiza o inserta beneficiarios usando MERGE."""
    if df.empty:
        return
    stg_name = f"STG_BEN_{uuid.uuid4().hex[:8]}"
    to_sql_with_retry(df, engine, stg_name, "dbo", logger=logger)
    
    merge_sql = f"""
    MERGE INTO {DB_SCHEMA}.DIM_BENEFICIARIOS AS t
    USING dbo.{stg_name} AS s
      ON t.ID_BENEFICIARIO = s.ID_BENEFICIARIO
     AND t.CODIGO_BANCO = s.CODIGO_BANCO
     AND t.CUENTA_BENEFICIARIO = s.CUENTA_BENEFICIARIO
    WHEN NOT MATCHED THEN
      INSERT (TIPO_ID_BENEFICIARIO, ID_BENEFICIARIO, DV, RAZON_SOCIAL, CODIGO_BANCO, CUENTA_BENEFICIARIO, TIPO_CUENTA_BENEFICIARIO)
      VALUES (s.TIPO_ID_BENEFICIARIO, s.ID_BENEFICIARIO, s.DV, s.RAZON_SOCIAL, s.CODIGO_BANCO, s.CUENTA_BENEFICIARIO, s.TIPO_CUENTA_BENEFICIARIO);
    """
    try:
        execute_sql_with_retry(engine, merge_sql, "merge_beneficiarios", logger=logger)
    finally:
        drop_table_safe(engine, f"dbo.{stg_name}", logger=logger)


def upsert_fact_movimientos(df: pd.DataFrame):
    """Actualiza o inserta movimientos usando MERGE."""
    if df.empty:
        return
    stg_name = f"STG_MOV_{uuid.uuid4().hex[:8]}"
    # Seleccionar solo columnas en DB
    cols_db = [c for c in df.columns if c not in ["NOMBRE_ARCHIVO_PLANO", "CONSECUTIVO_FILA"]]
    to_sql_with_retry(df[cols_db], engine, stg_name, "dbo", logger=logger)

    merge_sql = f"""
    MERGE INTO {DB_SCHEMA}.FACT_MOVIMIENTOS_CM AS t
    USING dbo.{stg_name} AS s
      ON t.ID_MOV_CM = s.ID_MOV_CM
    WHEN MATCHED THEN
      UPDATE SET t.VALOR = s.VALOR, t.FECHA_MOVIMIENTO = s.FECHA_MOVIMIENTO
    WHEN NOT MATCHED THEN
      INSERT ({", ".join(cols_db)})
      VALUES ({", ".join([f"s.{c}" for c in cols_db])});
    """
    try:
        execute_sql_with_retry(engine, merge_sql, "merge_movimientos", logger=logger)
    finally:
        drop_table_safe(engine, f"dbo.{stg_name}", logger=logger)


# -------------------------
# Proceso ETL Principal
# -------------------------
def run_etl():
    """Punto de entrada para la ejecución principal."""
    ensure_dir(OUTPUT_DIR)
    logger.info("Iniciando ETL Fact Movimientos CM")

    files = list_txt_files(INPUT_DIRS, FILE_PREFIX)
    if not files:
        logger.error("No se encontraron archivos.")
        return

    for fp in files:
        logger.info(f"Procesando: {fp.name}")
        layout = detect_layout(str(fp))
        if layout == "CURRENT_RECORD_LAYOUT":
            df_raw = parse_txt_type4_current(str(fp))
        elif layout == "LAYOUT_2023":
            df_raw = parse_txt_layout_2023(str(fp))
        else:
            logger.warning(f"Layout desconocido: {fp.name}")
            continue

        if df_raw.empty:
            continue

        df_mov = prepare_movements(df_raw)
        df_ben = prepare_beneficiarios(df_mov)

        upsert_dim_beneficiarios(df_ben)
        upsert_fact_movimientos(df_mov)
        logger.info(f"Fin archivo: {fp.name}")


if __name__ == "__main__":
    run_etl()
