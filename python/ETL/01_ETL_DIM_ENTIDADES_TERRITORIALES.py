"""
ETL - Carga y actualización de DIM_ENTIDADES_TERRITORIALES (TFM SGP CM/CMP).
Refactorizado siguiendo los estándares de código del proyecto en español.
"""

import sys
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


import logging
import tkinter as tk
from tkinter import filedialog
from typing import Optional, List, Tuple

import pandas as pd
from sqlalchemy import Engine, text

from python.common.db_connection import get_db_engine
from python.common.utils import (
    clean_str,
    drop_table_safe,
    ensure_dir,
    execute_sql_with_retry,
    get_logger,
    normalize_numeric_code,
    to_sql_with_retry,
)

# -------------------------
# Configuración
# -------------------------
DB_SCHEMA: str = "sgp"
OUTPUT_DIR: str = r"c:\Users\camil\Documents\TFM-SGP-CuentasMaestras\output"

logger = get_logger("etl_dim_entidades")
engine = get_db_engine()


def pick_excel() -> Optional[str]:
    """Abre un cuadro de diálogo para seleccionar el archivo Excel de entrada."""
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    path = filedialog.askopenfilename(
        title="Selecciona el Excel de entidades territoriales",
        filetypes=[("Excel", "*.xlsx *.xls"), ("Todos", "*.*")]
    )
    root.destroy()
    return path if path else None


def read_excel_df(path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Lee un archivo Excel y devuelve un DataFrame con todas las columnas como cadenas."""
    sn = 0 if sheet_name is None else sheet_name
    return pd.read_excel(path, dtype=str, engine="openpyxl", sheet_name=sn)


def normalize_decimal(series: pd.Series) -> pd.Series:
    """Normaliza valores decimales (coordenadas) reemplazando comas por puntos."""
    s = series.astype(str).str.strip()
    s = s.replace({"nan": None, "None": None, "null": None, "": None})
    s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def prepare_entidades(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Limpia, normaliza y valida los datos de entidades territoriales."""
    df = df_raw.copy()
    df.columns = [c.strip().upper() for c in df.columns]

    column_map = {
        "TIPO_TITULAR": "TIPO_TITULAR",
        "NIT": "NIT",
        "DV": "DV",
        "RAZON_SOCIAL": "RAZON_SOCIAL",
        "RAZÓN_SOCIAL": "RAZON_SOCIAL",
        "NOMBRE_DEPARTAMENTO": "NOMBRE_DEPARTAMENTO",
        "DEPARTAMENTO": "NOMBRE_DEPARTAMENTO",
        "NOMBRE_MUNICIPIO": "NOMBRE_MUNICIPIO",
        "MUNICIPIO": "NOMBRE_MUNICIPIO",
        "CERTIFICADAS_EDU": "CERTIFICADAS_EDU",
        "CERTIFICADA_EDU": "CERTIFICADAS_EDU",
        "DIVIPOLA": "DIVIPOLA",
        "COD_DEPARTAMENTO": "COD_DEPARTAMENTO",
        "COD_DPTO": "COD_DEPARTAMENTO",
        "COD_MUNICIPIO": "COD_MUNICIPIO",
        "LATITUD": "LATITUD",
        "LONGITUD": "LONGITUD",
        "UBICACIÓN": "UBICACIÓN",
        "UBICACION": "UBICACIÓN",
    }

    rename_map = {c: column_map[c] for c in df.columns if c in column_map}
    df = df.rename(columns=rename_map)

    target_cols = [
        "TIPO_TITULAR", "NIT", "DV", "RAZON_SOCIAL", "NOMBRE_DEPARTAMENTO",
        "NOMBRE_MUNICIPIO", "CERTIFICADAS_EDU", "DIVIPOLA", "COD_DEPARTAMENTO",
        "COD_MUNICIPIO", "LATITUD", "LONGITUD", "UBICACIÓN"
    ]

    processed_df = pd.DataFrame()
    for col in target_cols:
        processed_df[col] = df[col].map(clean_str) if col in df.columns else None

    # Normalización
    processed_df["NIT"] = normalize_numeric_code(processed_df["NIT"], width=9)
    processed_df["DV"] = normalize_numeric_code(processed_df["DV"], width=1)
    processed_df["DIVIPOLA"] = normalize_numeric_code(processed_df["DIVIPOLA"], width=5)
    processed_df["COD_DEPARTAMENTO"] = normalize_numeric_code(processed_df["COD_DEPARTAMENTO"], width=2)
    processed_df["COD_MUNICIPIO"] = normalize_numeric_code(processed_df["COD_MUNICIPIO"], width=3)
    processed_df["LATITUD"] = normalize_decimal(processed_df["LATITUD"])
    processed_df["LONGITUD"] = normalize_decimal(processed_df["LONGITUD"])

    # Validación
    obligatory = [
        "TIPO_TITULAR", "NIT", "DV", "RAZON_SOCIAL", "NOMBRE_DEPARTAMENTO",
        "NOMBRE_MUNICIPIO", "DIVIPOLA", "COD_DEPARTAMENTO", "COD_MUNICIPIO", "UBICACIÓN"
    ]
    errors = []
    for col in obligatory:
        null_mask = processed_df[col].isna()
        if null_mask.any():
            for idx in processed_df.index[null_mask]:
                errors.append((idx, col, f"{col} es obligatorio"))
    
    # Duplicados
    dup_mask = processed_df["NIT"].duplicated(keep=False) & processed_df["NIT"].notna()
    if dup_mask.any():
        for idx in processed_df.index[dup_mask]:
            errors.append((idx, "NIT", "NIT duplicado en fuente"))

    if errors:
        ensure_dir(OUTPUT_DIR)
        pd.DataFrame(errors, columns=["fila", "campo", "detalle"]).to_csv(f"{OUTPUT_DIR}/errores_dim_entidades.csv", index=False)
        logger.warning(f"Se encontraron {len(errors)} errores en los datos. Ver errores_dim_entidades.csv")
        processed_df = processed_df.drop(index=[e[0] for e in errors], errors="ignore").copy()

    return processed_df


def upsert_dim_entidades(db_engine: Engine, df: pd.DataFrame):
    """Actualiza o inserta datos de entidades territoriales en la base de datos."""
    if df.empty:
        return

    stg_name = "STG_DIM_ENTIDADES_TERRITORIALES"
    stg_full = f"dbo.{stg_name}"
    
    to_sql_with_retry(df, db_engine, stg_name, "dbo", logger=logger)

    cols = list(df.columns)
    update_cols = [c for c in cols if c != "NIT"]
    set_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
    
    diff_condition = " OR ".join([
        f"(t.{c} <> s.{c} OR (t.{c} IS NULL AND s.{c} IS NOT NULL) OR (t.{c} IS NOT NULL AND s.{c} IS NULL))"
        for c in update_cols
    ])

    insert_cols_sql = ", ".join(cols)
    insert_vals_sql = ", ".join([f"s.{c}" for c in cols])

    merge_sql = f"""
    MERGE INTO {DB_SCHEMA}.DIM_ENTIDADES_TERRITORIALES AS t
    USING {stg_full} AS s
      ON t.NIT = s.NIT
    WHEN MATCHED AND ({diff_condition}) THEN
      UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql});
    """

    try:
        execute_sql_with_retry(db_engine, merge_sql, "merge_entidades", logger=logger)
    finally:
        drop_table_safe(db_engine, stg_full, logger=logger)


def main():
    """Punto de entrada principal para el ETL de Entidades Territoriales."""
    path = pick_excel()
    if not path:
        return

    logger.info(f"Procesando DIM_ENTIDADES_TERRITORIALES desde: {path}")
    df_raw = read_excel_df(path)
    df_prepared = prepare_entidades(df_raw)

    logger.info(f"Cargando {len(df_prepared)} registros...")
    upsert_dim_entidades(engine, df_prepared)
    logger.info("✅ Finalizado ETL DIM_ENTIDADES_TERRITORIALES.")


if __name__ == "__main__":
    main()
