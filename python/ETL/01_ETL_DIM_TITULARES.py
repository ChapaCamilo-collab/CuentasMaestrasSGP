"""
ETL - Carga y actualización de DIM_TITULARES (TFM SGP CM/CMP).
Refactorizado siguiendo los estándares de código del proyecto en español.
"""

import logging
import tkinter as tk
from tkinter import filedialog
from typing import Optional

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

logger = get_logger("etl_dim_titulares")
engine = get_db_engine()


def pick_excel() -> Optional[str]:
    """Abre un cuadro de diálogo para seleccionar el archivo Excel de entrada."""
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    path = filedialog.askopenfilename(
        title="Selecciona el Excel de titulares",
        filetypes=[("Excel", "*.xlsx *.xls"), ("Todos", "*.*")]
    )
    root.destroy()
    return path if path else None


def read_excel_df(path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Lee un archivo Excel y devuelve un DataFrame con todas las columnas como cadenas."""
    sn = 0 if sheet_name is None else sheet_name
    return pd.read_excel(path, dtype=str, engine="openpyxl", sheet_name=sn)


def prepare_titulares(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Limpia y prepara los datos de titulares para la operación UPSERT."""
    df = df_raw.copy()
    df.columns = [c.strip().upper() for c in df.columns]

    target_cols = ["NIT", "DV", "RAZON_SOCIAL", "TIPO_TITULAR"]
    missing = [c for c in target_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas obligatorias en el archivo Excel: {missing}")

    processed_df = pd.DataFrame()
    for col in target_cols:
        processed_df[col] = df[col].map(clean_str)

    # Normalización
    processed_df["NIT"] = normalize_numeric_code(processed_df["NIT"], width=9)
    processed_df["DV"] = normalize_numeric_code(processed_df["DV"], width=1)
    
    # Fechas Técnicas
    hoy = pd.Timestamp.today()
    fecha_corte = pd.Timestamp(year=hoy.year, month=hoy.month, day=1).normalize()
    processed_df["FECHA_ACTUALIZACION"] = fecha_corte
    processed_df["FECHA_CREACION"] = fecha_corte

    # Validación
    for col in target_cols:
        if processed_df[col].isna().any():
            ensure_dir(OUTPUT_DIR)
            processed_df.to_csv(f"{OUTPUT_DIR}/errores_dim_titulares.csv", index=False)
            raise ValueError(f"Campo obligatorio '{col}' tiene valores nulos. Ver errores_dim_titulares.csv")

    # Deduplicación
    processed_df = processed_df.drop_duplicates(subset=["NIT"], keep="last").reset_index(drop=True)

    return processed_df


def upsert_dim_titulares(db_engine: Engine, df: pd.DataFrame):
    """Realiza la carga UPSERT de titulares en la tabla DIM_TITULARES."""
    if df.empty:
        return

    stg_name = "STG_DIM_TITULARES"
    stg_full = f"dbo.{stg_name}"
    
    to_sql_with_retry(df, db_engine, stg_name, "dbo", logger=logger)

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in {"NIT", "FECHA_CREACION"}]
    
    set_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
    
    diff_condition = " OR ".join([
        f"(t.{c} <> s.{c} OR (t.{c} IS NULL AND s.{c} IS NOT NULL) OR (t.{c} IS NOT NULL AND s.{c} IS NULL))"
        for c in update_cols if c != "FECHA_ACTUALIZACION"
    ])

    insert_cols_sql = ", ".join(cols)
    insert_vals_sql = ", ".join([f"s.{c}" for c in cols])

    merge_sql = f"""
    MERGE INTO {DB_SCHEMA}.DIM_TITULARES AS t
    USING {stg_full} AS s
      ON t.NIT = s.NIT
    WHEN MATCHED AND ({diff_condition}) THEN
      UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql});
    """

    try:
        execute_sql_with_retry(db_engine, merge_sql, "merge_titulares", logger=logger)
    finally:
        drop_table_safe(db_engine, stg_full, logger=logger)


def main():
    """Punto de ejecución principal para el ETL de Titulares."""
    path = pick_excel()
    if not path:
        return

    logger.info(f"Procesando DIM_TITULARES desde: {path}")
    df_raw = read_excel_df(path)
    df_prepared = prepare_titulares(df_raw)

    logger.info(f"Cargando {len(df_prepared)} registros...")
    upsert_dim_titulares(engine, df_prepared)
    logger.info("✅ Finalizado ETL DIM_TITULARES.")


if __name__ == "__main__":
    main()
