"""
ETL - Carga y actualización de DIM_RESGUARDOS (TFM SGP CM/CMP).
Refactorizado siguiendo los estándares de código del proyecto en español.
"""

import logging
import tkinter as tk
from tkinter import filedialog
from typing import List, Optional, Tuple

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

logger = get_logger("etl_dim_resguardos")
engine = get_db_engine()


def pick_excel() -> Optional[str]:
    """Abre un cuadro de diálogo para seleccionar el archivo Excel de entrada."""
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    path = filedialog.askopenfilename(
        title="Selecciona el Excel de resguardos",
        filetypes=[("Excel", "*.xlsx *.xls"), ("Todos", "*.*")]
    )
    root.destroy()
    return path if path else None


def read_excel_df(path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Lee un archivo Excel y devuelve un DataFrame con todas las columnas como cadenas."""
    expected_cols = {
        "COD_DEPARTAMENTO", "DEPARTAMENTO_NOMBRE", "COD_MUNICIPIO",
        "MUNICIPIO_NOMBRE", "COD_RESGUARDO", "RESGUARDO_NOMBRE", "NIT_TITULAR",
    }
    
    xls = pd.ExcelFile(path)
    if sheet_name:
        return pd.read_excel(path, sheet_name=sheet_name, dtype=str)

    # Detección automática de la hoja correcta
    for sh in xls.sheet_names:
        df_try = pd.read_excel(path, sheet_name=sh, dtype=str, nrows=5)
        cols_upper = {str(c).strip().upper() for c in df_try.columns}
        if expected_cols.intersection(cols_upper):
            return pd.read_excel(path, sheet_name=sh, dtype=str)

    return pd.read_excel(path, sheet_name=0, dtype=str)


def map_to_dim_resguardos(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Mapea las columnas del Excel a la estructura estándar de DIM_RESGUARDOS."""
    df = df_raw.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]

    column_map = {
        "COD_DEPARTAMENTO": "COD_DEPARTAMENTO", "COD_DPTO": "COD_DEPARTAMENTO",
        "DEPARTAMENTO_NOMBRE": "DEPARTAMENTO_NOMBRE", "DEPARTAMENTO": "DEPARTAMENTO_NOMBRE",
        "COD_MUNICIPIO": "COD_MUNICIPIO", "MUNICIPIO_NOMBRE": "MUNICIPIO_NOMBRE",
        "COD_RESGUARDO": "COD_RESGUARDO", "RESGUARDO_NOMBRE": "RESGUARDO_NOMBRE",
        "NIT_TITULAR": "NIT_TITULAR", "NIT": "NIT_TITULAR",
    }

    rename_map = {c: column_map[c] for c in df.columns if c in column_map}
    df = df.rename(columns=rename_map)

    target_cols = [
        "COD_DEPARTAMENTO", "DEPARTAMENTO_NOMBRE", "COD_MUNICIPIO",
        "MUNICIPIO_NOMBRE", "COD_RESGUARDO", "RESGUARDO_NOMBRE", "NIT_TITULAR"
    ]
    return df[[c for c in target_cols if c in df.columns]].copy()


def prepare_resguardos(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Limpia, normaliza y valida los datos de resguardos indígenas."""
    df = map_to_dim_resguardos(df_raw)
    
    # Normalización de códigos
    cols_to_clean = ["COD_DEPARTAMENTO", "COD_MUNICIPIO", "COD_RESGUARDO", "NIT_TITULAR"]
    for col in cols_to_clean:
        if col in df.columns:
            df[col] = normalize_numeric_code(df[col])
            # Relleno de ceros (zfill) específico
            if col == "COD_DEPARTAMENTO": df[col] = df[col].str.zfill(2)
            if col == "COD_MUNICIPIO": df[col] = df[col].str.zfill(3)
            if col == "NIT_TITULAR": df[col] = df[col].str.zfill(9)

    for col in ["DEPARTAMENTO_NOMBRE", "MUNICIPIO_NOMBRE", "RESGUARDO_NOMBRE"]:
        if col in df.columns:
            df[col] = df[col].map(clean_str)

    # Fechas Técnicas
    hoy = pd.Timestamp.today().normalize()
    df["FECHA_CREACION"] = hoy
    df["FECHA_ACTUALIZACION"] = hoy

    # Validación de campos obligatorios
    obligatory = ["COD_DEPARTAMENTO", "COD_MUNICIPIO", "COD_RESGUARDO", "NIT_TITULAR"]
    errors = []
    for col in obligatory:
        null_mask = df[col].isna()
        if null_mask.any():
            for idx in df.index[null_mask]:
                errors.append((idx, col, f"El campo {col} es obligatorio"))
    
    # Unicidad lógica
    dup_mask = df["COD_RESGUARDO"].duplicated(keep=False) & df["COD_RESGUARDO"].notna()
    if dup_mask.any():
        for idx in df.index[dup_mask]:
            errors.append((idx, "COD_RESGUARDO", "Código de resguardo duplicado en el archivo"))

    if errors:
        ensure_dir(OUTPUT_DIR)
        pd.DataFrame(errors, columns=["fila", "campo", "detalle"]).to_csv(f"{OUTPUT_DIR}/errores_dim_resguardos.csv", index=False)
        logger.warning(f"Se identificaron {len(errors)} errores técnicos. Ver errores_dim_resguardos.csv")
        df = df.drop(index=[e[0] for e in errors], errors="ignore").copy()

    return df


def upsert_dim_resguardos(db_engine: Engine, df: pd.DataFrame):
    """Realiza la carga UPSERT de resguardos en la base de datos usando MERGE."""
    if df.empty:
        return

    stg_name = "STG_DIM_RESGUARDOS"
    stg_full = f"dbo.{stg_name}"
    
    to_sql_with_retry(df, db_engine, stg_name, "dbo", logger=logger)

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in {"COD_RESGUARDO", "FECHA_CREACION"}]
    set_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
    
    diff_condition = " OR ".join([
        f"(t.{c} <> s.{c} OR (t.{c} IS NULL AND s.{c} IS NOT NULL) OR (t.{c} IS NOT NULL AND s.{c} IS NULL))"
        for c in update_cols if c != "FECHA_ACTUALIZACION"
    ])

    insert_cols_sql = ", ".join(cols)
    insert_vals_sql = ", ".join([f"s.{c}" for c in cols])

    merge_sql = f"""
    MERGE INTO {DB_SCHEMA}.DIM_RESGUARDOS AS t
    USING {stg_full} AS s
      ON t.COD_RESGUARDO = s.COD_RESGUARDO
    WHEN MATCHED AND ({diff_condition}) THEN
      UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql});
    """

    try:
        execute_sql_with_retry(db_engine, merge_sql, "merge_resguardos", logger=logger)
    finally:
        drop_table_safe(db_engine, stg_full, logger=logger)


def main():
    """Punto de ejecución principal para el ETL de Resguardos Indígenas."""
    path = pick_excel()
    if not path:
        return

    logger.info(f"Procesando DIM_RESGUARDOS desde: {path}")
    df_raw = read_excel_df(path)
    df_prepared = prepare_resguardos(df_raw)

    logger.info(f"Cargando {len(df_prepared)} registros a la base de datos...")
    upsert_dim_resguardos(engine, df_prepared)
    logger.info("✅ Finalizado ETL DIM_RESGUARDOS.")


if __name__ == "__main__":
    main()
