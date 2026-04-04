"""
ETL - Carga y actualización de DIM_CUENTAS_CMP (TFM SGP CM/CMP).
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
from typing import Optional, Set

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

logger = get_logger("etl_dim_cuentas_cmp")
engine = get_db_engine()


def pick_excel() -> Optional[str]:
    """Abre un cuadro de diálogo para seleccionar el archivo Excel de entrada."""
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    path = filedialog.askopenfilename(
        title="Selecciona el Excel de cuentas maestras pagadoras",
        filetypes=[("Excel", "*.xlsx *.xls"), ("Todos", "*.*")]
    )
    root.destroy()
    return path if path else None


def read_excel_df(path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Lee un archivo Excel y devuelve un DataFrame con todas las columnas como cadenas."""
    sn = 0 if sheet_name is None else sheet_name
    return pd.read_excel(path, dtype=str, engine="openpyxl", sheet_name=sn)


def get_fecha_corte_mes() -> pd.Timestamp:
    """Devuelve el primer día del mes actual normalizado."""
    return pd.Timestamp.today().replace(day=1).normalize()


def normalize_cod_resguardo(series: pd.Series) -> pd.Series:
    """Limpia códigos de resguardo manejando nulos y 'No Aplica'."""
    no_aplica_vals = {
        None, "", "0", "00", "000", "0000", "00000", "000000",
        "NO APLICA", "N/A", "NA", "SIN DATO"
    }
    
    def _map_val(val):
        s = clean_str(val)
        if s is None or s.upper() in no_aplica_vals:
            return None
        return "".join(ch for ch in s if ch.isdigit())

    return series.apply(_map_val)


def sync_missing_resguardos(db_engine: Engine, df: pd.DataFrame):
    """Identifica códigos de resguardo que no existen en la dimensión y los registra con información de apoyo."""
    if "COD_RESGUARDO" not in df.columns or df["COD_RESGUARDO"].isna().all():
        return

    try:
        # 1. Obtener códigos existentes
        with db_engine.connect() as conn:
            df_existing = pd.read_sql(text("SELECT COD_RESGUARDO FROM sgp.DIM_RESGUARDOS"), conn)
            existing_codes = set(df_existing["COD_RESGUARDO"].astype(str).str.strip())

        # 2. Identificar códigos faltantes únicos
        missing_mask = df["COD_RESGUARDO"].notna() & ~df["COD_RESGUARDO"].isin(existing_codes)
        if not missing_mask.any():
            return

        cols_needed = ["COD_DEPARTAMENTO", "COD_MUNICIPIO", "COD_RESGUARDO", "NIT_TITULAR"]
        if "RESGUARDO_NOMBRE" in df.columns:
            cols_needed.append("RESGUARDO_NOMBRE")

        missing_data = df[missing_mask][cols_needed].drop_duplicates("COD_RESGUARDO")
        
        if missing_data.empty:
            return

        # 3. Obtener nombres de apoyo de DIM_ENTIDADES_TERRITORIALES
        with db_engine.connect() as conn:
            query = "SELECT COD_DEPARTAMENTO, NOMBRE_DEPARTAMENTO, COD_MUNICIPIO, NOMBRE_MUNICIPIO FROM sgp.DIM_ENTIDADES_TERRITORIALES"
            df_territorios = pd.read_sql(text(query), conn)
            
            map_dpto = df_territorios.groupby("COD_DEPARTAMENTO")["NOMBRE_DEPARTAMENTO"].first().to_dict()
            map_muni = df_territorios.groupby(["COD_DEPARTAMENTO", "COD_MUNICIPIO"])["NOMBRE_MUNICIPIO"].first().to_dict()

        logger.info(f"Registrando {len(missing_data)} resguardos nuevos en DIM_RESGUARDOS.")
        
        now = pd.Timestamp.today().normalize()
        rows_to_insert = []

        for _, row in missing_data.iterrows():
            cd = row["COD_DEPARTAMENTO"]
            cm = row["COD_MUNICIPIO"]
            
            dpto_nom = map_dpto.get(cd, "POR REGISTRAR")
            muni_nom = map_muni.get((cd, cm))
            if not muni_nom:
                muni_nom = f"{cd}{cm}" if cd and cm else "POR REGISTRAR"
                
            rows_to_insert.append({
                "COD_DEPARTAMENTO": cd,
                "DEPARTAMENTO_NOMBRE": dpto_nom,
                "COD_MUNICIPIO": cm,
                "MUNICIPIO_NOMBRE": muni_nom,
                "COD_RESGUARDO": row["COD_RESGUARDO"],
                "RESGUARDO_NOMBRE": row.get("RESGUARDO_NOMBRE", "POR REGISTRAR"),
                "NIT_TITULAR": row["NIT_TITULAR"],
                "FECHA_CREACION": now,
                "FECHA_ACTUALIZACION": now
            })

        new_resguardos = pd.DataFrame(rows_to_insert)
        new_resguardos.to_sql("DIM_RESGUARDOS", schema="sgp", con=db_engine, if_exists="append", index=False)
        
    except Exception as e:
        logger.error(f"Error al sincronizar resguardos faltantes: {e}")


def prepare_cuentas_cmp(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Prepara y valida los datos de cuentas maestras pagadoras."""
    df = df_raw.copy()
    df.columns = [c.strip().upper() for c in df.columns]

    column_map = {
        "DIVIPOLA": "DIVIPOLA", "COD_DEPARTAMENTO": "COD_DEPARTAMENTO",
        "COD_MUNICIPIO": "COD_MUNICIPIO", "COD_RESGUARDO": "COD_RESGUARDO",
        "NIT_TITULAR": "NIT_TITULAR", "NIT": "NIT_TITULAR", "DV": "DV",
        "TIPO_TITULAR": "TIPO_TITULAR", "SECTOR": "SECTOR", "RUBRO": "RUBRO",
        "TIPO_CMP": "TIPO_CMP", "NUMERO_CMP": "NUMERO_CMP", "NUM_CMP": "NUMERO_CMP",
        "NUMERO_CM_PRINCIPAL": "NUMERO_CM_PRINCIPAL", "NUMERO_CM": "NUMERO_CM_PRINCIPAL",
        "TIPO_CUENTA": "TIPO_CUENTA", "NIT_BANCO": "NIT_BANCO",
        "CODIGO_ACH_BANCO": "CODIGO_ACH_BANCO", "COD_ACH_BANCO": "CODIGO_ACH_BANCO",
    }

    rename_map = {c: column_map[c] for c in df.columns if c in column_map}
    df = df.rename(columns=rename_map)

    # Columnas que pueden estar en el Excel
    potential_cols = [
        "DIVIPOLA", "COD_DEPARTAMENTO", "COD_MUNICIPIO", "COD_RESGUARDO", "RESGUARDO_NOMBRE",
        "NIT_TITULAR", "DV", "TIPO_CMP", "NUMERO_CMP", "NUMERO_CM_PRINCIPAL", "TIPO_CUENTA",
        "NIT_BANCO", "CODIGO_ACH_BANCO"
    ]

    # Columnas obligatorias para el destino
    mandatory_cols = [
        "DIVIPOLA", "COD_DEPARTAMENTO", "COD_MUNICIPIO", "COD_RESGUARDO",
        "NIT_TITULAR", "DV", "TIPO_CMP", "NUMERO_CMP"
    ]

    missing = [c for c in mandatory_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas obligatorias en el archivo Excel: {missing}")

    processed_df = pd.DataFrame()
    for col in potential_cols:
        if col in df.columns:
            processed_df[col] = df[col].map(clean_str)
        else:
            processed_df[col] = None

    # Normalización
    processed_df["DIVIPOLA"] = normalize_numeric_code(processed_df["DIVIPOLA"], width=5)
    processed_df["COD_DEPARTAMENTO"] = normalize_numeric_code(processed_df["COD_DEPARTAMENTO"], width=2)
    processed_df["COD_MUNICIPIO"] = normalize_numeric_code(processed_df["COD_MUNICIPIO"], width=3)
    processed_df["COD_RESGUARDO"] = normalize_cod_resguardo(processed_df["COD_RESGUARDO"])

    # Sincronizar resguardos faltantes en DIM_RESGUARDOS
    sync_missing_resguardos(engine, processed_df)


    processed_df["NIT_TITULAR"] = normalize_numeric_code(processed_df["NIT_TITULAR"], width=9)
    processed_df["DV"] = normalize_numeric_code(processed_df["DV"], width=1)
    processed_df["TIPO_CMP"] = processed_df["TIPO_CMP"].str.upper()
    processed_df["NIT_BANCO"] = normalize_numeric_code(processed_df["NIT_BANCO"], width=9)
    processed_df["CODIGO_ACH_BANCO"] = normalize_numeric_code(processed_df["CODIGO_ACH_BANCO"], width=4)

    # Validación
    obligatory = [c for c in mandatory_cols if c != "COD_RESGUARDO"]

    errors = []
    for col in obligatory:
        if processed_df[col].isna().any():
            for idx in processed_df.index[processed_df[col].isna()]:
                errors.append((idx, col, f"{col} es obligatorio"))
    
    # Duplicados
    dup_mask = processed_df["NUMERO_CMP"].duplicated(keep=False) & processed_df["NUMERO_CMP"].notna()
    if dup_mask.any():
        for idx in processed_df.index[dup_mask]:
            errors.append((idx, "NUMERO_CMP", "NUMERO_CMP duplicado en fuente"))

    if errors:
        ensure_dir(OUTPUT_DIR)
        df_err = pd.DataFrame(errors, columns=["fila", "campo", "detalle"]).drop_duplicates()
        df_err.to_csv(f"{OUTPUT_DIR}/errores_dim_cuentas_cmp.csv", index=False)
        logger.warning(f"Se encontraron {len(df_err)} errores técnicos. Ver errores_dim_cuentas_cmp.csv")
        processed_df = processed_df.drop(index=df_err["fila"].unique(), errors="ignore").copy()

    # Fechas técnicas
    fecha_corte = get_fecha_corte_mes()
    processed_df["FECHA_CREACION"] = fecha_corte
    processed_df["FECHA_ACTUALIZACION"] = fecha_corte

    final_cols = mandatory_cols + ["FECHA_CREACION", "FECHA_ACTUALIZACION"]
    return processed_df[final_cols].copy()


def upsert_dim_cuentas_cmp(db_engine: Engine, df: pd.DataFrame):
    """Actualiza o inserta cuentas CMP en la base de datos."""
    if df.empty:
        return

    stg_name = "STG_DIM_CUENTAS_CMP"
    stg_full = f"dbo.{stg_name}"
    
    to_sql_with_retry(df, db_engine, stg_name, "dbo", logger=logger)

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in {"NUMERO_CMP", "FECHA_CREACION"}]
    set_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
    
    diff_condition = " OR ".join([
        f"(t.{c} <> s.{c} OR (t.{c} IS NULL AND s.{c} IS NOT NULL) OR (t.{c} IS NOT NULL AND s.{c} IS NULL))"
        for c in update_cols if c != "FECHA_ACTUALIZACION"
    ])

    insert_cols_sql = ", ".join(cols)
    insert_vals_sql = ", ".join([f"s.{c}" for c in cols])

    merge_sql = f"""
    MERGE INTO {DB_SCHEMA}.DIM_CUENTAS_CMP AS t
    USING {stg_full} AS s
      ON t.NUMERO_CMP = s.NUMERO_CMP
    WHEN MATCHED AND ({diff_condition}) THEN
      UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql});
    """

    try:
        execute_sql_with_retry(db_engine, merge_sql, "merge_cuentas_cmp", logger=logger)
    finally:
        drop_table_safe(db_engine, stg_full, logger=logger)


def main():
    """Punto de entrada para el ETL de Cuentas Maestras Pagadoras."""
    path = pick_excel()
    if not path:
        return

    logger.info(f"Procesando DIM_CUENTAS_CMP desde: {path}")
    df_raw = read_excel_df(path)
    df_prepared = prepare_cuentas_cmp(df_raw)

    logger.info(f"Cargando {len(df_prepared)} cuentas...")
    upsert_dim_cuentas_cmp(engine, df_prepared)
    logger.info("✅ Finalizado ETL DIM_CUENTAS_CMP.")


if __name__ == "__main__":
    main()
