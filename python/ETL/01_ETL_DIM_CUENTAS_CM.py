"""
ETL - Carga y actualización de DIM_CUENTAS_CM (TFM SGP CM/CMP).
Refactorizado siguiendo los estándares de código del proyecto en español.
"""

import sys
from pathlib import Path

# Agregar la raíz del proyecto al sys.path para resolver imports absolutos
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

logger = get_logger("etl_dim_cuentas_cm")
engine = get_db_engine()


def pick_excel() -> Optional[str]:
    """Abre un cuadro de diálogo para seleccionar el archivo Excel de entrada."""
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    path = filedialog.askopenfilename(
        title="Selecciona el Excel de cuentas maestras",
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
        # 1. Obtener códigos de resguardo que ya existen
        with db_engine.connect() as conn:
            df_existing = pd.read_sql(text("SELECT COD_RESGUARDO FROM sgp.DIM_RESGUARDOS"), conn)
            existing_codes = set(df_existing["COD_RESGUARDO"].astype(str).str.strip())

        # 2. Identificar códigos faltantes únicos junto con su información asociada
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
            
            # Crear mapeos únicos por código
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


def prepare_cuentas_cm(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Prepara y valida los datos de cuentas maestras para la operación UPSERT."""
    df = df_raw.copy()
    df.columns = [c.strip().upper() for c in df.columns]

    # Columnas que pueden estar en el Excel
    potential_cols = [
        "DIVIPOLA", "COD_DEPARTAMENTO", "COD_MUNICIPIO", "COD_RESGUARDO", "RESGUARDO_NOMBRE",
        "NIT_TITULAR", "DV", "TIPO_TITULAR", "SECTOR", "RUBRO",
        "TIPO_CM", "NUMERO_CM", "TIPO_CUENTA", "NIT_BANCO", "CODIGO_ACH_BANCO"
    ]

    # Columnas que son obligatorias para el destino final
    mandatory_cols = [
        "DIVIPOLA", "COD_DEPARTAMENTO", "COD_MUNICIPIO", "COD_RESGUARDO",
        "NIT_TITULAR", "DV", "TIPO_TITULAR", "SECTOR", "RUBRO",
        "TIPO_CM", "NUMERO_CM", "TIPO_CUENTA", "NIT_BANCO", "CODIGO_ACH_BANCO"
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
    processed_df["NIT_BANCO"] = normalize_numeric_code(processed_df["NIT_BANCO"], width=9)
    processed_df["CODIGO_ACH_BANCO"] = normalize_numeric_code(processed_df["CODIGO_ACH_BANCO"], width=4)

    # Validación de campos obligatorios
    obligatory_fields = [c for c in mandatory_cols if c != "COD_RESGUARDO"]

    errors = []
    for col in obligatory_fields:
        null_mask = processed_df[col].isna()
        if null_mask.any():
            for idx in processed_df.index[null_mask]:
                errors.append((idx, col, f"El campo {col} es obligatorio"))

    # Chequeo de unicidad lógica
    dup_mask = processed_df["NUMERO_CM"].duplicated(keep=False) & processed_df["NUMERO_CM"].notna()
    if dup_mask.any():
        for idx in processed_df.index[dup_mask]:
            errors.append((idx, "NUMERO_CM", "NUMERO_CM duplicado en el archivo fuente"))

    # Fechas técnicas
    fecha_corte = get_fecha_corte_mes()
    processed_df["FECHA_CREACION"] = fecha_corte
    processed_df["FECHA_ACTUALIZACION"] = fecha_corte

    if errors:
        ensure_dir(OUTPUT_DIR)
        df_err = pd.DataFrame(errors, columns=["fila", "campo", "detalle"]).drop_duplicates()
        df_err.to_csv(f"{OUTPUT_DIR}/errores_dim_cuentas_cm.csv", index=False)
        logger.warning(f"Se encontraron {len(df_err)} errores técnicos. Ver errores_dim_cuentas_cm.csv")
        processed_df = processed_df.drop(index=df_err["fila"].unique(), errors="ignore").copy()

    final_cols = mandatory_cols + ["FECHA_CREACION", "FECHA_ACTUALIZACION"]
    return processed_df[final_cols].copy()



def upsert_dim_cuentas_cm(db_engine: Engine, df: pd.DataFrame):
    """Realiza la carga UPSERT de cuentas maestras en la tabla DIM_CUENTAS_CM."""
    if df.empty:
        return

    stg_name = "STG_DIM_CUENTAS_CM"
    stg_full = f"dbo.{stg_name}"
    
    to_sql_with_retry(df, db_engine, stg_name, "dbo", logger=logger)

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in {"NUMERO_CM", "FECHA_CREACION"}]
    
    set_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
    
    diff_condition = " OR ".join([
        f"(t.{c} <> s.{c} OR (t.{c} IS NULL AND s.{c} IS NOT NULL) OR (t.{c} IS NOT NULL AND s.{c} IS NULL))"
        for c in update_cols if c != "FECHA_ACTUALIZACION"
    ])

    insert_cols_sql = ", ".join(cols)
    insert_vals_sql = ", ".join([f"s.{c}" for c in cols])

    merge_sql = f"""
    MERGE INTO {DB_SCHEMA}.DIM_CUENTAS_CM AS t
    USING {stg_full} AS s
      ON t.NUMERO_CM = s.NUMERO_CM
    WHEN MATCHED AND ({diff_condition}) THEN
      UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql});
    """

    try:
        execute_sql_with_retry(db_engine, merge_sql, "merge_cuentas_cm", logger=logger)
    finally:
        drop_table_safe(db_engine, stg_full, logger=logger)


def main():
    """Punto de ejecución principal para el ETL de Cuentas Maestras CM."""
    path = pick_excel()
    if not path:
        return

    logger.info(f"Procesando DIM_CUENTAS_CM desde: {path}")
    df_raw = read_excel_df(path)
    df_prepared = prepare_cuentas_cm(df_raw)

    logger.info(f"Cargando {len(df_prepared)} registros a la base de datos...")
    upsert_dim_cuentas_cm(engine, df_prepared)
    logger.info("✅ ETL DIM_CUENTAS_CM finalizado con éxito.")


if __name__ == "__main__":
    main()
