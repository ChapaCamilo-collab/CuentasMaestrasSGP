"""
ETL - Carga y actualización de DIM_BANCOS (TFM SGP CM/CMP).
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
from typing import Optional

import pandas as pd
from sqlalchemy import Engine, text

from python.common.db_connection import get_db_engine
from python.common.utils import (
    clean_str,
    ensure_dir,
    get_logger,
)

# -------------------------
# Configuración
# -------------------------
DB_SCHEMA: str = "sgp"
OUTPUT_DIR: str = r"c:\Users\camil\Documents\TFM-SGP-CuentasMaestras\output"

logger = get_logger("etl_dim_bancos")
engine = get_db_engine()


def pick_excel() -> Optional[str]:
    """Abre un cuadro de diálogo para seleccionar el archivo Excel de entrada."""
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    path = filedialog.askopenfilename(
        title="Selecciona el Excel de DIM_BANCOS",
        filetypes=[("Excel", "*.xlsx *.xls"), ("Todos", "*.*")]
    )
    root.destroy()
    return path if path else None


def read_excel_df(path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Lee un archivo Excel y devuelve un DataFrame con todas las columnas como cadenas."""
    sn = 0 if sheet_name is None else sheet_name
    return pd.read_excel(path, dtype=str, engine="openpyxl", sheet_name=sn)


def build_id_banco(nit_banco: str, cod_ach: str) -> str:
    """Genera un ID_BANCO determinístico: NIT de 9 dígitos + ACH de 4 dígitos."""
    nit = "".join(ch for ch in str(nit_banco) if ch.isdigit()).zfill(9)
    ach = "".join(ch for ch in str(cod_ach) if ch.isdigit()).zfill(4)
    return f"{nit}{ach}"


def prepare_bancos(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Limpia y prepara los datos de bancos para la operación UPSERT."""
    df = df_raw.copy()
    df.columns = [c.strip() for c in df.columns]

    rename_map = {
        "COD ACH": "COD_ACH",
        "COD_ACH_BANCO": "COD_ACH",
        "NIT": "NIT_BANCO",
        "DV": "NIT_DV",
        "DIGITO_VERIFICACION": "NIT_DV",
        "RAZON": "RAZON_SOCIAL",
        "NOMBRE": "NOMBRE_BANCO",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    cols_target = [
        "COD_SUPERFINANCIERA", "COD_ACH", "RAZON_SOCIAL", "NOMBRE_BANCO",
        "NIT_BANCO", "NIT_DV", "TIPO_ENTIDAD", "ESTADO_CM"
    ]
    
    processed_df = pd.DataFrame()
    for col in cols_target:
        processed_df[col] = df[col].map(clean_str) if col in df.columns else None

    # Normalización
    processed_df["COD_ACH"] = processed_df["COD_ACH"].str.replace(r"\D", "", regex=True).str.zfill(4)
    processed_df["NIT_BANCO"] = processed_df["NIT_BANCO"].str.replace(r"\D", "", regex=True).str.zfill(9)
    processed_df["NIT_DV"] = processed_df["NIT_DV"].str[:1]

    # Generación de PK
    processed_df["ID_BANCO"] = [
        build_id_banco(n, a) 
        for n, a in zip(processed_df["NIT_BANCO"], processed_df["COD_ACH"])
    ]

    # Fechas Técnicas
    hoy = pd.Timestamp.today()
    fecha_corte = pd.Timestamp(year=hoy.year, month=hoy.month, day=1).normalize()
    processed_df["FECHA_ACTUALIZACION"] = fecha_corte
    processed_df["FECHA_CREACION"] = fecha_corte

    # Validación
    obligatory_cols = ["COD_ACH", "RAZON_SOCIAL", "NOMBRE_BANCO", "NIT_BANCO", "NIT_DV", "ESTADO_CM", "ID_BANCO"]
    for col in obligatory_cols:
        if processed_df[col].isna().any():
            ensure_dir(OUTPUT_DIR)
            processed_df.to_csv(f"{OUTPUT_DIR}/errores_dim_bancos.csv", index=False)
            raise ValueError(f"Campo obligatorio '{col}' tiene valores nulos. Ver errores_dim_bancos.csv")

    # Deduplicación
    processed_df = processed_df.drop_duplicates(subset=["COD_ACH"], keep="last").reset_index(drop=True)

    return processed_df


def upsert_dim_bancos(db_engine: Engine, df: pd.DataFrame):
    """Realiza la carga UPSERT de datos bancarios en DIM_BANCOS usando MERGE."""
    if df.empty:
        return

    stg_name = "STG_DIM_BANCOS"
    stg_full = f"dbo.{stg_name}"
    
    df.to_sql(stg_name, db_engine, schema="dbo", if_exists="replace", index=False)

    cols = list(df.columns)
    set_clause = ", ".join([f"t.{c} = s.{c}" for c in cols if c not in {"FECHA_CREACION", "COD_ACH"}])
    
    diff_condition = " OR ".join([
        f"(t.{c} <> s.{c} OR (t.{c} IS NULL AND s.{c} IS NOT NULL) OR (t.{c} IS NOT NULL AND s.{c} IS NULL))"
        for c in cols if c not in {"FECHA_CREACION", "COD_ACH", "FECHA_ACTUALIZACION"}
    ])

    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"s.{c}" for c in cols])

    merge_sql = f"""
    MERGE INTO {DB_SCHEMA}.DIM_BANCOS AS t
    USING {stg_full} AS s
      ON t.COD_ACH = s.COD_ACH
    WHEN MATCHED AND ({diff_condition}) THEN
      UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols}) VALUES ({insert_vals});
    """

    with db_engine.begin() as conn:
        conn.execute(text(merge_sql))
        conn.execute(text(f"DROP TABLE {stg_full};"))


def main():
    """Función de ejecución principal para el ETL de Bancos."""
    path = pick_excel()
    if not path:
        logger.warning("No se seleccionó ningún archivo.")
        return

    logger.info(f"Procesando DIM_BANCOS desde: {path}")
    df_raw = read_excel_df(path)
    df_prepared = prepare_bancos(df_raw)

    logger.info(f"Cargando {len(df_prepared)} registros...")
    upsert_dim_bancos(engine, df_prepared)
    logger.info("✅ Proceso DIM_BANCOS finalizado con éxito.")


if __name__ == "__main__":
    main()
