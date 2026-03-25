"""
Script de utilidad para verificar el esquema de la base de datos SQL Server (TFM_SGP).
Lista tablas y columnas del esquema 'sgp' para depuración y validación del ETL.
"""

import logging
import os
from typing import List, Optional

import pandas as pd
from sqlalchemy import Engine, text

from python.common.db_connection import get_db_engine

# Configuración básica del logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def list_tables_and_columns(engine: Engine, schema: str = "sgp") -> None:
    """Consulta los metadatos de SQL Server y muestra las tablas y columnas del esquema.

    Args:
        engine: El motor de SQLAlchemy conectado.
        schema: El nombre del esquema a inspeccionar. Predeterminado 'sgp'.
    """
    logger.info(f"Inspeccionando esquema: {schema}")
    
    query = f"""
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{schema}'
    ORDER BY TABLE_NAME, ORDINAL_POSITION;
    """
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        
        if df.empty:
            logger.warning(f"No se encontraron tablas en el esquema '{schema}'.")
            return

        for table in df["TABLE_NAME"].unique():
            cols = df[df["TABLE_NAME"] == table]
            logger.info(f"Tabla: {table}")
            for _, col in cols.iterrows():
                nullable = "NULL" if col["IS_NULLABLE"] == "YES" else "NOT NULL"
                logger.info(f"  - {col['COLUMN_NAME']}: {col['DATA_TYPE']} ({nullable})")
                
    except Exception as e:
        logger.error(f"Error al listar tablas: {e}")


def main():
    """Punto de entrada para la inspección del esquema."""
    try:
        engine = get_db_engine()
        list_tables_and_columns(engine)
        logger.info("Verificación de esquema completada con éxito.")
    except Exception as e:
        logger.error(f"Falla crítica en check_schema: {e}")


if __name__ == "__main__":
    main()
