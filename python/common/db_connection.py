"""
Utilidades de conexión a la base de datos para el proyecto TFM-SGP.
Este módulo provee e inicializa el motor de SQLAlchemy (Engine).
"""

import os
import urllib.parse
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Cadena de conexión predeterminada (usar variables de entorno para producción)
DEFAULT_CONN_STR = (
    "mssql+pyodbc:///?odbc_connect="
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=TFM_SGP;"
    "UID=sa;"
    "PWD=TFM_SQL_2026!;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)


def get_db_engine(connection_string: Optional[str] = None) -> Engine:
    """Crea y devuelve un motor (Engine) de SQLAlchemy para SQL Server.

    Args:
        connection_string: Cadena de conexión opcional. 
            Si no se provee, se usa la variable de entorno DB_CONN_STR o el valor por defecto.

    Returns:
        Engine: Instancia de motor SQLAlchemy configurada.
    """
    conn_str = connection_string or os.getenv("DB_CONN_STR", DEFAULT_CONN_STR)
    
    # Manejar codificación de caracteres especiales si es necesario
    if "odbc_connect=" in conn_str and "///" in conn_str:
        # Ya está formateado correctamente para SQLAlchemy
        pass

    return create_engine(
        conn_str, 
        fast_executemany=True, 
        future=True,
        pool_pre_ping=True
    )
