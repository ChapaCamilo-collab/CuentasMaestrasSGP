"""
Funciones de utilidad comunes para el procesamiento ETL en el proyecto TFM-SGP.
Sigue las convenciones de docstrings de Google y PEP 8 en español.
"""

import hashlib
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Callable, List, Optional, Set

import pandas as pd


def get_logger(name: str) -> logging.Logger:
    """Configura y devuelve un logger estándar.

    Args:
        name: El nombre del logger (usualmente __name__).

    Returns:
        logging.Logger: Una instancia de logger configurada.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def clean_str(x: Any) -> Optional[str]:
    """Limpia una cadena eliminando espacios, tabulaciones y saltos de línea.

    Args:
        x: El valor a limpiar.

    Returns:
        Optional[str]: Cadena limpia o None si la entrada es vacía/null/NaN.
    """
    if x is None or pd.isna(x):
        return None
    s = str(x).replace("\u00A0", " ").replace("\t", " ").replace("\r", " ").replace("\n", " ").strip()
    if s == "" or s.lower() in {"nan", "none", "null"}:
        return None
    return s


def normalize_spaces_text(x: Any) -> Optional[str]:
    """Colapsa múltiples espacios en un solo espacio."""
    s = clean_str(x)
    return " ".join(s.split()) if s else None


def normalize_razon_social(x: Any) -> Optional[str]:
    """Normaliza razones sociales a mayúsculas y limita la longitud a 240 caracteres."""
    s = normalize_spaces_text(x)
    return s.upper()[:240] if s else None


def clean_upper(x: Any) -> Optional[str]:
    """Limpia y convierte una cadena a mayúsculas."""
    s = clean_str(x)
    return s.upper() if s else None


def normalize_numeric_code(series: pd.Series, width: Optional[int] = None) -> pd.Series:
    """Normaliza códigos numéricos (IDs, códigos bancarios) eliminando caracteres no numéricos.

    Args:
        series: Serie de Pandas a normalizar.
        width: Longitud opcional para rellenar con ceros a la izquierda.

    Returns:
        pd.Series: Serie de cadenas normalizada.
    """
    s = series.astype("string").str.strip()
    s = s.mask(s.isin(["", "nan", "None", "null"]))
    s = s.str.replace(r"\D", "", regex=True)
    s = s.mask(s == "")
    if width is not None:
        s = s.where(s.isna(), s.str.zfill(width))
    return s


def normalize_amount(series: pd.Series, chunk_size: int = 500000) -> pd.Series:
    """Normaliza valores monetarios por bloques para ahorrar memoria.

    Args:
        series: Serie de Pandas con cadenas de montos originales.
        chunk_size: Número de registros a procesar simultáneamente.

    Returns:
        pd.Series: Serie numérica (float64).
    """
    resultados: List[pd.Series] = []
    n = len(series)
    for start in range(0, n, chunk_size):
        chunk = series.iloc[start : start + chunk_size].astype("string").str.strip()
        chunk = chunk.mask(chunk.isin(["", "nan", "None", "null"]))
        # Reemplazar separadores decimales comunes en archivos bancarios colombianos
        chunk = chunk.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
        resultados.append(pd.to_numeric(chunk, errors="coerce"))
    return pd.concat(resultados, ignore_index=False)


def normalize_date(series: pd.Series) -> pd.Series:
    """Intenta normalizar varios formatos de fecha encontrados en archivos bancarios.

    Soportados: YYYYMMDD, DD/MM/YYYY, YYYY-MM-DD.

    Args:
        series: Serie de Pandas con cadenas de fechas.

    Returns:
        pd.Series: Serie de objetos datetime.date.
    """
    s = series.astype("string").str.strip()
    s = s.mask(s.isin(["", "nan", "None", "null"]))

    out = pd.to_datetime(s, format="%Y%m%d", errors="coerce")
    out = out.fillna(pd.to_datetime(s, format="%d/%m/%Y", errors="coerce"))
    out = out.fillna(pd.to_datetime(s, format="%Y-%m-%d", errors="coerce"))
    out = out.fillna(pd.to_datetime(s, errors="coerce"))

    return out.dt.date


def ensure_dir(path: str) -> None:
    """Asegura que un directorio exista en el sistema de archivos."""
    Path(path).mkdir(parents=True, exist_ok=True)


def build_hash_id(parts: List[str], length: int = 20) -> str:
    """Genera un ID hash SHA1 consistente a partir de una lista de partes.

    Args:
        parts: Lista de cadenas a unir para el hash.
        length: Longitud del hash resultante (predeterminado 20).

    Returns:
        str: Cadena hexadecimal del hash resultante.
    """
    base = "|".join(str(p) for p in parts)
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:length]


# -------------------------
# Robustez SQL Server
# -------------------------
def execute_sql_with_retry(
    engine: Any,
    sql: str,
    context_label: str,
    max_retries: int = 5,
    base_wait: float = 2.0,
    logger: Optional[logging.Logger] = None
) -> None:
    """Ejecuta un comando SQL con reintentos para manejar interbloqueos (deadlocks 1205)."""
    from sqlalchemy import text
    for attempt in range(max_retries):
        try:
            with engine.begin() as conn:
                conn.execute(text(sql))
            return
        except Exception as e:
            if "1205" in str(e) or "deadlock" in str(e).lower():
                wait_time = base_wait * (attempt + 1)
                if logger:
                    logger.warning(f"Deadlock (1205) en {context_label}. Reintento {attempt + 1}/{max_retries} en {wait_time:.0f}s...")
                time.sleep(wait_time)
            else:
                raise
    raise Exception(f"Falló {context_label} después de {max_retries} reintentos debido a deadlocks.")


def to_sql_with_retry(
    df: pd.DataFrame,
    engine: Any,
    name: str,
    schema: str,
    if_exists: str = "replace",
    max_retries: int = 5,
    base_wait: float = 2.0,
    logger: Optional[logging.Logger] = None
) -> None:
    """Envuelve df.to_sql() con reintentos para deadlocks."""
    for attempt in range(max_retries):
        try:
            df.to_sql(name=name, con=engine, schema=schema, if_exists=if_exists, index=False)
            return
        except Exception as e:
            if "1205" in str(e) or "deadlock" in str(e).lower():
                wait_time = base_wait * (attempt + 1)
                if logger:
                    logger.warning(f"Deadlock en to_sql '{schema}.{name}'. Reintento {attempt + 1}/{max_retries} en {wait_time:.0f}s...")
                time.sleep(wait_time)
            else:
                raise
    raise Exception(f"Falló la carga de staging '{schema}.{name}' después de {max_retries} reintentos.")


def drop_table_safe(
    engine: Any,
    table_full: str,
    max_retries: int = 3,
    base_wait: float = 1.0,
    logger: Optional[logging.Logger] = None
) -> None:
    """Elimina una tabla de forma segura con reintentos para deadlocks."""
    from sqlalchemy import text
    for attempt in range(max_retries):
        try:
            with engine.begin() as conn:
                conn.execute(text(f"IF OBJECT_ID('{table_full}', 'U') IS NOT NULL DROP TABLE {table_full};"))
            return
        except Exception as e:
            if "1205" in str(e) or "deadlock" in str(e).lower():
                wait_time = base_wait * (attempt + 1)
                if logger:
                    logger.warning(f"Deadlock en DROP TABLE {table_full}. Reintento {attempt + 1}/{max_retries} en {wait_time:.0f}s...")
                time.sleep(wait_time)
            else:
                raise
    if logger:
        logger.error(f"Falló la limpieza de staging {table_full} después de {max_retries} reintentos.")
