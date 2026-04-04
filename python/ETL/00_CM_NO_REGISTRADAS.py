"""
Script de saneamiento - Backfill de cuentas faltantes en DIM_CUENTAS_CM desde registros tipo 2.
Este script identifica cuentas en archivos planos que no están en el maestro de Excel.
"""

import sys
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


import logging
import tkinter as tk
from pathlib import Path
from tkinter import filedialog
from typing import Dict, List, Optional, Tuple

import pandas as pd
from openpyxl import Workbook

from python.common.utils import (
    clean_str,
    clean_upper,
    ensure_dir,
    get_logger,
    normalize_numeric_code,
)

# -------------------------
# Configuración
# -------------------------
INPUT_DIRS: List[str] = [
    r"C:\Users\camil\Ministerio de Hacienda\Capacitación Cuentas Maestras - RBancos\Prinicipales",
    r"C:\Users\camil\Ministerio de Hacienda\Capacitación Cuentas Maestras - RBancos\Resguardos Indigenas",
]
OUTPUT_DIR: str = r"c:\Users\camil\Documents\TFM-SGP-CuentasMaestras\output\backfill_cm"
TXT_ENCODING: str = "ISO-8859-1"
TXT_DELIMITER: str = ";"

logger = get_logger("backfill_cuentas_cm")


def pick_excel() -> Optional[str]:
    """Abre un cuadro de diálogo para seleccionar el Excel maestro de DIM_CUENTAS_CM."""
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    path = filedialog.askopenfilename(
        title="Selecciona el Excel local de DIM_CUENTAS_CM",
        filetypes=[("Excel", "*.xlsx *.xls"), ("Todos", "*.*")]
    )
    root.destroy()
    return path if path else None


def get_ach_from_filename(filename: str) -> Optional[str]:
    """Extrae el código ACH (últimos 4 dígitos) del nombre del archivo."""
    stem = Path(filename).stem
    return stem[-4:] if len(stem) >= 4 else None


def parse_txt_records(path_txt: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extrae registros tipo 1 y tipo 2 de un archivo plano."""
    rows_t1, rows_t2 = [], []
    ach = get_ach_from_filename(path_txt)
    base_name = Path(path_txt).stem

    try:
        with open(path_txt, "r", encoding=TXT_ENCODING, errors="replace") as f:
            for line in f:
                parts = line.strip().split(TXT_DELIMITER)
                if not parts:
                    continue
                tipo = clean_str(parts[0])
                if tipo == "1":
                    rows_t1.append([base_name, ach] + parts)
                elif tipo == "2":
                    rows_t2.append([base_name, ach] + parts)
    except Exception as e:
        logger.error(f"Error leyendo {path_txt}: {e}")

    df1 = pd.DataFrame(rows_t1)
    df2 = pd.DataFrame(rows_t2)
    return df1, df2


def run_backfill():
    """Ejecuta el proceso completo de identificación de cuentas no registradas."""
    ensure_dir(OUTPUT_DIR)
    excel_path = pick_excel()
    if not excel_path:
        logger.warning("No se seleccionó el archivo Excel maestro.")
        return

    # 1. Leer maestro actual
    logger.info("Leyendo Excel maestro...")
    df_master = pd.read_excel(excel_path, dtype=str)
    df_master.columns = [c.upper().strip() for c in df_master.columns]
    master_cuentas = set(df_master["NUMERO_CM"].dropna().unique())

    # 2. Procesar archivos planos
    all_t2 = []
    files = []
    for d in INPUT_DIRS:
        p = Path(d)
        if p.exists():
            files.extend(list(p.glob("*.txt")))

    logger.info(f"Procesando {len(files)} archivos planos...")
    for f in files:
        _, df2 = parse_txt_records(str(f))
        if not df2.empty:
            # Columnas mínimas esperadas en tipo 2 (ajustar según layout real)
            # Col 15 (index 14) suele ser el número de cuenta en el layout tradicional
            if df2.shape[1] > 14:
                all_t2.append(df2)

    if not all_t2:
        logger.warning("No se encontraron registros tipo 2.")
        return

    df_all_t2 = pd.concat(all_t2, ignore_index=True)
    # Asumimos layout: 0:FILENAME, 1:ACH, 2:TIPO_REG, ... 14:NUMERO_CM
    df_all_t2["NUM_CUENTA"] = df_all_t2.iloc[:, 14].map(clean_str)
    
    # 3. Identificar faltantes
    faltantes = df_all_t2[~df_all_t2["NUM_CUENTA"].isin(master_cuentas)].copy()
    faltantes = faltantes.drop_duplicates(subset=["NUM_CUENTA"])

    logger.info(f"Cuentas faltantes encontradas: {len(faltantes)}")
    
    if not faltantes.empty:
        out_path = Path(OUTPUT_DIR) / "cuentas_no_registradas.xlsx"
        faltantes.to_excel(out_path, index=False)
        logger.info(f"✅ Reporte generado en: {out_path}")


if __name__ == "__main__":
    run_backfill()
