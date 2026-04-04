"""
Script de utilidad - Extracción de cuentas CMP únicas desde registros tipo 2 en archivos planos.
Este script ayuda a validar la existencia de cuentas maestras pagadoras reportadas en el detalle.
"""

import sys
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pandas as pd

from python.common.utils import clean_str, ensure_dir, get_logger

# -------------------------
# Configuración
# -------------------------
INPUT_DIR: str = r"C:\Users\camil\Ministerio de Hacienda\Capacitación Cuentas Maestras - RBancos\Pagadoras"
OUTPUT_DIR: str = r"c:\Users\camil\Documents\TFM-SGP-CuentasMaestras\output"
FILE_PREFIX: str = "CMH145"

logger = get_logger("extraer_cmp_tipo2")


def extract_cmp_accounts():
    """Busca y extrae registros tipo 2 de los archivos planos de CMP."""
    ensure_dir(OUTPUT_DIR)
    
    files = sorted([
        f for f in Path(INPUT_DIR).iterdir() 
        if f.is_file() and f.suffix.lower() == ".txt" and f.name.upper().startswith(FILE_PREFIX)
    ])
    
    if not files:
        logger.warning(f"No se encontraron archivos con el prefijo {FILE_PREFIX} en {INPUT_DIR}")
        return

    logger.info(f"Procesando {len(files)} archivos para extracción de Tipo 2...")
    
    seen_accounts: Set[Tuple[str, str]] = set()
    results: List[Dict[str, str]] = []

    for fp in files:
        try:
            with open(fp, "r", encoding="latin-1", errors="replace") as f:
                for line in f:
                    parts = line.strip().split(";")
                    if not parts:
                        continue
                    
                    tipo = clean_str(parts[0])
                    # Registro Tipo 2 en CMP según Res 0660:
                    # Index 4: Tipo CMP, Index 5: Número CMP
                    if tipo == "2" and len(parts) >= 6:
                        tipo_cmp = clean_str(parts[4])
                        num_cmp = clean_str(parts[5])
                        
                        if tipo_cmp and num_cmp:
                            key = (tipo_cmp, num_cmp)
                            if key not in seen_accounts:
                                seen_accounts.add(key)
                                results.append({
                                    "ARCHIVO_ORIGEN": fp.name,
                                    "TIPO_CMP": tipo_cmp,
                                    "NUMERO_CMP": num_cmp,
                                    "LONGITUD": str(len(num_cmp))
                                })
        except Exception as e:
            logger.error(f"Error procesando {fp.name}: {e}")

    if results:
        df = pd.DataFrame(results).sort_values(["TIPO_CMP", "NUMERO_CMP"])
        out_path = Path(OUTPUT_DIR) / "validacion_cuentas_cmp_tipo2.xlsx"
        df.to_excel(out_path, index=False)
        logger.info(f"✅ Extracción exitosa: {len(df)} cuentas únicas encontradas.")
    else:
        logger.warning("No se detectaron registros Tipo 2 válidos.")


if __name__ == "__main__":
    extract_cmp_accounts()
