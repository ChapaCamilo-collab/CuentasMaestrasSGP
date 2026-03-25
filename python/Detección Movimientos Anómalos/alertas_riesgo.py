"""
Script de generación de alertas de riesgo basadas en reglas de negocio (A1-A5).
Identifica comportamientos anómalos en los movimientos de recursos del SGP
y contrasta los resultados contra movimientos de riesgo previamente identificados.
"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns
from sqlalchemy import Engine, text

# Añadir el directorio raíz al path para asegurar importaciones de módulos comunes
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from python.common.db_connection import get_db_engine
from python.common.utils import ensure_dir, get_logger

logger = get_logger("alertas_riesgo")
engine = get_db_engine()

# Configuración de estilo para gráficos (coherente con concentracion_mercado.py)
sns.set_theme(style="whitegrid")
plt.rcParams.update({
    "figure.dpi": 150,
    "savefig.dpi": 300,
    "font.size": 11,
    "axes.titlesize": 14,
    "axes.labelsize": 12,
})

# --- Parámetros de Riesgo (Ajustados para mayor precisión) ---
PARAMETROS = {
    "UMBRAL_TRASLADO_TOTAL": 0.95,
    "UMBRAL_SIGNIFICATIVO": 0.80,              # A3: Antes 0.50
    "UMBRAL_SIGNIFICATIVO_MISMO_TITULAR": 0.80, # A4: Antes 0.30
    "N_MIN_TRANSACCIONES": 20,                # A5: Antes 5
    "VENTANA_DIAS_RAPIDA": 3,
    "UMBRAL_VALOR_MINIMO": 5_000_000          # Solo flaggear movimientos relevantes (>5M)
}

# Tipos de movimiento que NO representan operaciones transaccionales reales
TIPOS_EXCLUIDOS = {"500", "600"}

# Ruta al archivo de contraste con riesgos previamente identificados
RUTA_CONTRASTE = os.path.join(
    os.path.dirname(__file__), "Constraste",
    "consolidado_movimientos_suspension_masiva_RIESGOS.txt"
)


def enriquecer_datos(df: pd.DataFrame, db_engine: Engine) -> pd.DataFrame:
    """Enriquece los movimientos con información de titulares y clasificación de tipo.

    Agrega:
        - NIT_TITULAR: identificación del titular de la cuenta maestra.
        - CATEGORIA: clasificación del tipo de movimiento (INGRESO/EGRESO).
        - BENEFICIARIO_CLAVE: clave compuesta para identificar beneficiarios únicos.
        - ES_MISMO_TITULAR: indicador booleano de auto-traslado.
    """
    # Obtener titulares de cuentas maestras
    query = "SELECT NUMERO_CM, NIT_TITULAR FROM sgp.DIM_CUENTAS_CM"
    df_cuentas = pd.read_sql(text(query), db_engine)
    df = df.merge(df_cuentas, on="NUMERO_CM", how="left")

    # Clasificar INGRESO / EGRESO usando la tabla de catálogo
    query_tipos = "SELECT TIPO_MOVIMIENTO, CATEGORIA FROM sgp.DIM_TIPO_MOVIMIENTO"
    df_tipos = pd.read_sql(text(query_tipos), db_engine)
    tipo_a_cat = dict(zip(df_tipos["TIPO_MOVIMIENTO"].astype(str), df_tipos["CATEGORIA"].str.upper()))
    df["CATEGORIA"] = df["TIPO_MOVIMIENTO"].astype(str).map(tipo_a_cat).fillna("OTRO")

    # Clave compuesta de beneficiario
    df["BENEFICIARIO_CLAVE"] = (
        df["TIPO_ID_BENEFICIARIO"].fillna("XX").astype(str) + "_" +
        df["ID_BENEFICIARIO"].fillna("000").astype(str) + "_" +
        df.get("CUENTA_BENEFICIARIO", pd.Series("000", index=df.index)).fillna("000").astype(str)
    )

    # Evaluar si el beneficiario es el mismo titular (normalizar sin ceros a la izquierda)
    nit_norm = df["NIT_TITULAR"].astype(str).str.lstrip("0")
    id_benef_norm = df["ID_BENEFICIARIO"].astype(str).str.lstrip("0")
    df["ES_MISMO_TITULAR"] = nit_norm == id_benef_norm

    return df


# =====================================================================
#  GRÁFICOS PRE-CONTRASTE (Resultados de detección por reglas)
# =====================================================================

def generar_graficos_deteccion(df_alertas: pd.DataFrame, output_dir: str):
    """Genera gráficos descriptivos de las alertas detectadas por reglas de negocio.

    Produce 4 gráficos:
        1. Distribución de alertas por tipo de regla (A1-A5).
        2. Evolución temporal de alertas por mes.
        3. Top 15 cuentas con más alertas.
        4. Heatmap de reglas × mes.
    """
    logger.info("Generando gráficos de resultados de detección (pre-contraste)...")

    # --- 1. Distribución de alertas por regla ---
    fig, ax = plt.subplots(figsize=(9, 5))
    orden_reglas = ["A1", "A2", "A3", "A4", "A5"]
    conteo = df_alertas["REGLA"].value_counts().reindex(orden_reglas, fill_value=0)
    colores = sns.color_palette("YlOrRd", n_colors=len(orden_reglas))
    bars = ax.bar(conteo.index, conteo.values, color=colores, edgecolor="white", linewidth=0.8)
    for bar, v in zip(bars, conteo.values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                str(v), ha="center", va="bottom", fontweight="bold", fontsize=11)
    ax.set_title("Distribución de Alertas por Regla de Negocio")
    ax.set_xlabel("Regla")
    ax.set_ylabel("Número de Alertas")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "reglas_01_distribucion_alertas.png"))
    plt.close()

    # --- 2. Evolución temporal de alertas ---
    fig, ax = plt.subplots(figsize=(12, 5))
    alertas_mes = df_alertas.groupby("MES").size().sort_index()
    ax.plot(range(len(alertas_mes)), alertas_mes.values,
            marker="o", color="#c0392b", linewidth=2, markersize=6)
    ax.fill_between(range(len(alertas_mes)), alertas_mes.values, alpha=0.15, color="#c0392b")
    ax.set_xticks(range(len(alertas_mes)))
    ax.set_xticklabels(alertas_mes.index, rotation=45, ha="right", fontsize=8)
    ax.set_title("Evolución Temporal de Alertas de Riesgo")
    ax.set_xlabel("Período")
    ax.set_ylabel("Número de Alertas")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "reglas_02_evolucion_temporal.png"))
    plt.close()

    # --- 3. Top 15 cuentas con más alertas ---
    fig, ax = plt.subplots(figsize=(10, 7))
    top_cuentas = df_alertas["CUENTA"].value_counts().head(15)
    # Enmascarar parcialmente las cuentas para privacidad TFM
    labels = [f"...{c[-4:]}" if len(str(c)) > 4 else str(c) for c in top_cuentas.index]
    sns.barplot(x=top_cuentas.values, y=labels, palette="Reds_r", ax=ax)
    ax.set_title("Top 15 Cuentas con Mayor Número de Alertas")
    ax.set_xlabel("Número de Alertas")
    ax.set_ylabel("Cuenta Maestra (enmascarada)")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "reglas_03_top_cuentas.png"))
    plt.close()

    # --- 4. Heatmap de reglas × período ---
    fig, ax = plt.subplots(figsize=(14, 5))
    pivot = df_alertas.pivot_table(
        index="REGLA", columns="MES", aggfunc="size", fill_value=0
    )
    # Limitar columnas si son demasiadas
    if pivot.shape[1] > 24:
        pivot = pivot.iloc[:, -24:]  # Últimos 24 meses
    sns.heatmap(pivot, cmap="YlOrRd", annot=True, fmt="d", linewidths=0.5,
                cbar_kws={"label": "N° Alertas"}, ax=ax)
    ax.set_title("Mapa de Calor: Alertas por Regla y Período")
    ax.set_xlabel("Período")
    ax.set_ylabel("Regla")
    plt.xticks(rotation=45, ha="right", fontsize=7)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "reglas_04_heatmap_reglas_periodo.png"))
    plt.close()

    logger.info(f"📊 Gráficos de detección exportados en: {output_dir}")


# =====================================================================
#  GRÁFICOS POST-CONTRASTE (Evaluación de eficacia)
# =====================================================================

def generar_graficos_contraste(
    verdaderos_positivos: int,
    falsos_positivos: int,
    falsos_negativos: int,
    precision: float,
    recall: float,
    f1: float,
    nombre_modelo: str,
    output_dir: str
):
    """Genera gráficos de evaluación del contraste del modelo de reglas.

    Produce 3 gráficos:
        1. Matriz de confusión simplificada.
        2. Barras de métricas de eficacia (Precision, Recall, F1).
        3. Diagrama de Venn conceptual (detecciones vs riesgos reales).
    """
    logger.info("Generando gráficos de evaluación post-contraste...")
    prefijo = nombre_modelo.lower().replace(" ", "_").replace("-", "")

    # --- 1. Matriz de confusión simplificada ---
    fig, ax = plt.subplots(figsize=(7, 5))
    matriz = np.array([
        [verdaderos_positivos, falsos_negativos],
        [falsos_positivos, 0]
    ])
    labels = np.array([
        [f"VP\n{verdaderos_positivos:,}", f"FN\n{falsos_negativos:,}"],
        [f"FP\n{falsos_positivos:,}", "VN\n(N/A)"]
    ])
    sns.heatmap(
        matriz, annot=labels, fmt="", cmap="RdYlGn_r",
        xticklabels=["Predijo Riesgo", "Predijo Normal"],
        yticklabels=["Real Riesgo", "Real Normal"],
        linewidths=2, linecolor="white",
        cbar_kws={"label": "Cantidad"},
        ax=ax
    )
    ax.set_title(f"Matriz de Confusión — {nombre_modelo}", fontsize=13)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"contraste_{prefijo}_01_matriz_confusion.png"))
    plt.close()

    # --- 2. Barras de métricas ---
    fig, ax = plt.subplots(figsize=(8, 5))
    metricas = ["Precisión\n(Precision)", "Sensibilidad\n(Recall)", "F1-Score"]
    valores = [precision, recall, f1]
    colores_met = ["#3498db", "#e67e22", "#2ecc71"]
    bars = ax.bar(metricas, valores, color=colores_met, edgecolor="white", width=0.55)
    for bar, v in zip(bars, valores):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                f"{v:.1%}", ha="center", va="bottom", fontweight="bold", fontsize=13)
    ax.set_ylim(0, 1.15)
    ax.set_ylabel("Valor")
    ax.set_title(f"Métricas de Eficacia — {nombre_modelo}", fontsize=13)
    ax.axhline(y=0.5, color="gray", linestyle="--", alpha=0.5, label="Umbral 50%")
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"contraste_{prefijo}_02_metricas.png"))
    plt.close()

    # --- 3. Diagrama de composición (stacked bar) ---
    fig, ax = plt.subplots(figsize=(8, 4))
    categorias = ["Verdaderos Positivos", "Falsos Positivos", "Falsos Negativos"]
    valores_comp = [verdaderos_positivos, falsos_positivos, falsos_negativos]
    colores_comp = ["#27ae60", "#e74c3c", "#f39c12"]
    bars = ax.barh(categorias, valores_comp, color=colores_comp, edgecolor="white", height=0.55)
    for bar, v in zip(bars, valores_comp):
        ax.text(bar.get_width() + max(valores_comp) * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{v:,}", ha="left", va="center", fontweight="bold", fontsize=11)
    ax.set_title(f"Composición de Resultados — {nombre_modelo}", fontsize=13)
    ax.set_xlabel("Número de Movimientos")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"contraste_{prefijo}_03_composicion.png"))
    plt.close()

    logger.info(f"📊 Gráficos de contraste exportados en: {output_dir}")


# =====================================================================
#  LÓGICA PRINCIPAL
# =====================================================================

def procesar_alertas(
    años: Optional[list] = None,
    fecha_inicio: Optional[str] = None,
    fecha_fin: Optional[str] = None
) -> pd.DataFrame:
    """Analiza los movimientos y genera alertas basadas en los umbrales definidos.

    Args:
        años: Lista de años a analizar (ej: [2018], [2018, 2019]). None = todos.
        fecha_inicio: Fecha de inicio del rango (formato 'YYYY-MM-DD').
        fecha_fin: Fecha de fin del rango (formato 'YYYY-MM-DD').

    Returns:
        pd.DataFrame: DataFrame con los movimientos de egreso de las cuentas/períodos
        que generaron alertas. Vacío si no hay alertas.
    """
    query = "SELECT * FROM sgp.FACT_MOVIMIENTOS_CM"
    condiciones = []

    if años:
        condiciones.append(f"YEAR(FECHA_MOVIMIENTO) IN ({','.join(map(str, años))})")
    if fecha_inicio:
        condiciones.append(f"FECHA_MOVIMIENTO >= '{fecha_inicio}'")
    if fecha_fin:
        condiciones.append(f"FECHA_MOVIMIENTO <= '{fecha_fin}'")

    if condiciones:
        query += " WHERE " + " AND ".join(condiciones)

    filtro_desc = f" (Filtro: {condiciones})" if condiciones else " (todos los períodos)"
    logger.info(f"Consultando movimientos...{filtro_desc}")
    df = pd.read_sql(text(query), engine)

    if df.empty:
        logger.warning("No hay movimientos registrados.")
        return pd.DataFrame()

    # Filtrar tipos que no representan transacciones reales (Saldo Inicial/Final)
    n_antes = len(df)
    df = df[~df["TIPO_MOVIMIENTO"].isin(TIPOS_EXCLUIDOS)].copy()
    n_excluidos = n_antes - len(df)
    if n_excluidos > 0:
        logger.info(f"Se excluyeron {n_excluidos} registros de saldo inicial/final (tipos 500/600).")

    # Filtrar movimientos de valor irrelevante para reducir Falsos Positivos
    n_antes_v = len(df)
    df = df[df["VALOR"] >= PARAMETROS["UMBRAL_VALOR_MINIMO"]].copy()
    n_pequeños = n_antes_v - len(df)
    if n_pequeños > 0:
        logger.info(f"Se filtraron {n_pequeños} movimientos menores a ${PARAMETROS['UMBRAL_VALOR_MINIMO']:,} COP.")

    df["FECHA_MOVIMIENTO"] = pd.to_datetime(df["FECHA_MOVIMIENTO"])
    df = enriquecer_datos(df, engine)
    df["PERIODO"] = df["FECHA_MOVIMIENTO"].dt.to_period("M").astype(str)

    alertas = []

    # Agrupar por Cuenta y Mes
    for (cuenta, periodo), df_mes in df.groupby(["NUMERO_CM", "PERIODO"]):
        ingresos = df_mes[df_mes["CATEGORIA"] == "INGRESO"]["VALOR"].sum()
        egresos_df = df_mes[df_mes["CATEGORIA"] == "EGRESO"]
        egresos = egresos_df["VALOR"].sum()

        if ingresos <= 0:
            continue

        ratio_egreso = egresos / ingresos
        egresos_titular = df_mes[(df_mes["CATEGORIA"] == "EGRESO") & df_mes["ES_MISMO_TITULAR"]]["VALOR"].sum()
        ratio_titular = egresos_titular / ingresos

        # --- Regla A1: Traslado total al mismo titular (≥95%) ---
        if ratio_titular >= PARAMETROS["UMBRAL_TRASLADO_TOTAL"]:
            alertas.append({
                "REGLA": "A1", "CUENTA": cuenta, "MES": periodo,
                "DETALLE": f"Traslado total mismo titular: {ratio_titular:.2%}"
            })
        # --- Regla A4: Traslado significativo al mismo titular (≥30% y <95%) ---
        elif ratio_titular >= PARAMETROS["UMBRAL_SIGNIFICATIVO_MISMO_TITULAR"]:
            alertas.append({
                "REGLA": "A4", "CUENTA": cuenta, "MES": periodo,
                "DETALLE": f"Traslado significativo mismo titular: {ratio_titular:.2%}"
            })

        # --- Regla A2: Salida casi total de recursos (≥95%) ---
        if ratio_egreso >= PARAMETROS["UMBRAL_TRASLADO_TOTAL"]:
            alertas.append({
                "REGLA": "A2", "CUENTA": cuenta, "MES": periodo,
                "DETALLE": f"Salida casi total de recursos: {ratio_egreso:.2%}"
            })

        # --- Reglas A3, A5: Concentración y fraccionamiento por beneficiario ---
        for benef, g in egresos_df.groupby("BENEFICIARIO_CLAVE"):
            v_total = g["VALOR"].sum()
            n_tx = len(g)
            ratio_b = v_total / ingresos

            if ratio_b >= PARAMETROS["UMBRAL_SIGNIFICATIVO"]:
                alertas.append({
                    "REGLA": "A3", "CUENTA": cuenta, "MES": periodo,
                    "DETALLE": f"Concentración beneficiario {benef}: {ratio_b:.2%}"
                })
                if n_tx >= PARAMETROS["N_MIN_TRANSACCIONES"]:
                    alertas.append({
                        "REGLA": "A5", "CUENTA": cuenta, "MES": periodo,
                        "DETALLE": f"Fraccionamiento detectado ({n_tx} tx) al beneficiario {benef}"
                    })

    # === Exportar resultados ===
    out_dir = os.path.join(os.path.dirname(__file__), "resultados")
    ensure_dir(out_dir)

    if alertas:
        df_alertas = pd.DataFrame(alertas)
        f_path = os.path.join(out_dir, "alertas_riesgo_reglas.xlsx")
        df_alertas.to_excel(f_path, index=False)
        logger.info(f"✅ Se generaron {len(df_alertas)} alertas en {f_path}")

        # Generar gráficos de detección (pre-contraste)
        try:
            generar_graficos_deteccion(df_alertas, out_dir)
        except Exception as e:
            logger.error(f"Error generando gráficos de detección: {e}")

        # Obtener los movimientos individuales que están detrás de cada alerta
        cuentas_alerta = set(df_alertas["CUENTA"].unique())
        periodos_alerta = set(df_alertas["MES"].unique())
        df_mov_riesgo = df[
            (df["NUMERO_CM"].isin(cuentas_alerta)) &
            (df["PERIODO"].isin(periodos_alerta)) &
            (df["CATEGORIA"] == "EGRESO")
        ].copy()

        return df_mov_riesgo
    else:
        logger.info("No se detectaron riesgos según las reglas A1-A5.")
        return pd.DataFrame()


def cargar_contraste() -> pd.DataFrame:
    """Carga el archivo de riesgos previamente identificados para contraste.

    Returns:
        pd.DataFrame: Movimientos ya marcados como riesgo, con columnas normalizadas.
    """
    if not os.path.exists(RUTA_CONTRASTE):
        logger.warning(f"No se encontró el archivo de contraste en: {RUTA_CONTRASTE}")
        return pd.DataFrame()

    df = pd.read_csv(RUTA_CONTRASTE, sep=";", encoding="latin-1", dtype=str)
    logger.info(f"Archivo de contraste cargado: {len(df)} registros.")

    # Normalizar columnas clave para el join
    df["NUMERO_CM"] = df["NUMERO_CM"].astype(str).str.strip()
    df["FECHA_MOVIMIENTO"] = pd.to_datetime(df["FECHA_MOVIMIENTO"], errors="coerce")
    df["TIPO_MOVIMIENTO"] = df["TIPO_MOVIMIENTO"].astype(str).str.strip()
    df["VALOR"] = pd.to_numeric(df["VALOR"], errors="coerce")
    df["ID_BENEFICIARIO"] = df["ID_BENEFICIARIO"].astype(str).str.strip()

    return df


def evaluar_contraste(df_detectados: pd.DataFrame, df_reales: pd.DataFrame, nombre_modelo: str):
    """Evalúa la eficacia del modelo comparando detecciones contra riesgos reales.

    Calcula métricas de Precision, Recall y F1-Score a nivel de movimiento,
    genera gráficos de evaluación y exporta resumen a Excel.

    Args:
        df_detectados: Movimientos marcados como riesgo por el modelo.
        df_reales: Movimientos de riesgo previamente identificados (ground truth).
        nombre_modelo: Nombre descriptivo del modelo para los reportes.
    """
    if df_detectados.empty or df_reales.empty:
        logger.warning(f"[{nombre_modelo}] No es posible evaluar: datos insuficientes.")
        return

    columnas_clave = ["NUMERO_CM", "FECHA_MOVIMIENTO", "TIPO_MOVIMIENTO", "VALOR", "ID_BENEFICIARIO"]

    for col in columnas_clave:
        if col not in df_detectados.columns or col not in df_reales.columns:
            logger.error(f"[{nombre_modelo}] Falta la columna '{col}' para el contraste.")
            return

    # Normalizar
    df_det = df_detectados.copy()
    df_det["FECHA_MOVIMIENTO"] = pd.to_datetime(df_det["FECHA_MOVIMIENTO"])
    df_det["VALOR"] = pd.to_numeric(df_det["VALOR"], errors="coerce")
    df_det["NUMERO_CM"] = df_det["NUMERO_CM"].astype(str).str.strip()
    df_det["TIPO_MOVIMIENTO"] = df_det["TIPO_MOVIMIENTO"].astype(str).str.strip()
    df_det["ID_BENEFICIARIO"] = df_det["ID_BENEFICIARIO"].astype(str).str.strip()

    df_real = df_reales.copy()

    # Crear clave compuesta vectorizada (Pandas Best Practice)
    fecha_det = df_det["FECHA_MOVIMIENTO"].dt.strftime("%Y-%m-%d").fillna("")
    claves_detectadas = set((df_det["NUMERO_CM"].astype(str) + "|" + fecha_det + "|" + df_det["TIPO_MOVIMIENTO"].astype(str) + "|" + df_det["VALOR"].astype(str) + "|" + df_det["ID_BENEFICIARIO"].astype(str)))

    fecha_real = df_real["FECHA_MOVIMIENTO"].dt.strftime("%Y-%m-%d").fillna("")
    claves_reales = set((df_real["NUMERO_CM"].astype(str) + "|" + fecha_real + "|" + df_real["TIPO_MOVIMIENTO"].astype(str) + "|" + df_real["VALOR"].astype(str) + "|" + df_real["ID_BENEFICIARIO"].astype(str)))

    verdaderos_positivos = len(claves_detectadas & claves_reales)
    falsos_positivos = len(claves_detectadas - claves_reales)
    falsos_negativos = len(claves_reales - claves_detectadas)

    precision = verdaderos_positivos / (verdaderos_positivos + falsos_positivos) if (verdaderos_positivos + falsos_positivos) > 0 else 0
    recall = verdaderos_positivos / (verdaderos_positivos + falsos_negativos) if (verdaderos_positivos + falsos_negativos) > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    logger.info("=" * 70)
    logger.info(f"📊 CONTRASTE DE EFICACIA — {nombre_modelo}")
    logger.info("=" * 70)
    logger.info(f"  Movimientos detectados como riesgo:    {len(claves_detectadas):>8,}")
    logger.info(f"  Movimientos de riesgo real (contraste): {len(claves_reales):>8,}")
    logger.info(f"  Verdaderos Positivos (TP):              {verdaderos_positivos:>8,}")
    logger.info(f"  Falsos Positivos (FP):                  {falsos_positivos:>8,}")
    logger.info(f"  Falsos Negativos (FN):                  {falsos_negativos:>8,}")
    logger.info(f"  ─────────────────────────────────────")
    logger.info(f"  Precisión (Precision):                  {precision:>8.2%}")
    logger.info(f"  Sensibilidad (Recall):                  {recall:>8.2%}")
    logger.info(f"  F1-Score:                               {f1:>8.2%}")
    logger.info("=" * 70)

    # Exportar resumen numérico
    out_dir = os.path.join(os.path.dirname(__file__), "resultados")
    ensure_dir(out_dir)

    resumen = {
        "Modelo": [nombre_modelo],
        "Detectados": [len(claves_detectadas)],
        "Riesgos_Reales": [len(claves_reales)],
        "VP (True Pos)": [verdaderos_positivos],
        "FP (False Pos)": [falsos_positivos],
        "FN (False Neg)": [falsos_negativos],
        "Precision": [round(precision, 4)],
        "Recall": [round(recall, 4)],
        "F1_Score": [round(f1, 4)],
    }
    df_resumen = pd.DataFrame(resumen)
    f_path = os.path.join(out_dir, f"contraste_{nombre_modelo.lower().replace(' ', '_')}.xlsx")
    df_resumen.to_excel(f_path, index=False)
    logger.info(f"📄 Resumen exportado a: {f_path}")

    # Exportar tabla en formato Markdown para copiar/pegar en el TFM
    md_path = os.path.join(out_dir, f"tabla_metricas_{nombre_modelo.lower().replace(' ', '_')}_TFM.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"### Resultados Analíticos: {nombre_modelo}\n\n")
        f.write("| Métrica | Valor |\n")
        f.write("| :--- | :--- |\n")
        f.write(f"| **Movimientos Evaluados (Detectados)** | {len(claves_detectadas):,} |\n")
        f.write(f"| **Riesgos Reales (Ground Truth humana)** | {len(claves_reales):,} |\n")
        f.write(f"| **Verdaderos Positivos (Éxitos)** | {verdaderos_positivos:,} |\n")
        f.write(f"| **Falsos Positivos (Ruido)** | {falsos_positivos:,} |\n")
        f.write(f"| **Falsos Negativos (Riesgos Omitidos)** | {falsos_negativos:,} |\n")
        f.write(f"| **Precisión (Precision)** | {precision:.2%} |\n")
        f.write(f"| **Sensibilidad (Recall)** | {recall:.2%} |\n")
        f.write(f"| **F1-Score** | {f1:.2%} |\n")
    logger.info(f"📄 Tabla Markdown para TFM exportada a: {md_path}")

    # Generar gráficos de contraste (post-contraste)
    try:
        generar_graficos_contraste(
            verdaderos_positivos, falsos_positivos, falsos_negativos,
            precision, recall, f1,
            nombre_modelo, out_dir
        )
    except Exception as e:
        logger.error(f"Error generando gráficos de contraste: {e}")

    return df_resumen


if __name__ == "__main__":
    # Configuración: especificar años puntuales o dejarlo en None para analizar todo.
    # Ejemplos: años=[2018], años=[2018, 2019], o usar fecha_inicio/fecha_fin.
    AÑOS_ANALISIS = [2018, 2019]  # Acotado a los años con datos de contraste disponibles

    # 1. Ejecutar detección por reglas
    df_movimientos_riesgo = procesar_alertas(años=AÑOS_ANALISIS)

    # 2. Cargar datos de contraste
    df_contraste = cargar_contraste()

    # 3. Evaluar eficacia del modelo de reglas
    if not df_movimientos_riesgo.empty and not df_contraste.empty:
        evaluar_contraste(df_movimientos_riesgo, df_contraste, "Reglas de Negocio A1-A5")
