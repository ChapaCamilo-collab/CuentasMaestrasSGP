"""
Script de detección de anomalías híbrido (Clustering + Outliers) mediante Machine Learning.
Combina KMeans para identificar perfiles transaccionales y Isolation Forest para detectar 
movimientos de riesgo, optimizando la reducción de Falsos Negativos (Recall).
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
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, QuantileTransformer, RobustScaler
from sklearn.cluster import KMeans
from sqlalchemy import Engine, text

# Añadir el directorio raíz al path para asegurar importaciones de módulos comunes
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from python.common.db_connection import get_db_engine
from python.common.utils import ensure_dir, get_logger

logger = get_logger("alertas_ml")
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

# Tipos de movimiento que NO representan operaciones transaccionales reales
TIPOS_EXCLUIDOS = {"500", "600"}

# Ruta al archivo de contraste con riesgos previamente identificados
RUTA_CONTRASTE = r"c:\Users\camil\Documents\TFM-SGP-CuentasMaestras\python\Detección Movimientos Anómalos\Constraste\consolidado_movimientos_suspension_masiva_RIESGOS.txt"


def enriquecer_datos(df: pd.DataFrame, db_engine: Engine) -> pd.DataFrame:
    """Enriquece los movimientos con información contextual."""
    # Clasificar INGRESO / EGRESO
    query_tipos = "SELECT TIPO_MOVIMIENTO, CATEGORIA FROM sgp.DIM_TIPO_MOVIMIENTO"
    df_tipos = pd.read_sql(text(query_tipos), db_engine)
    tipo_a_cat = dict(zip(df_tipos["TIPO_MOVIMIENTO"].astype(str), df_tipos["CATEGORIA"].str.upper()))
    df["CATEGORIA"] = df["TIPO_MOVIMIENTO"].astype(str).map(tipo_a_cat).fillna("OTRO")
    
    # Obtener titulares
    query_cuentas = "SELECT NUMERO_CM, NIT_TITULAR FROM sgp.DIM_CUENTAS_CM"
    df_cuentas = pd.read_sql(text(query_cuentas), db_engine)
    df = df.merge(df_cuentas, on="NUMERO_CM", how="left")

    # Indicador de auto-traslado
    nit_norm = df["NIT_TITULAR"].astype(str).str.lstrip("0")
    id_benef_norm = df["ID_BENEFICIARIO"].astype(str).str.lstrip("0")
    df["ES_MISMO_TITULAR"] = (nit_norm == id_benef_norm).astype(int)

    return df


def calcular_features_agregados(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula features mensuales y los une al detalle transaccional."""
    df["PERIODO"] = df["FECHA_MOVIMIENTO"].dt.to_period("M").astype(str)

    agg = df.groupby(["NUMERO_CM", "PERIODO"]).agg(
        TOTAL_INGRESOS=("VALOR", lambda x: x[df.loc[x.index, "CATEGORIA"] == "INGRESO"].sum()),
        TOTAL_EGRESOS=("VALOR", lambda x: x[df.loc[x.index, "CATEGORIA"] == "EGRESO"].sum()),
        N_EGRESOS_MES=("VALOR", lambda x: (df.loc[x.index, "CATEGORIA"] == "EGRESO").sum())
    ).reset_index()

    agg["RATIO_EGRESO_INGRESO"] = (agg["TOTAL_EGRESOS"] / agg["TOTAL_INGRESOS"]).fillna(0).replace(np.inf, 100)
    
    df = df.merge(
        agg[["NUMERO_CM", "PERIODO", "RATIO_EGRESO_INGRESO", "N_EGRESOS_MES"]],
        on=["NUMERO_CM", "PERIODO"],
        how="left"
    )
    # Ratio específico de este movimiento vs ingreso mensual
    df["RATIO_EGRESO_TITULAR"] = (df["VALOR"] * df["ES_MISMO_TITULAR"] / df["RATIO_EGRESO_INGRESO"].map(lambda x: 1)).fillna(0)
    
    return df


def graficar_metodo_codo_ml(df_features: pd.DataFrame, output_dir: str):
    """Genera la gráfica del Método del Codo para el modelo híbrido."""
    logger.info("Generando gráfico del Método del Codo para ML...")
    X = df_features.fillna(0)
    scaler = RobustScaler()
    try:
        X_scaled = scaler.fit_transform(X)
        wcss = []
        max_k = min(10, len(X))
        for i in range(1, max_k + 1):
            kmeans = KMeans(n_clusters=i, random_state=42, n_init=10)
            kmeans.fit(X_scaled)
            wcss.append(kmeans.inertia_)
        
        plt.figure(figsize=(10, 6))
        plt.plot(range(1, max_k + 1), wcss, marker='o', linestyle='--')
        plt.title('Método del Codo - Justificación de Clusters de Riesgo')
        plt.xlabel('Número de Clusters (K)')
        plt.ylabel('WCSS')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "ml_00_justificacion_clusters_codo.png"))
        plt.close()
    except Exception as e:
        logger.error(f"No se pudo generar gráfico del codo: {e}")


def generar_graficos_deteccion_ml(df: pd.DataFrame, anomalias: pd.DataFrame, output_dir: str):
    """Genera gráficos de diagnóstico del modelo ML."""
    logger.info("Generando gráficos de diagnóstico ML...")
    
    # 1. Distribución de Scores
    plt.figure(figsize=(10, 5))
    plt.hist(df["SCORE_ANOMALIA"], bins=100, color='skyblue', alpha=0.7, label='Todos')
    plt.hist(anomalias["SCORE_ANOMALIA"], bins=50, color='red', alpha=0.7, label='Riesgo')
    plt.title("Distribución de Scores de Riesgo (IA)")
    plt.legend()
    plt.savefig(os.path.join(output_dir, "ml_01_scores.png"))
    plt.close()

    # 2. Clusters vs Valor
    if "CLUSTER_ID" in df.columns:
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df.sample(n=min(10000, len(df))), x="VALOR", y="SCORE_ANOMALIA", 
                        hue="CLUSTER_ID", palette="viridis", alpha=0.5)
        plt.xscale("log")
        plt.title("Perfiles Transaccionales vs Score de Riesgo")
        plt.savefig(os.path.join(output_dir, "ml_02_clusters.png"))
        plt.close()


def ejecutar_deteccion_anomalias(
    contamination: float = 0.15,
    años: Optional[list] = None,
    fecha_inicio: Optional[str] = None,
    fecha_fin: Optional[str] = None
) -> tuple:
    """Ejecuta el modelo ML híbrido para detección de riesgos."""
    query = "SELECT * FROM sgp.FACT_MOVIMIENTOS_CM"
    condiciones = []
    if años: condiciones.append(f"YEAR(FECHA_MOVIMIENTO) IN ({','.join(map(str, años))})")
    if condiciones: query += " WHERE " + " AND ".join(condiciones)

    logger.info(f"Consultando movimientos para IA...")
    df = pd.read_sql(text(query), engine)
    if df.empty: return pd.DataFrame(), pd.DataFrame()

    df["TIPO_MOVIMIENTO"] = df["TIPO_MOVIMIENTO"].astype(str).str.strip()
    df = df[~df["TIPO_MOVIMIENTO"].isin(TIPOS_EXCLUIDOS)].copy()
    
    df["FECHA_MOVIMIENTO"] = pd.to_datetime(df["FECHA_MOVIMIENTO"])
    df = enriquecer_datos(df, engine)
    df = calcular_features_agregados(df)

    # Features para Clustering y Outliers
    features = ["VALOR", "RATIO_EGRESO_INGRESO", "RATIO_EGRESO_TITULAR", "ES_MISMO_TITULAR", "N_EGRESOS_MES"]
    X = df[features].fillna(0)
    
    out_dir = os.path.join(os.path.dirname(__file__), "resultados")
    ensure_dir(out_dir)
    
    # Clustering para contexto
    scaler_c = RobustScaler()
    X_scaled_c = scaler_c.fit_transform(X)
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    df["CLUSTER_ID"] = kmeans.fit_predict(X_scaled_c)
    
    graficar_metodo_codo_ml(df[features].sample(n=min(10000, len(df))), out_dir)

    # Isolation Forest
    X_ml = df[features + ["CLUSTER_ID"]].fillna(0)
    scaler_ml = QuantileTransformer(output_distribution='normal', random_state=42)
    X_ml_scaled = scaler_ml.fit_transform(X_ml)
    
    logger.info(f"Entrenando Isolation Forest (Contaminación: {contamination})...")
    iso = IsolationForest(n_estimators=300, contamination=contamination, random_state=42, n_jobs=-1)
    df["ES_ANOMALIA"] = iso.fit_predict(X_ml_scaled)
    df["SCORE_ANOMALIA"] = iso.decision_function(X_ml_scaled)

    # Filtro crucial de negocio: Solo evaluamos como RIESGO las salidas de dinero (EGRESOS)
    anomalias = df[(df["ES_ANOMALIA"] == -1) & (df["CATEGORIA"] == "EGRESO")].copy()
    anomalias = anomalias.sort_values(by="SCORE_ANOMALIA")

    if not anomalias.empty:
        f_path = os.path.join(out_dir, "alertas_ml_hibrido.xlsx")
        anomalias.to_excel(f_path, index=False)
        generar_graficos_deteccion_ml(df, anomalias, out_dir)

    return df, anomalias


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
    """Genera gráficos de evaluación del contraste del modelo ML."""
    logger.info("Generando gráficos de evaluación post-contraste...")
    prefijo = nombre_modelo.lower().replace(" ", "_").replace("-", "").replace("+", "_").replace("(", "").replace(")", "")

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


def cargar_contraste() -> pd.DataFrame:
    """Carga datos de contraste."""
    if not os.path.exists(RUTA_CONTRASTE): return pd.DataFrame()
    df = pd.read_csv(RUTA_CONTRASTE, sep=";", encoding="latin-1", dtype=str)
    df["NUMERO_CM"] = df["NUMERO_CM"].astype(str).str.strip()
    df["FECHA_MOVIMIENTO"] = pd.to_datetime(df["FECHA_MOVIMIENTO"], errors="coerce")
    df["VALOR"] = pd.to_numeric(df["VALOR"], errors="coerce")
    return df


def evaluar_contraste(df_det: pd.DataFrame, df_real: pd.DataFrame, nombre: str):
    """Evalúa eficacia."""
    # Generar clave compuesta de forma vectorizada (Best Practice Pandas O(1) vs O(N))
    fecha_det = df_det["FECHA_MOVIMIENTO"].dt.strftime("%Y-%m-%d").fillna("")
    c_det = set((df_det["NUMERO_CM"].astype(str) + "|" + fecha_det + "|" + df_det["VALOR"].astype(str)))

    fecha_real = df_real["FECHA_MOVIMIENTO"].dt.strftime("%Y-%m-%d").fillna("")
    c_real = set((df_real["NUMERO_CM"].astype(str) + "|" + fecha_real + "|" + df_real["VALOR"].astype(str)))

    tp = len(c_det & c_real)
    fp = len(c_det - c_real)
    fn = len(c_real - c_det)

    prec = tp / (tp + fp) if (tp+fp)>0 else 0
    rec = tp / (tp + fn) if (tp+fn)>0 else 0
    f1 = 2*prec*rec/(prec+rec) if (prec+rec)>0 else 0

    out_dir = os.path.join(os.path.dirname(__file__), "resultados")
    ensure_dir(out_dir)

    resumen = {
        "Modelo": [nombre],
        "Detectados": [len(c_det)],
        "Riesgos_Reales": [len(c_real)],
        "VP (True Pos)": [tp],
        "FP (False Pos)": [fp],
        "FN (False Neg)": [fn],
        "Precision": [round(prec, 4)],
        "Recall": [round(rec, 4)],
        "F1_Score": [round(f1, 4)],
    }
    df_resumen = pd.DataFrame(resumen)
    sane_name = nombre.lower().replace(' ', '_').replace('+', '_').replace('(', '').replace(')', '')
    f_path = os.path.join(out_dir, f"contraste_{sane_name}.xlsx")
    df_resumen.to_excel(f_path, index=False)
    logger.info(f"📄 Resumen Excel exportado a: {f_path}")
    
    # Exportar tabla en formato Markdown para copiar/pegar en el TFM
    md_path = os.path.join(out_dir, f"tabla_metricas_{sane_name}_TFM.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"### Resultados de Eficacia: {nombre}\n\n")
        f.write("| Métrica | Valor |\n")
        f.write("| :--- | :--- |\n")
        f.write(f"| **Movimientos Detectados (Riesgo IA)** | {len(c_det):,} |\n")
        f.write(f"| **Riesgos Reales (Ground Truth)** | {len(c_real):,} |\n")
        f.write(f"| **Verdaderos Positivos (Éxitos)** | {tp:,} |\n")
        f.write(f"| **Falsos Positivos (Falsas Alarmas)** | {fp:,} |\n")
        f.write(f"| **Falsos Negativos (Riesgos Omitidos)** | {fn:,} |\n")
        f.write(f"| **Precisión (Precision)** | {prec:.2%} |\n")
        f.write(f"| **Sensibilidad (Recall)** | {rec:.2%} |\n")
        f.write(f"| **F1-Score** | {f1:.2%} |\n")
    logger.info(f"📄 Tabla Markdown para TFM exportada a: {md_path}")

    # Generar gráficos de contraste (post-contraste)
    try:
        generar_graficos_contraste(
            tp, fp, fn,
            prec, rec, f1,
            nombre, out_dir
        )
    except Exception as e:
        logger.error(f"Error generando gráficos de contraste: {e}")

    logger.info(f"Eficacia {nombre}: Recall={rec:.2%}, Precision={prec:.2%}")
    return {"Precision": prec, "Recall": rec, "F1": f1}


if __name__ == "__main__":
    AÑOS = [2018, 2019]
    df_c, anom = ejecutar_deteccion_anomalias(contamination=0.15, años=AÑOS)
    df_real = cargar_contraste()
    if not anom.empty and not df_real.empty:
        evaluar_contraste(anom, df_real, "IA Hibrida (IsoForest+KMeans)")
