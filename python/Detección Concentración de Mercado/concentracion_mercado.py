"""
Script de análisis de concentración de mercado para el proyecto TFM-SGP.
Calcula el perfil transaccional de beneficiarios e identifica posibles monopolios o riesgos.
"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sqlalchemy import Engine, text
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

# Añadir el directorio raíz al path para asegurar importaciones de módulos comunes
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from python.common.db_connection import get_db_engine
from python.common.utils import ensure_dir, get_logger
 
# --- CONFIGURACIÓN PARA TFM (PRIVACIDAD) ---
ANONYMIZE_SENSITIVE_DATA = True  # Cambia a False para ver datos completos (uso interno)
# -------------------------------------------
 
# Configuración de estilo para gráficos
sns.set_theme(style="whitegrid")
logger = get_logger("concentracion_mercado")


def enriquecer_con_cuentas(df_movimientos: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Cruza los movimientos con la información de las cuentas maestras (DIM_CUENTAS_CM)."""
    query = "SELECT NUMERO_CM, NIT_TITULAR, DIVIPOLA FROM sgp.DIM_CUENTAS_CM"
    df_cuentas = pd.read_sql(text(query), engine)
    return df_movimientos.merge(df_cuentas, on="NUMERO_CM", how="left")


def filtrar_egresos(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Filtra los movimientos para incluir solo egresos (salidas de recursos)."""
    if "TIPO_MOVIMIENTO" not in df.columns:
        logger.warning("Columna 'TIPO_MOVIMIENTO' no detectada. No se aplicó filtro de egresos.")
        return df

    query = "SELECT TIPO_MOVIMIENTO FROM sgp.DIM_TIPO_MOVIMIENTO WHERE UPPER(CATEGORIA) = 'EGRESO'"
    df_tipo_mov = pd.read_sql(text(query), engine)
    codigos_egreso = df_tipo_mov["TIPO_MOVIMIENTO"].astype(str).tolist()
    
    return df[df["TIPO_MOVIMIENTO"].astype(str).isin(codigos_egreso)].copy()


def construir_beneficiario_clave(df: pd.DataFrame) -> pd.Series:
    """Genera una clave única para cada beneficiario basada en ID y cuenta bancaria."""
    tipo_id = df["TIPO_ID_BENEFICIARIO"].fillna("XX").astype(str)
    id_benef = df["ID_BENEFICIARIO"].fillna("000").astype(str)
    cuenta_benef = df.get("CUENTA_BENEFICIARIO", pd.Series("000", index=df.index)).fillna("000").astype(str)
    return tipo_id + "_" + id_benef + "_" + cuenta_benef


def generar_graficos(perfil: pd.DataFrame, output_dir: str, timestamp: str):
    """Genera visualizaciones analíticas de la concentración de recursos."""
    logger.info("Generando visualizaciones analíticas...")
    
    # 1. Curva de Lorenz
    plt.figure(figsize=(10, 6))
    p_sorted = perfil.sort_values("TOTAL_RECIBIDO", ascending=False)
    lorenz_y = p_sorted["TOTAL_RECIBIDO"].cumsum() / p_sorted["TOTAL_RECIBIDO"].sum() * 100
    lorenz_x = np.arange(1, len(lorenz_y) + 1) / len(lorenz_y) * 100
    plt.plot(lorenz_x, lorenz_y, color="darkred", label="Curva de Concentración")
    plt.plot([0, 100], [0, 100], color="grey", linestyle="--", label="Igualdad Perfecta")
    plt.title("Concentración de Capital (Lorenz)")
    plt.xlabel("% Beneficiarios")
    plt.ylabel("% Monto Acumulado")
    plt.legend()
    plt.savefig(os.path.join(output_dir, "01_lorenz.png"))
    plt.close()

    # 2. Top 20 Beneficiarios por Monto
    top_n = min(20, len(perfil))
    plt.figure(figsize=(12, 8))
    sns.barplot(x="TOTAL_RECIBIDO", y="BENEFICIARIO_CLAVE", data=p_sorted.head(top_n), palette="Reds_r")
    plt.title(f"Top {top_n} Beneficiarios por Monto Total Recibido")
    plt.xlabel("Monto Total (COP)")
    plt.savefig(os.path.join(output_dir, "02_top_beneficiarios_monto.png"))
    plt.close()

    # 2b. Top 20 Beneficiarios por Cantidad de Territorios (Diversidad de Origen)
    p_sorted_territorios = perfil.sort_values("N_TERRITORIOS_ORIGEN", ascending=False)
    plt.figure(figsize=(12, 8))
    sns.barplot(x="N_TERRITORIOS_ORIGEN", y="BENEFICIARIO_CLAVE", data=p_sorted_territorios.head(top_n), palette="GnBu_r")
    plt.title(f"Top {top_n} Beneficiarios por Cantidad de Territorios de Origen")
    plt.xlabel("Número de Territorios (DIVIPOLA)")
    plt.savefig(os.path.join(output_dir, "03_top_beneficiarios_territorios.png"))
    plt.close()

    # 3. Distribución Acumulada de Montos (ECDF - Más informativa)
    plt.figure(figsize=(10, 6))
    sns.ecdfplot(perfil["TOTAL_RECIBIDO"], color="darkblue")
    plt.title("Probabilidad Acumulada de Montos Recibidos (ECDF)")
    plt.xlabel("Monto Recibido (COP)")
    plt.ylabel("Proporción de Beneficiarios")
    plt.xscale("log")
    plt.grid(True, which="both", linestyle="--", alpha=0.5)
    plt.savefig(os.path.join(output_dir, "04_probabilidad_acumulada_montos.png"))
    plt.close()

    # 4. Distribución de Territorios por Rango de Monto (Boxplot)
    perfil["CATEGORIA_MONTO"] = pd.qcut(perfil["TOTAL_RECIBIDO"], q=4, labels=["Q1 (Bajo)", "Q2", "Q3", "Q4 (Alto)"])
    plt.figure(figsize=(10, 6))
    sns.boxplot(x="CATEGORIA_MONTO", y="N_TERRITORIOS_ORIGEN", data=perfil, palette="Set2")
    plt.title("Dispersión de Territorios Origen por Cuartil de Monto", fontsize=14)
    plt.xlabel("Cuartil de Monto Total Recibido")
    plt.ylabel("N° Territorios")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "05_territorios_origen_boxplot.png"), dpi=300)
    plt.close()

    # 5. Visualización de Clusters (Machine Learning)
    if "CLUSTER_RIESGO" in perfil.columns:
        plt.figure(figsize=(10, 7))
        sns.scatterplot(
            x="N_TRANSACCIONES", 
            y="TOTAL_RECIBIDO", 
            hue="CLUSTER_RIESGO", 
            size="N_TERRITORIOS_ORIGEN",
            sizes=(20, 200),
            data=perfil, 
            palette="viridis",
            alpha=0.7
        )
        plt.xscale('log')
        plt.yscale('log')
        plt.title("Clusters K-Means: Patrones Transaccionales y Riesgo", fontsize=14)
        plt.xlabel("Número de Transacciones (Log)")
        plt.ylabel("Monto Total Recibido (Log)")
        plt.legend(title="Cluster (Riesgo)", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "06_ml_clusters_kmeans.png"), dpi=300)
        plt.close()
    else:
        logger.warning("La columna 'CLUSTER_RIESGO' no está presente en el perfil, no se generó el gráfico de clustering.")
    
    # 6. Método del Codo (Justificación de K)
    try:
        graficar_metodo_codo(perfil, output_dir)
    except Exception as e:
        logger.error(f"Error al graficar método del codo: {e}")
    
    logging.info(f"Gráficos exportados exitosamente en la carpeta: {output_dir}")


def graficar_metodo_codo(perfil: pd.DataFrame, output_dir: str):
    """Genera la gráfica del Método del Codo para justificar el número de clusters (K)."""
    features = ["TOTAL_RECIBIDO", "N_TRANSACCIONES", "N_TERRITORIOS_ORIGEN", "RECURRENCIA"]
    X = perfil[features].fillna(0)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Probar diferentes valores de K (de 1 a 10)
    wcss = []
    max_k = min(10, len(perfil))
    for i in range(1, max_k + 1):
        kmeans = KMeans(n_clusters=i, random_state=42, n_init=10)
        kmeans.fit(X_scaled)
        wcss.append(kmeans.inertia_)
    
    plt.figure(figsize=(10, 6))
    plt.plot(range(1, max_k + 1), wcss, marker='o', linestyle='--', color='teal')
    plt.title('Método del Codo (Elbow Method) - Selección Óptima de Clusters', fontsize=14)
    plt.xlabel('Número de Clusters (K)', fontsize=12)
    plt.ylabel('WCSS (Suma de cuadrados intra-cluster)', fontsize=12)
    plt.xticks(range(1, max_k + 1))
    plt.grid(True, alpha=0.3)
    
    # Anotar el punto elegido (K=4)
    if max_k >= 4:
        plt.annotate('Punto de inflexión sugerido (K=4)', 
                     xy=(4, wcss[3]), xytext=(5, wcss[3]*1.5),
                     arrowprops=dict(facecolor='black', shrink=0.05, width=1, headwidth=5))
                     
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "07_justificacion_clusters_codo.png"), dpi=300)
    plt.close()
    logger.info("Gráfico del Método del Codo generado.")


def aplicar_clustering_riesgo(perfil: pd.DataFrame, n_clusters: int = 4) -> pd.DataFrame:
    """
    Aplica K-Means para agrupar beneficiarios según su perfil de riesgo.
    Variables usadas: TOTAL_RECIBIDO, N_TRANSACCIONES, N_TERRITORIOS_ORIGEN, RECURRENCIA.
    """
    if len(perfil) < n_clusters:
        logger.warning(f"Número de beneficiarios ({len(perfil)}) es menor que el número de clusters ({n_clusters}). No se aplicará clustering.")
        perfil["CLUSTER_RIESGO"] = -1 # Asignar un valor por defecto
        return perfil

    # Seleccionar características para el modelo
    features = ["TOTAL_RECIBIDO", "N_TRANSACCIONES", "N_TERRITORIOS_ORIGEN", "RECURRENCIA"]
    
    # Asegurarse de que todas las características existan y manejar NaNs
    for col in features:
        if col not in perfil.columns:
            logger.warning(f"La característica '{col}' no se encontró en el perfil. Se omitirá o se llenará con 0.")
            perfil[col] = 0 # O manejar de otra forma si es más apropiado
    
    X = perfil[features].fillna(0)
    
    # Estandarización de datos (K-Means es sensible a la escala)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Entrenar modelo
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    perfil["CLUSTER_RIESGO"] = kmeans.fit_predict(X_scaled)
    
    # Opcional: Intentar ordenar los clusters por "importancia" de monto para que el label sea consistente
    # Esto ayuda a interpretar que un cluster con mayor número es de "mayor riesgo" o "mayor monto"
    cluster_order = perfil.groupby("CLUSTER_RIESGO")["TOTAL_RECIBIDO"].mean().sort_values().index
    cluster_mapping = {old: new for new, old in enumerate(cluster_order)}
    perfil["CLUSTER_RIESGO"] = perfil["CLUSTER_RIESGO"].map(cluster_mapping)
    
    return perfil
 
 
def enmascarar_datos_sensibles(perfil: pd.DataFrame) -> pd.DataFrame:
    """
    Enmascara identificaciones y cuentas bancarias para reportes de TFM. 
    Ejemplo: 'CC_12345678_000012345' -> 'CC_XXXX5678_XXXXX2345'
    """
    logger.info("Enmascarando información sensible para reporte TFM...")
    df = perfil.copy()
    
    def mask_id(val):
        if not isinstance(val, str): return str(val)
        parts = val.split("_")
        masked_parts = []
        for p in parts:
            if len(p) > 4:
                masked_parts.append("X" * (len(p) - 4) + p[-4:])
            else:
                masked_parts.append(p)
        return "_".join(masked_parts)
 
    df["BENEFICIARIO_CLAVE"] = df["BENEFICIARIO_CLAVE"].apply(mask_id)
    return df
 
 
def ejecutar_analisis(años: Optional[list] = None, fecha_inicio: Optional[str] = None, fecha_fin: Optional[str] = None):
    """Orquestador del análisis de concentración de mercado con manejo de memoria eficiente."""
    engine = get_db_engine()
    
    query = "SELECT * FROM sgp.FACT_MOVIMIENTOS_CM"
    condiciones = []
    
    if años:
        # Permite filtrar por una lista de años (ej: [2023, 2024])
        condiciones.append(f"YEAR(FECHA_MOVIMIENTO) IN ({','.join(map(str, años))})")
    
    if fecha_inicio:
        condiciones.append(f"FECHA_MOVIMIENTO >= '{fecha_inicio}'")
    if fecha_fin:
        condiciones.append(f"FECHA_MOVIMIENTO <= '{fecha_fin}'")
    
    if condiciones:
        query += " WHERE " + " AND ".join(condiciones)

    logger.info("Consultando movimientos transaccionales por lotes (procesamiento eficiente)...")
    
    # Procesamiento por lotes para evitar MemoryError
    chunk_size = 200000 
    perfil_acumulado = []
    
    try:
        for chunk in pd.read_sql(text(query), engine, chunksize=chunk_size):
            chunk["FECHA_MOVIMIENTO"] = pd.to_datetime(chunk["FECHA_MOVIMIENTO"])
            
            # Enriquecimiento y filtrado por lote
            df_enriquecido = enriquecer_con_cuentas(chunk, engine)
            df_egresos = filtrar_egresos(df_enriquecido, engine)
            
            if not df_egresos.empty:
                df_egresos["BENEFICIARIO_CLAVE"] = construir_beneficiario_clave(df_egresos)
                
                # Agregación parcial por lote
                resumen_lote = df_egresos.groupby("BENEFICIARIO_CLAVE").agg(
                    TOTAL_RECIBIDO=("VALOR", "sum"),
                    N_TRANSACCIONES=("VALOR", "count"),
                    # Guardamos conjuntos para nunique global posterior
                    SET_CUENTAS=("NUMERO_CM", lambda x: set(x)),
                    SET_TERRITORIOS=("DIVIPOLA", lambda x: set(x))
                )
                perfil_acumulado.append(resumen_lote)
                
        if not perfil_acumulado:
            logger.warning("No se encontraron egresos para el periodo seleccionado.")
            return

        # Consolidación final de todos los lotes
        logger.info("Consolidando resultados globales...")
        perfil = pd.concat(perfil_acumulado)
        perfil = perfil.groupby("BENEFICIARIO_CLAVE").agg({
            "TOTAL_RECIBIDO": "sum",
            "N_TRANSACCIONES": "sum",
            "SET_CUENTAS": lambda x: set().union(*x),
            "SET_TERRITORIOS": lambda x: set().union(*x)
        }).reset_index()
        
        # Convertir sets a conteos finales
        perfil["N_CUENTAS_ORIGEN"] = perfil["SET_CUENTAS"].apply(len)
        perfil["N_TERRITORIOS_ORIGEN"] = perfil["SET_TERRITORIOS"].apply(len)
        perfil.drop(columns=["SET_CUENTAS", "SET_TERRITORIOS"], inplace=True)

        # Métricas de concentración
        perfil["PARTICIPACION"] = perfil["TOTAL_RECIBIDO"] / perfil["TOTAL_RECIBIDO"].sum()
        perfil = perfil.sort_values("TOTAL_RECIBIDO", ascending=False)
        perfil["RANKING_RECIBIDO"] = range(1, len(perfil) + 1)
        perfil["RECURRENCIA"] = perfil["N_TRANSACCIONES"]
        
        # --- APRENDIZAJE AUTOMÁTICO (K-MEANS) ---
        logging.info("Aplicando Clustering K-Means para segmentación de riesgo...")
        perfil = aplicar_clustering_riesgo(perfil)
     
        # --- ANONIMIZACIÓN PARA TFM ---
        if ANONYMIZE_SENSITIVE_DATA:
            perfil = enmascarar_datos_sensibles(perfil)
        # ------------------------------
     
        # Exportación de Resultados
        out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados")
        ensure_dir(out_dir)
        
        perfil_file = os.path.join(out_dir, "analisis_concentracion_mercado.xlsx")
        perfil.to_excel(perfil_file, index=False)
        logger.info(f"Resultados exportados a: {perfil_file}")

        try:
            generar_graficos(perfil, out_dir, "")
        except Exception as e:
            logger.error(f"Error generando gráficos: {e}")

    except Exception as e:
        logger.error(f"Error durante la ejecución del análisis: {e}")
        raise e


if __name__ == "__main__":
    # Ejemplo: Análisis solo para el año 2024
    # Se puede usar años=[2023], años=[2018, 2023], o dejarlo vacío para todo (None).
    ejecutar_analisis(años=None)
