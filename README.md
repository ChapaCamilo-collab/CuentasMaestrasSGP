# 🏦 TFM: SGP - Cuentas Maestras

**Trabajo de Fin de Máster (TFM)** enfocado en el diseño, desarrollo e implementación de una base de datos relacional unificada y modelos analíticos para la gestión de las **Cuentas Maestras y Cuentas Maestras Pagadoras (CMP)** del **Sistema General de Participaciones (SGP)**.

---

## 🎯 Objetivo del Proyecto
El proyecto busca diseñar e implementar una arquitectura de datos unificada que integre estructuras previamente fragmentadas debido a la complejidad de los anexos técnicos, financieros y normativos. Las metas principales son:
- **Mejorar la trazabilidad** del ciclo de los recursos públicos.
- Facilitar el **seguimiento y control automatizado** de los movimientos bancarios.
- Servir como motor de datos para la **detección temprana de movimientos de riesgo** y el **análisis de concentración de mercado** orientado a Beneficiarios y Territorios.

## 🧩 Arquitectura y Componentes Principales
1. **Pipelines ETL (Python):** Scripts que extraen los archivos planos técnicos (`.txt`), los homologan asegurando compatibilidad transversal y cargan la base de datos aplicando reglas de limpieza e integridad.
2. **Modelo Bidimensional (SQL Server):** Un esquema relacional analítico robusto compuesto por tablas de Hechos (`FACT_MOVIMIENTOS_CM`, `FACT_MOVIMIENTOS_CMP`) para las transacciones financieras y Dimensiones (`DIM`) para Bancos, Entidades Territoriales, Titulares y Cuentas.
3. **Machine Learning y Analítica:** 
   - *Detección de Movimientos Anómalos:* Código y heurísticas (`alertas_riesgo.py`) apoyados en Machine Learning (`alertas_riesgo_ml.py`) para detectar operaciones inusuales, suspensiones masivas, u operaciones no permitidas.
   - *Concentración de Mercado:* Evaluación profunda del reparto de capital utilizando indicadores de distribución (Curvas de Lorenz) y algoritmos de clusterización espacial multidimensional (K-Means).

## 📂 Estructura del Repositorio
- `context/`: Documentación del marco normativo, diccionario de datos, reglas de negocio y flujogramas del sistema.
- `sql/`: Scripts de inicialización `T-SQL` organizados en definición (`/ddl/`), vistas (`/views/`) y validaciones lógicas (`/checks/`).
- `python/ETL/`: Pipelines encargados de la ingesta (Bancos, Titulares, Resguardos, Movimientos CM/CMP).
- `python/Detección Movimientos Anómalos/`: Código con los algoritmos y testeo cruzado de riesgo sistémico.
- `python/Detección Concentración de Mercado/`: Scripts estadísticos para el análisis de distribución y algoritmos de Machine Learning.
- `docs/`: Guías técnicas, bitácoras, registro de uso de IA y decisiones de arquitectura del TFM.

## ⚙️ Configuración y Despliegue Local
1. Clona el repositorio e ingresa a la carpeta:
   ```bash
   git clone https://github.com/ChapaCamilo-collab/CuentasMaestrasSGP.git
   cd TFM-SGP-CuentasMaestras
   ```
2. Instala las dependencias en tu entorno virtual:
   ```bash
   pip install -r requirements.txt
   ```
3. Configura tus variables de entorno duplicando `.env.example`:
   ```bash
   cp .env.example .env
   ```
   *Agrega tu cadena de conexión hacia tu instancia de SQL Server dentro del archivo creado.*
4. Inicializa el esquema de Base de Datos:
   - Utiliza SSMS o Azure Data Studio para correr el archivo orquestador `sql/ddl/99_run_all.sql`.
5. Orquesta y ejecuta los archivos de la carpeta `python/ETL/` en orden numérico para poblar la información.

---
📄 *Proyecto desarrollado para la sustentación y memoria académica.*
