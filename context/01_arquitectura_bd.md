# 01_arquitectura_bd

## Enfoque general

La arquitectura actual del proyecto se basa en un modelo relacional normalizado implementado en SQL Server, bajo la base de datos `TFM_SGP` y el esquema `sgp`. El diseño organiza la información en tablas de dimensiones (`DIM_`), hechos (`FACT_`) y referencias (`REF_`), con el objetivo de consolidar en una misma estructura la información de Cuentas Maestras (CM) y Cuentas Maestras Pagadoras (CMP), preservando integridad referencial, trazabilidad operativa y capacidad de análisis posterior.

Aunque se utiliza la nomenclatura `DIM/FACT/REF` por claridad y utilidad analítica, el modelo mantiene un enfoque relacional normalizado, no un esquema estrella puro. Las entidades maestras se separan para reducir redundancia y anomalías de actualización, mientras que las tablas de hechos concentran los eventos transaccionales asociados a cuentas y beneficiarios.

## Capas actuales del modelo

### 1. Catálogos y dimensiones maestras

Esta capa contiene catálogos técnicos y entidades maestras necesarias para normalizar la información operativa.

**Tablas incluidas:**
- `DIM_BANCOS`
- `DIM_TITULARES`
- `DIM_ENTIDADES_TERRITORIALES`
- `DIM_RESGUARDOS`
- `DIM_BENEFICIARIOS`
- `DIM_TIPO_CM`
- `DIM_TIPO_CMP`
- `DIM_TIPO_MOVIMIENTO`
- `DIM_TIPO_CUENTA_BANCARIA`
- `DIM_TIPO_ID_BENEFICIARIO`
- `DIM_CUENTAS_CM`
- `DIM_CUENTAS_CMP`

**Rol de la capa:**
- Estandarizar bancos, tipos de cuenta, tipos de identificación, tipos de cuenta maestra y tipos de movimiento.
- Consolidar el inventario de titulares, entidades territoriales, resguardos y cuentas.
- Reducir duplicidad en atributos repetidos en archivos operativos.
- Servir como base de referencia para la validación de integridad en las tablas de movimientos.

### 2. Hechos operativos

Esta capa almacena los movimientos reportados por las entidades financieras, ya homologados a una estructura común.

**Tablas incluidas:**
- `FACT_MOVIMIENTOS_CM`
- `FACT_MOVIMIENTOS_CMP`

**Rol de la capa:**
- Registrar los movimientos de las Cuentas Maestras y de las Cuentas Maestras Pagadoras.
- Mantener la trazabilidad transaccional de fechas, valores, tipos de movimiento y beneficiarios.
- Soportar análisis de concentración, identificación de patrones y detección de operaciones no permitidas.

### 3. Referencias de correspondencia y reglas base

Esta capa soporta la homologación entre estructuras normativas y catálogos de movimientos permitidos.

**Tablas incluidas:**
- `REF_TIPOS_CM_CMP`
- `REF_TIPO_MOV_TIPO_CM`

**Rol de la capa:**
- Relacionar tipos de CM con tipos de CMP.
- Mantener una matriz de correspondencia entre sector, asignación, tipo de cuenta y tipo pagador.
- Definir el conjunto base de tipos de movimiento asociados a cada tipo de cuenta maestra y sector.
- Servir como insumo para reglas de validación explicables.

## Relaciones principales del modelo

Las relaciones estructurales más importantes del modelo actual son:

- `DIM_CUENTAS_CM` se relaciona con:
  - `DIM_TITULARES`
  - `DIM_TIPO_CM`
  - `DIM_BANCOS`
  - `DIM_RESGUARDOS` (cuando aplica)

- `DIM_CUENTAS_CMP` se relaciona con:
  - `DIM_TITULARES`
  - `DIM_TIPO_CMP`
  - `DIM_CUENTAS_CM`
  - `DIM_BANCOS`
  - `DIM_RESGUARDOS` (cuando aplica)

- `FACT_MOVIMIENTOS_CM` se relaciona con:
  - `DIM_TIPO_CM`
  - `DIM_CUENTAS_CM`
  - `DIM_TIPO_MOVIMIENTO`
  - `DIM_TIPO_ID_BENEFICIARIO`
  - `DIM_BENEFICIARIOS`
  - `DIM_BANCOS`

- `FACT_MOVIMIENTOS_CMP` se relaciona con:
  - `DIM_TIPO_CMP`
  - `DIM_CUENTAS_CMP`
  - `DIM_CUENTAS_CM`
  - `DIM_TIPO_MOVIMIENTO`
  - `DIM_TIPO_ID_BENEFICIARIO`

- `DIM_TIPO_CMP` se relaciona con `DIM_TIPO_CM`

- `REF_TIPO_MOV_TIPO_CM` se relaciona con `DIM_TIPO_CM`

## Principios de diseño aplicados

- Integridad referencial mediante PK y FK.
- Preservación de códigos con ceros a la izquierda usando tipos texto cuando aplica.
- Separación entre catálogos, entidades maestras, hechos transaccionales y referencias de correspondencia.
- Reutilización de catálogos para evitar redundancia.
- Preparación para cargas ETL desde archivos `.txt` normativos.
- Compatibilidad con análisis posterior en SQL Server, Python y Power BI Desktop.

## Estrategia de carga

El modelo actual distingue tres tipos de carga:

### 1. Datos semilla
Corresponde a catálogos relativamente estables y tablas de referencia.

**Ejemplos:**
- `DIM_TIPO_CM`
- `DIM_TIPO_CMP`
- `DIM_TIPO_CUENTA_BANCARIA`
- `DIM_TIPO_ID_BENEFICIARIO`
- `DIM_TIPO_MOVIMIENTO`
- `DIM_BANCOS`
- `REF_TIPOS_CM_CMP`
- `REF_TIPO_MOV_TIPO_CM`

### 2. Mini-ETL para dimensiones maestras
Corresponde a tablas con menor frecuencia de cambio, pero críticas para la consistencia del modelo.

**Ejemplos:**
- `DIM_TITULARES`
- `DIM_ENTIDADES_TERRITORIALES`
- `DIM_RESGUARDOS`
- `DIM_CUENTAS_CM`
- `DIM_CUENTAS_CMP`
- `DIM_BENEFICIARIOS`

### 3. ETL periódico para hechos
Corresponde a la ingesta mensual o periódica de los movimientos de CM y CMP.

**Ejemplos:**
- `FACT_MOVIMIENTOS_CM`
- `FACT_MOVIMIENTOS_CMP`

Adicionalmente, el proyecto contempla procesos auxiliares de contraste y completitud, como la identificación de Cuentas Maestras no registradas a partir de registros tipo 2 de los archivos fuente, con el fin de apoyar el mantenimiento de dimensiones maestras y mejorar la cobertura del modelo.

## Script maestro de despliegue

La arquitectura se despliega mediante el script `99_run_all.sql`, que ejecuta en secuencia:

1. creación del esquema `sgp`
2. creación de tablas DIM
3. creación de tablas REF
4. creación de tablas FACT
5. carga inicial de datos semilla donde aplica

Esta estrategia reduce errores por dependencias entre claves primarias y foráneas y estandariza la construcción del entorno de base de datos.

## Observaciones actuales del modelo

- El modelo ya implementa la base relacional principal necesaria para CM y CMP.
- Las tablas `REF_` actuales soportan homologación y reglas base, pero aún no existe una capa formal de alertas o trazabilidad de validaciones.
- Las tablas de log y alertas siguen siendo una línea de evolución del proyecto, no una parte del modelo físico actual.
