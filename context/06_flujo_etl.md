# 06_flujo_etl

## Enfoque general
El proceso ETL del proyecto se implementa en Python y está diseñado para alimentar el modelo relacional de SQL Server de forma controlada, trazable y reproducible. Su propósito es transformar fuentes heterogéneas en estructuras consistentes para el modelo `DIM/FACT/REF`, preservando la integridad de los datos y facilitando validaciones posteriores.

El diseño contempla dos niveles de ETL:

1. **Mini-ETL para tablas DIM/REF**, orientado a cargas iniciales y actualizaciones puntuales de catálogos, dimensiones y tablas de referencia.
2. **ETL mensual para tablas FACT**, orientado a ingerir los movimientos reportados por las entidades financieras en archivos `.txt`, homologarlos y cargarlos en una estructura transaccional unificada.

---

## 1. Mini-ETL para tablas DIM/REF

### Objetivo
Mantener consistentes los catálogos y dimensiones que soportan el modelo relacional, reduciendo ambigüedades y asegurando que el ETL mensual de movimientos se base en fuentes de verdad confiables.

### Tablas típicas de este flujo
- `DIM_TITULARES`
- `DIM_ENTIDADES_TERRITORIALES`
- `DIM_RESGUARDOS`
- `DIM_CUENTAS_CM`
- `DIM_CUENTAS_CMP`
- `DIM_BENEFICIARIOS`
- tablas `REF_` de homologación y correspondencia

### Flujo general del mini-ETL
1. **Extracción** desde la fuente oficial o archivo de entrada correspondiente.
2. **Estandarización** de tipos y formatos.
3. **Validación** de unicidad, consistencia y dominios.
4. **Carga o actualización controlada** mediante inserción o lógica tipo upsert.
5. **Registro de trazabilidad** de la ejecución.

### Validaciones típicas del mini-ETL
- Limpieza de espacios y caracteres no deseados.
- Normalización de tipos de dato.
- Preservación de códigos como texto cuando aplica.
- Verificación de duplicados en llaves o códigos de negocio.
- Consistencia entre campos relacionados, por ejemplo:
  - `DIVIPOLA`, departamento y municipio.
  - titular y cuenta.
  - cuenta principal y cuenta pagadora.
- Validación de dominios frente a catálogos existentes.

### Resultado esperado
Un conjunto de tablas `DIM/REF` estable, consistente y preparado para soportar:
- cruces del ETL mensual,
- reglas de validación,
- homologación de estructuras,
- trazabilidad del modelo.

---

## 2. ETL mensual para tablas FACT

### Objetivo
Ingerir los movimientos reportados por las entidades financieras y cargarlos en las tablas de hechos del modelo, principalmente:
- `FACT_MOVIMIENTOS_CM`
- `FACT_MOVIMIENTOS_CMP`

Adicionalmente, antes de cargar los movimientos, este proceso identifica los beneficiarios presentes en la información transaccional para alimentar `DIM_BENEFICIARIOS`.

### Alcance funcional
Este ETL es el componente central para habilitar:
- trazabilidad de recursos,
- análisis transaccional,
- validación de operaciones,
- soporte para detección de riesgos,
- análisis posteriores en SQL, Python y Power BI.

---

## 3. Esquemas de reporte soportados

El proceso ETL está diseñado para soportar las variaciones normativas y estructurales de los archivos fuente. Actualmente contempla dos grandes esquemas de reporte:

### 3.1 Estructuras históricas por tipos de registro
Corresponde a archivos donde la información se organiza en registros diferenciados por tipo, por ejemplo:
- control o periodo,
- cuenta o titular,
- beneficiario,
- movimientos.

Este enfoque aplica a:
- estructuras históricas de **CM** asociadas a los marcos 2005 y 2015,
- estructuras históricas de **CMP** asociadas a la regulación de 2018.

### 3.2 Estructura 2023 por bloques de columnas
Corresponde a archivos donde la información se presenta en un mismo registro, pero distribuida en bloques de columnas equivalentes a:
- periodo,
- cuenta o titular,
- beneficiario cuando aplica,
- movimientos.

Aunque cambia la forma física del archivo, el ETL homologa estos bloques a una estructura equivalente a la de los esquemas históricos.

---

## 4. Flujo general del ETL mensual

### Paso 1. Identificación del archivo y de la estructura
Se determina:
- si el archivo corresponde a **CM** o **CMP**,
- si su organización es por **tipos de registro** o por **bloques de columnas**,
- y qué lógica de extracción y homologación debe aplicarse.

### Paso 2. Lectura del archivo fuente
Se realiza la lectura del archivo `.txt` usando separador `;`, preservando como texto los códigos que así lo requieran para evitar:
- pérdida de ceros a la izquierda,
- conversión incorrecta de identificadores,
- inconsistencias en cruces con dimensiones.

### Paso 3. Extracción del bloque de movimientos
Según la estructura detectada:
- en archivos por tipos, se recorre el archivo y se captura la información del tipo de registro asociado a movimientos;
- en archivos por bloques, se toma el conjunto de columnas que representa el bloque transaccional y se transforman en registros de movimiento.

### Paso 4. Normalización y homologación
La información extraída se transforma a la estructura unificada del modelo mediante:
- estandarización de fechas,
- normalización de valores numéricos,
- limpieza de textos,
- preservación de códigos,
- homologación de campos a los nombres del modelo relacional.

En esta fase también se construye un dataframe específico con los beneficiarios identificados en los movimientos, para alimentar la dimensión correspondiente.

### Paso 5. Validación
Se aplican reglas mínimas de calidad y consistencia, tales como:
- obligatoriedad de campos,
- validación de dominios,
- consistencia básica entre campos relacionados,
- verificación de correspondencia con catálogos y dimensiones,
- detección de errores estructurales o de negocio.

Los errores y alertas deben registrarse con suficiente trazabilidad para revisión posterior.

### Paso 6. Generación de identificador técnico
Para garantizar unicidad por registro reportado, se genera un identificador técnico de forma determinística:

- `ID_MOV_CM` para movimientos de cuentas maestras
- `ID_MOV_CMP` para movimientos de cuentas maestras pagadoras

La lógica definida para ello parte de la combinación:
- `NOMBRE_ARCHIVO + CONSECUTIVO_FILA`

Esto permite rastrear cada movimiento hasta su origen exacto dentro del archivo fuente.

### Paso 7. Carga de beneficiarios
Antes de la carga de movimientos, se inserta o actualiza la información de beneficiarios identificados en el archivo sobre `DIM_BENEFICIARIOS`, respetando las reglas de unicidad y consistencia definidas para esa dimensión.

### Paso 8. Carga a tablas FACT
Se insertan únicamente los movimientos válidos en:
- `FACT_MOVIMIENTOS_CM`, o
- `FACT_MOVIMIENTOS_CMP`

según corresponda.

La carga debe conservar evidencia suficiente para:
- auditoría,
- revisión de errores,
- reconstrucción del proceso,
- análisis posterior.

### Paso 9. Trazabilidad de ejecución
Al finalizar el proceso, se registra un resumen con:
- archivo procesado,
- tipo de estructura identificada,
- periodo reportado,
- total de registros detectados,
- total de movimientos cargados,
- total de registros rechazados,
- resumen de errores y alertas.

Cuando aplique, también se genera un reporte de errores para revisión manual o control de calidad.

---

## 5. Buenas prácticas del proceso ETL

- Registrar fecha, hora, archivo y origen de cada ejecución.
- Conservar trazabilidad entre archivo fuente, fila origen e identificador técnico cargado.
- Preservar códigos como texto cuando exista riesgo de alteración por conversión automática.
- Separar errores de:
  - estructura,
  - calidad,
  - consistencia,
  - negocio.
- No realizar cargas silenciosas con registros inválidos.
- Validar primero catálogos y dimensiones antes de cargar hechos.
- Mantener homologaciones explícitas entre estructuras históricas y estructura 2023.
- Garantizar que cada movimiento cargado pueda rastrearse hasta su archivo y registro fuente.

---

## 6. Resultado esperado del ETL

La estrategia ETL del proyecto permite:
- integrar reportes de distintos años y estructuras normativas,
- transformar información heterogénea en una estructura unificada,
- preservar consistencia entre dimensiones y hechos,
- habilitar trazabilidad completa de cada movimiento,
- soportar validaciones y análisis de riesgo,
- facilitar auditoría técnica y revisión posterior.

En conjunto, este flujo constituye la base operativa para que el modelo relacional del proyecto funcione como una fuente confiable de información para control, seguimiento y análisis.

---

## 7. Observación de evolución futura
Aunque el flujo ya contempla trazabilidad de ejecución, el modelo puede fortalecerse en versiones posteriores mediante tablas específicas como:
- `LOG_CARGA_ETL`
- `LOG_ERROR_ETL`
- `FACT_ALERTA_VALIDACION`

Estas estructuras permitirían desacoplar mejor la evidencia técnica del proceso, las reglas de validación y las alertas derivadas del análisis.

## 8. Estado actual de implementación en notebooks

Actualmente, el proyecto ya cuenta con notebooks específicos que materializan parte del flujo ETL descrito en este documento.

### Mini-ETL implementados para tablas DIM
Se dispone de notebooks para cargas o actualizaciones controladas de:
- `DIM_BANCOS`
- `DIM_TITULARES`
- `DIM_ENTIDADES_TERRITORIALES`
- `DIM_RESGUARDOS`
- `DIM_CUENTAS_CM`
- `DIM_CUENTAS_CMP`

Estos procesos siguen una lógica común de:
- selección de archivo fuente,
- lectura como texto para preservar códigos,
- normalización de columnas,
- validaciones mínimas,
- detección de duplicados cuando aplica,
- carga o actualización mediante `MERGE` o lógica equivalente.

### ETL implementado para hechos
Se cuenta con un notebook para la carga de:
- `DIM_BENEFICIARIOS`
- `FACT_MOVIMIENTOS_CM`

Este proceso:
- lee archivos `.txt`,
- identifica la estructura del reporte,
- soporta estructuras históricas y estructura 2023,
- extrae movimientos,
- deriva beneficiarios desde movimientos,
- valida datos,
- genera `ID_MOV_CM` de forma determinística,
- realiza carga mediante staging y `MERGE`,
- exporta resúmenes y reportes de ejecución.

### Proceso auxiliar identificado
También existe un notebook auxiliar orientado a identificar Cuentas Maestras no registradas, a partir de registros tipo 2 de archivos fuente, para apoyar procesos de completitud, contraste y eventual backfill de `DIM_CUENTAS_CM`.

### Alcance actual
Con base en los notebooks revisados, el flujo ETL implementado se encuentra más avanzado en:
- mini-ETL de dimensiones,
- carga de hechos para movimientos CM,
- derivación de beneficiarios,
- procesos auxiliares de completitud de cuentas.

La implementación equivalente para `FACT_MOVIMIENTOS_CMP` deberá documentarse en este archivo cuando se consolide su notebook o script definitivo.