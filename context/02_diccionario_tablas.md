# 02_diccionario_tablas

## Alcance
Este archivo documenta el inventario actual de tablas creadas en la base de datos `TFM_SGP`, esquema `sgp`, a partir de los scripts DDL del proyecto. Su propósito es servir como mapa funcional y técnico para asistentes de IA, desarrollo ETL, validaciones y documentación del TFM.

---

## DIM_BANCOS
- Propósito: catálogo maestro de entidades financieras habilitadas o referenciadas en el modelo.
- Tipo: DIM
- Llave primaria: `ID_BANCO`
- Llaves alternas / unicidad:
  - `COD_ACH` único
- Llaves foráneas: no aplica
- Columnas clave:
  - `ID_BANCO`
  - `COD_ACH`
  - `NIT_BANCO`
  - `NOMBRE_BANCO`
  - `RAZON_SOCIAL`
  - `ESTADO_CM`
- Fuente de carga: datos semilla
- Observaciones:
  - `ID_BANCO` se construye como combinación de `NIT_BANCO` + `COD_ACH`.
  - `COD_ACH` se usa como referencia en otras tablas.
  - Mantener formato texto para preservar ceros a la izquierda.

## DIM_TITULARES
- Propósito: catálogo de titulares de Cuentas Maestras y Cuentas Maestras Pagadoras.
- Tipo: DIM
- Llave primaria: `NIT`
- Llaves foráneas: no aplica
- Columnas clave:
  - `TIPO_TITULAR`
  - `NIT`
  - `DV`
  - `RAZON_SOCIAL`
- Fuente de carga: mini-ETL / maestro de titulares
- Observaciones:
  - Es la referencia principal para vincular titulares con cuentas.
  - Actualmente la PK es solo `NIT`.

## DIM_ENTIDADES_TERRITORIALES
- Propósito: tabla maestra para análisis territoriales y cruces geográficos.
- Tipo: DIM
- Llave primaria: `NIT`
- Llaves foráneas: no aplica
- Columnas clave:
  - `NIT`
  - `TIPO_TITULAR`
  - `DIVIPOLA`
  - `COD_DEPARTAMENTO`
  - `COD_MUNICIPIO`
  - `NOMBRE_DEPARTAMENTO`
  - `NOMBRE_MUNICIPIO`
  - `LATITUD`
  - `LONGITUD`
  - `UBICACIÓN`
- Fuente de carga: mini-ETL / maestros territoriales
- Observaciones:
  - Se usa para análisis territorial, no como FK directa en las tablas de hechos actuales.
  - `DIVIPOLA` se conserva como texto.

## DIM_RESGUARDOS
- Propósito: catálogo de resguardos indígenas y su relación territorial y de titularidad.
- Tipo: DIM
- Llave primaria: `COD_RESGUARDO`
- Llaves foráneas: no aplica
- Columnas clave:
  - `COD_RESGUARDO`
  - `RESGUARDO_NOMBRE`
  - `COD_DEPARTAMENTO`
  - `COD_MUNICIPIO`
  - `NIT_TITULAR`
- Fuente de carga: mini-ETL / maestro de resguardos
- Observaciones:
  - Es referenciada por `DIM_CUENTAS_CM` y `DIM_CUENTAS_CMP` cuando aplica.
  - Incluye fechas de creación y actualización.

## DIM_TIPO_CM
- Propósito: catálogo de tipos de Cuenta Maestra definidos en la normatividad.
- Tipo: DIM
- Llave primaria: `TIPO_CM`
- Llaves foráneas: no aplica
- Columnas clave:
  - `TIPO_CM`
  - `NOMBRE_TIPO_CM`
- Fuente de carga: datos semilla
- Observaciones:
  - Define dominios como `PG`, `AE`, `RI`, `PI`, `ED`, `AP`, `SN`, `SO`, `MT`, `CA`, `GR`.

## DIM_TIPO_CMP
- Propósito: catálogo de tipos de Cuenta Maestra Pagadora.
- Tipo: DIM
- Llave primaria: `TIPO_CMP`
- Llaves foráneas:
  - `TIPO_CM` -> `DIM_TIPO_CM.TIPO_CM`
- Columnas clave:
  - `TIPO_CMP`
  - `TIPO_CM`
  - `NOMBRE_TIPO_CMP`
- Fuente de carga: datos semilla
- Observaciones:
  - Permite relacionar cada tipo CMP con su tipo CM asociado.

## DIM_TIPO_MOVIMIENTO
- Propósito: catálogo de tipos de movimiento reportados en CM y CMP.
- Tipo: DIM
- Llave primaria: `TIPO_MOVIMIENTO`
- Llaves foráneas: no aplica
- Columnas clave:
  - `TIPO_MOVIMIENTO`
  - `DESCRIPCION_MOVIMIENTO`
  - `CATEGORIA`
- Fuente de carga: datos semilla
- Observaciones:
  - Incluye ingresos, egresos y saldos.
  - Es una dimensión crítica para reglas de validación.

## DIM_TIPO_CUENTA_BANCARIA
- Propósito: catálogo de tipos de cuenta bancaria.
- Tipo: DIM
- Llave primaria: `TIPO_CUENTA_BANCARIA`
- Llaves foráneas: no aplica
- Columnas clave:
  - `TIPO_CUENTA_BANCARIA`
  - `TIPO_CUENTA_BANCARIA_DESCRIPCION`
- Fuente de carga: datos semilla
- Observaciones:
  - Actualmente maneja dominios como ahorro y corriente.

## DIM_TIPO_ID_BENEFICIARIO
- Propósito: catálogo de tipos de identificación de beneficiarios.
- Tipo: DIM
- Llave primaria: `TIPO_IDENTIFICACION_BENEFICIARIO`
- Llaves foráneas: no aplica
- Columnas clave:
  - `TIPO_IDENTIFICACION_BENEFICIARIO`
  - `TIPO_ID_BENEFICIARIO`
- Fuente de carga: datos semilla
- Observaciones:
  - Se usa en `DIM_BENEFICIARIOS`, `FACT_MOVIMIENTOS_CM` y `FACT_MOVIMIENTOS_CMP`.

## DIM_BENEFICIARIOS
- Propósito: catálogo de beneficiarios inscritos y sus datos bancarios asociados.
- Tipo: DIM
- Llave primaria:
  - compuesta por `ID_BENEFICIARIO`, `CODIGO_BANCO`, `CUENTA_BENEFICIARIO`, `RAZON_SOCIAL`
- Llaves foráneas:
  - `TIPO_ID_BENEFICIARIO` -> `DIM_TIPO_ID_BENEFICIARIO.TIPO_IDENTIFICACION_BENEFICIARIO`
  - `CODIGO_BANCO` -> `DIM_BANCOS.COD_ACH`
- Columnas clave:
  - `TIPO_ID_BENEFICIARIO`
  - `ID_BENEFICIARIO`
  - `DV`
  - `RAZON_SOCIAL`
  - `CODIGO_BANCO`
  - `CUENTA_BENEFICIARIO`
  - `TIPO_CUENTA_BENEFICIARIO`
- Fuente de carga: mini-ETL / consolidado de beneficiarios
- Observaciones:
  - La granularidad real del beneficiario depende de la combinación identificación + banco + cuenta + razón social.
  - Se usa directamente como referencia en `FACT_MOVIMIENTOS_CM`.

## DIM_CUENTAS_CM
- Propósito: inventario unificado de Cuentas Maestras.
- Tipo: DIM
- Llave primaria: `NUMERO_CM`
- Llaves foráneas:
  - `COD_RESGUARDO` -> `DIM_RESGUARDOS.COD_RESGUARDO`
  - `NIT_TITULAR` -> `DIM_TITULARES.NIT`
  - `TIPO_CM` -> `DIM_TIPO_CM.TIPO_CM`
  - `ID_BANCO` -> `DIM_BANCOS.ID_BANCO`
- Columnas clave:
  - `DIVIPOLA`
  - `COD_DEPARTAMENTO`
  - `COD_MUNICIPIO`
  - `COD_RESGUARDO`
  - `NIT_TITULAR`
  - `TIPO_TITULAR`
  - `SECTOR`
  - `RUBRO`
  - `TIPO_CM`
  - `NUMERO_CM`
  - `TIPO_CUENTA`
  - `NIT_BANCO`
  - `CODIGO_ACH_BANCO`
  - `ID_BANCO` (columna calculada persistida)
- Fuente de carga: mini-ETL / maestro de cuentas
- Observaciones:
  - `ID_BANCO` se calcula a partir de `NIT_BANCO` + `CODIGO_ACH_BANCO`.
  - Es la dimensión central para los movimientos de CM.

## DIM_CUENTAS_CMP
- Propósito: inventario unificado de Cuentas Maestras Pagadoras.
- Tipo: DIM
- Llave primaria: `NUMERO_CMP`
- Llaves foráneas:
  - `COD_RESGUARDO` -> `DIM_RESGUARDOS.COD_RESGUARDO`
  - `NIT_TITULAR` -> `DIM_TITULARES.NIT`
  - `TIPO_CMP` -> `DIM_TIPO_CMP.TIPO_CMP`
  - `NUMERO_CM_PRINCIPAL` -> `DIM_CUENTAS_CM.NUMERO_CM`
  - `ID_BANCO` -> `DIM_BANCOS.ID_BANCO`
- Columnas clave:
  - `DIVIPOLA`
  - `COD_DEPARTAMENTO`
  - `COD_MUNICIPIO`
  - `COD_RESGUARDO`
  - `NIT_TITULAR`
  - `TIPO_TITULAR`
  - `SECTOR`
  - `RUBRO`
  - `TIPO_CMP`
  - `NUMERO_CMP`
  - `NUMERO_CM_PRINCIPAL`
  - `TIPO_CUENTA`
  - `NIT_BANCO`
  - `CODIGO_ACH_BANCO`
  - `ID_BANCO` (columna calculada persistida)
- Fuente de carga: mini-ETL / maestro de cuentas pagadoras
- Observaciones:
  - Mantiene vínculo explícito con la cuenta maestra principal.
  - Es la dimensión central para los movimientos de CMP.

## FACT_MOVIMIENTOS_CM
- Propósito: registrar movimientos de Cuentas Maestras.
- Tipo: FACT
- Llave primaria: `ID_MOV_CM`
- Llaves foráneas:
  - `TIPO_CM` -> `DIM_TIPO_CM.TIPO_CM`
  - `NUMERO_CM` -> `DIM_CUENTAS_CM.NUMERO_CM`
  - `TIPO_MOVIMIENTO` -> `DIM_TIPO_MOVIMIENTO.TIPO_MOVIMIENTO`
  - `TIPO_ID_BENEFICIARIO` -> `DIM_TIPO_ID_BENEFICIARIO.TIPO_IDENTIFICACION_BENEFICIARIO`
  - (`ID_BENEFICIARIO`, `CODIGO_BANCO`, `CUENTA_BENEFICIARIO`, `RAZON_SOCIAL`) -> `DIM_BENEFICIARIOS`
  - `CODIGO_BANCO` -> `DIM_BANCOS.COD_ACH`
- Columnas clave:
  - `ID_MOV_CM`
  - `TIPO_CM`
  - `NUMERO_CM`
  - `FECHA_MOVIMIENTO`
  - `TIPO_MOVIMIENTO`
  - `DESCRIPCION_MOVIMIENTO`
  - `VALOR`
  - `TIPO_ID_BENEFICIARIO`
  - `ID_BENEFICIARIO`
  - `DV`
  - `RAZON_SOCIAL`
  - `CODIGO_BANCO`
  - `CUENTA_BENEFICIARIO`
  - `TIPO_CUENTA_BENEFICIARIO`
- Fuente de carga: ETL periódico de movimientos CM
- Observaciones:
  - Es la tabla transaccional principal para trazabilidad y análisis de uso de recursos.
  - El `ID_MOV_CM` debe generarse de forma determinística en ETL.

## FACT_MOVIMIENTOS_CMP
- Propósito: registrar movimientos de Cuentas Maestras Pagadoras.
- Tipo: FACT
- Llave primaria: `ID_MOV_CMP`
- Llaves foráneas:
  - `TIPO_CMP` -> `DIM_TIPO_CMP.TIPO_CMP`
  - `NUMERO_CMP` -> `DIM_CUENTAS_CMP.NUMERO_CMP`
  - `NUMERO_CM_PRINCIPAL` -> `DIM_CUENTAS_CM.NUMERO_CM`
  - `TIPO_MOVIMIENTO` -> `DIM_TIPO_MOVIMIENTO.TIPO_MOVIMIENTO`
  - `TIPO_ID_BENEFICIARIO` -> `DIM_TIPO_ID_BENEFICIARIO.TIPO_IDENTIFICACION_BENEFICIARIO`
- Columnas clave:
  - `ID_MOV_CMP`
  - `TIPO_CMP`
  - `NUMERO_CMP`
  - `NUMERO_CM_PRINCIPAL`
  - `FECHA_MOVIMIENTO`
  - `TIPO_MOVIMIENTO`
  - `DESCRIPCION_MOVIMIENTO`
  - `VALOR`
  - `TIPO_ID_BENEFICIARIO`
  - `ID_BENEFICIARIO`
  - `DV`
  - `RAZON_SOCIAL`
  - `CODIGO_SERVICIO`
  - `CUS`
- Fuente de carga: ETL periódico de movimientos CMP / PSE
- Observaciones:
  - Es la tabla transaccional principal para pagos electrónicos y su trazabilidad.
  - Mantiene vínculo tanto con la cuenta pagadora como con la cuenta maestra principal.

## REF_TIPOS_CM_CMP
- Propósito: matriz de correspondencia entre sector, tipo de CM, asignación y tipo de CMP.
- Tipo: REF
- Llave primaria: no definida en el DDL actual
- Llaves foráneas: no definidas en el DDL actual
- Columnas clave:
  - `SECTOR`
  - `NOMBRE_SECTOR`
  - `TIPO_CM`
  - `ASIGNACION`
  - `TIPO_CMP`
- Fuente de carga: datos semilla
- Observaciones:
  - Es una tabla de referencia normativa y de homologación.
  - Sería recomendable definir una clave técnica o una restricción de unicidad en una versión futura.

## REF_TIPO_MOV_TIPO_CM
- Propósito: relacionar tipos de movimiento permitidos con tipo de CM y sector.
- Tipo: REF
- Llave primaria: no definida en el DDL actual
- Llaves foráneas:
  - `TIPO_CM` -> `DIM_TIPO_CM.TIPO_CM`
- Columnas clave:
  - `TIPO_CM`
  - `SECTOR`
  - `TIPO_MOVIMIENTO`
  - `DESCRIPCION_MOVIMIENTO`
- Fuente de carga: datos semilla
- Observaciones:
  - Funciona como tabla base para validar movimientos autorizados por tipo de cuenta.
  - Sería recomendable definir clave técnica o unicidad en una versión futura.
