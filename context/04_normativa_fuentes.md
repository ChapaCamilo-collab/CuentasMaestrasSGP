# 04_normativa_fuentes

## Propósito del archivo
Este documento consolida las fuentes normativas principales que soportan el diseño funcional y técnico del proyecto **SGP-CuentasMaestras**. Su objetivo es servir como referencia para la definición del modelo de datos, las reglas de negocio, las validaciones en Python y la trazabilidad de decisiones sobre Cuentas Maestras, Cuentas Maestras para Pagos Electrónicos - PSE y el manejo de recursos del Sistema General de Participaciones.

## Criterio de uso normativo en el proyecto
Para efectos del desarrollo del modelo de base de datos y de las reglas de validación, se adopta el siguiente criterio:

- La **Resolución 2394 de 2023** se toma como el **marco normativo consolidado principal** para las reglas vigentes de apertura, registro, operación, cancelación, sustitución, beneficiarios, cuentas PSE y manejo del PAE.
- Las **Resoluciones 3841 de 2015, 4835 de 2015 y 0660 de 2018** se conservan como **antecedentes normativos y fuente de contexto histórico y funcional**, útiles para:
  - entender la evolución regulatoria del producto financiero,
  - justificar la estructura conceptual del modelo,
  - interpretar anexos técnicos y nomenclaturas previas,
  - rastrear el origen de ciertos tipos de cuenta y reglas operativas.

## Normas principales incluidas

### 1. Resolución 3841 de 2015
**Entidad expedidora:** Ministerio de Hacienda y Crédito Público.  
**Objeto general:** reglamenta las Cuentas Maestras para la administración de los recursos de la Asignación Especial del Sistema General de Participaciones para Resguardos Indígenas - AESGPRI por parte de los Territorios Indígenas certificados, los Resguardos Indígenas y las asociaciones que estos conformen.

**Aporte principal al proyecto:**
- Define la Cuenta Maestra para AESGPRI como una cuenta bancaria sujeta a operaciones electrónicas.
- Establece reglas sobre:
  - apertura,
  - convenio con la entidad bancaria,
  - operaciones autorizadas,
  - beneficiarios,
  - inscripción de beneficiarios,
  - registro,
  - sustitución,
  - reporte de información.
- Es la base conceptual del componente de **Resguardos Indígenas** dentro del modelo del proyecto.

**Impacto en el modelo de datos:**
- Justifica la existencia de estructuras asociadas a:
  - resguardos,
  - titulares,
  - cuentas maestras RI,
  - beneficiarios inscritos,
  - movimientos de ingresos y egresos.
- Soporta la necesidad de identificar:
  - tipo de cuenta,
  - titular,
  - beneficiario,
  - banco,
  - operación autorizada,
  - registro y reporte.

### 2. Resolución 4835 de 2015
**Entidad expedidora:** Ministerio de Hacienda y Crédito Público.  
**Objeto general:** reglamenta las Cuentas Maestras de las entidades territoriales y sus entidades descentralizadas para la administración de los recursos del Sistema General de Participaciones de Propósito General, las Asignaciones Especiales y la Asignación para la Atención Integral a la Primera Infancia.

**Aporte principal al proyecto:**
- Extiende la regulación de Cuentas Maestras al nivel de:
  - entidades territoriales,
  - entidades descentralizadas,
  - Propósito General,
  - Alimentación Escolar,
  - Municipios Ribereños,
  - Resguardos Indígenas administrados por entidades territoriales,
  - Atención Integral a la Primera Infancia.
- Define:
  - apertura de cuentas por tipo de recurso,
  - nomenclatura,
  - operaciones crédito y débito autorizadas,
  - beneficiarios,
  - inscripción de beneficiarios,
  - registro,
  - sustitución,
  - reporte de información.

**Impacto en el modelo de datos:**
- Es una de las bases del diseño de:
  - `DIM_TIPO_CM`,
  - `DIM_CUENTAS_CM`,
  - `DIM_TITULARES`,
  - `DIM_ENTIDADES_TERRITORIALES`,
  - `DIM_BENEFICIARIOS`,
  - `FACT_MOVIMIENTOS_CM`.
- Justifica la necesidad de modelar tipos de cuenta como:
  - PG,
  - AE,
  - MR,
  - RI,
  - AIPI,
  - y cuentas asociadas a entidades descentralizadas.
- Sustenta la lógica de una cuenta maestra por fuente o por tipo de asignación, según corresponda.

### 3. Resolución 0660 de 2018
**Entidad expedidora:** Ministerios de Hacienda y Crédito Público, Educación Nacional y Vivienda, Ciudad y Territorio.  
**Objeto general:** reglamenta las Cuentas Maestras Pagadoras y las Cuentas de Manejo de Garantías de las Participaciones de Agua Potable y Saneamiento Básico, Educación, Propósito General, las Asignaciones Especiales y la Asignación para la Atención Integral a la Primera Infancia del Sistema General de Participaciones.

**Aporte principal al proyecto:**
- Introduce la figura de la **Cuenta Maestra Pagadora** como cuenta complementaria de la Cuenta Maestra.
- Regula pagos por **Botón de Pago Electrónico Seguro en Línea - PSE**.
- Define:
  - apertura de cuentas pagadoras,
  - convenios,
  - nomenclatura,
  - operaciones crédito y débito autorizadas,
  - reporte,
  - responsabilidad,
  - cuentas de manejo de garantías para servicio de la deuda.
- Resuelve el problema operativo de pagos que no admiten preinscripción tradicional de beneficiarios.

**Impacto en el modelo de datos:**
- Justifica la existencia del componente CMP/PSE en el proyecto.
- Soporta el diseño de:
  - `DIM_TIPO_CMP`,
  - `DIM_CUENTAS_CMP`,
  - `FACT_MOVIMIENTOS_CMP`,
  - tablas de referencia entre tipos de CM y CMP.
- Sustenta reglas específicas para:
  - pagos PSE autorizados,
  - operaciones no autorizadas,
  - cuentas subsidiarias,
  - trazabilidad del origen de fondos desde una Cuenta Maestra principal.
- También aporta contexto para una futura tabla relacionada con **garantías o deuda**, si el alcance del proyecto se amplía.

### 4. Resolución 2394 de 2023
**Entidad expedidora:** Ministerios de Hacienda y Crédito Público, Educación Nacional, Vivienda, Ciudad y Territorio y la Unidad Administrativa Especial de Alimentación Escolar - Alimentos para Aprender.  
**Objeto general:** fija las condiciones de apertura, registro, operación, cancelación y sustitución de las Cuentas Maestras y de las Cuentas Maestras para Pagos Electrónicos - PSE en las que se administran recursos del Sistema General de Participaciones y, en el sector Educación, las demás fuentes del Programa de Alimentación Escolar - PAE.

**Aporte principal al proyecto:**
- Consolida en un solo marco regulatorio la operación de:
  - Cuentas Maestras,
  - Cuentas Maestras para Pagos Electrónicos - PSE,
  - cuentas asociadas al PAE.
- Amplía y unifica reglas sobre:
  - apertura,
  - registro,
  - operación,
  - cancelación,
  - sustitución,
  - beneficiarios,
  - beneficiarios especiales,
  - inscripción documental,
  - manejo de PAE,
  - reglas de PSE,
  - reportes.
- Redefine la vigencia práctica del marco regulatorio del proyecto y debe ser tomada como referencia principal para reglas actuales.

**Impacto en el modelo de datos:**
- Es la norma más importante para alinear el modelo físico con la regulación vigente.
- Refuerza la necesidad de modelar:
  - catálogos de tipos de cuenta,
  - beneficiarios ordinarios y especiales,
  - cuentas PSE como producto complementario,
  - relaciones entre cuentas maestras y cuentas PSE,
  - manejo independiente de fuentes del PAE,
  - reglas de operaciones crédito, débito y operaciones especiales,
  - componentes territoriales, sectoriales y por asignación.
- Es la fuente principal para el futuro catálogo de reglas de negocio y de validaciones explicables del proyecto.

## Relación entre las normas y el proyecto

### Normas base para la estructura de Cuentas Maestras
- Resolución 3841 de 2015
- Resolución 4835 de 2015

Estas normas explican el origen de la estructura de Cuentas Maestras para distintos tipos de titular y asignación, y justifican la existencia de cuentas separadas por fuente de recurso.

### Norma base para pagos electrónicos y cuentas complementarias
- Resolución 0660 de 2018

Esta norma explica la necesidad funcional de las cuentas pagadoras y su lógica de operación por PSE, elemento fundamental para el diseño del componente CMP del proyecto.

### Norma consolidada vigente
- Resolución 2394 de 2023

Esta resolución debe orientar la mayor parte de las reglas funcionales vigentes del modelo, especialmente cuando exista diferencia entre la regulación anterior y la consolidada.

## Implicaciones directas para SQL y Python

### En SQL Server
Estas normas justifican modelar, como mínimo:
- tipos de cuenta maestra y tipos de cuenta para pagos electrónicos,
- cuentas maestras y cuentas complementarias,
- titulares y beneficiarios,
- movimientos,
- bancos,
- sectores, asignaciones y relaciones entre tipos de cuenta,
- estructuras futuras para reglas normativas y alertas.

### En Python
Estas normas justifican construir validaciones sobre:
- apertura y clasificación de cuentas,
- consistencia entre tipo de cuenta y tipo de movimiento,
- correspondencia entre cuenta principal y cuenta PSE,
- beneficiarios inscritos,
- operaciones crédito y débito autorizadas,
- restricciones por sector y asignación,
- reglas documentales de registro y reporte.

## Recomendación de uso en el repositorio
Cuando el asistente o cualquier script del proyecto deba proponer reglas de negocio, validaciones o cambios de estructura, deberá aplicar esta jerarquía:

1. Revisar primero la **Resolución 2394 de 2023** como fuente principal.
2. Acudir a las Resoluciones **3841 de 2015**, **4835 de 2015** y **0660 de 2018** como antecedentes para interpretar el origen de productos, nomenclaturas o anexos técnicos históricos.
3. Cuando exista una diferencia entre la norma histórica y la norma consolidada, se debe priorizar la norma consolidada vigente, salvo que el análisis sea histórico o comparativo.

## Observación para el TFM
Este archivo no reemplaza la redacción académica de la memoria, sino que funciona como contexto técnico y normativo para asistentes de IA, diseño del modelo relacional y construcción de validaciones. En la memoria del TFM, estas normas deberán desarrollarse con un estilo expositivo y argumentativo, mientras que aquí se documentan con enfoque operativo.