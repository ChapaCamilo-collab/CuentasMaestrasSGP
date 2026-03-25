# 09_convenciones_codigo

## Propósito
Este archivo define las convenciones mínimas de desarrollo para el proyecto `SGP-CuentasMaestras`, con el fin de mantener consistencia entre scripts SQL, notebooks, procesos ETL, herramientas analíticas y documentación técnica.

## SQL
- Usar T-SQL compatible con SQL Server.
- Referenciar siempre las tablas del modelo con el esquema `sgp`.
- Separar scripts por objetivo:
  - creación,
  - alteración,
  - carga semilla,
  - validación,
  - consultas analíticas.
- Nombrar constraints, índices y llaves de forma explícita.
- Comentar decisiones de diseño relevantes cuando no sean evidentes.
- Evitar cambios destructivos sin documentar impacto sobre PK, FK y ETL existentes.

## Python
- Mantener módulos pequeños, reutilizables y con responsabilidad clara.
- Separar funciones de:
  - lectura,
  - transformación,
  - validación,
  - carga,
  - exportación.
- Usar nombres de variables y funciones claros y orientados al negocio.
- Implementar manejo explícito de errores.
- Incorporar logging descriptivo en procesos ETL y analíticos.
- Validar antes de insertar en base de datos.
- No incrustar credenciales reales en el código.

## Notebooks
- Usar notebooks para exploración, pruebas controladas y prototipos de carga o análisis.
- Mantener una secuencia clara de secciones:
  - configuración,
  - lectura,
  - transformación,
  - validación,
  - carga o análisis,
  - exportación de resultados.
- Incluir títulos y comentarios suficientes para entender el flujo.
- Evitar dejar celdas obsoletas, duplicadas o sin uso.
- Cuando un notebook madure funcionalmente, considerar migrar la lógica a scripts o módulos reutilizables.

## ETL
- Preservar códigos como texto cuando exista riesgo de pérdida de información.
- Mantener trazabilidad entre archivo fuente, fila origen e identificador técnico generado.
- Separar errores de:
  - estructura,
  - calidad,
  - consistencia,
  - negocio.
- No realizar cargas silenciosas de registros inválidos.
- Registrar resumen de ejecución cuando aplique.
- Documentar la lógica de homologación entre estructuras históricas y estructura 2023.

## Herramientas analíticas
- Toda salida debe ser explicable y auditable.
- Las alertas o casos priorizados no deben presentarse como conclusiones definitivas.
- Los parámetros y umbrales deben mantenerse documentados.
- Las métricas calculadas deben poder reproducirse sobre la misma fuente de datos.
- Cada resultado debe conservar evidencia mínima suficiente para revisión posterior.

## Parámetros y configuración
- Centralizar parámetros ajustables cuando sea posible.
- Documentar umbrales utilizados en herramientas de alertas o concentración.
- Diferenciar claramente entre:
  - parámetros de conexión,
  - parámetros ETL,
  - parámetros analíticos.
- Preferir variables de entorno o archivos de configuración para valores sensibles o reutilizables.

## Nombres y trazabilidad
- Mantener consistencia con la nomenclatura del modelo:
  - `DIM_`
  - `FACT_`
  - `REF_`
  - `LOG_`
- Usar nombres de salida que incluyan contexto, periodo o fecha cuando aplique.
- Cuando se genere un identificador técnico, documentar su lógica de construcción.

## Documentación
- Toda decisión importante se documenta en `docs/decisiones_tecnicas.md`.
- El uso de IA se registra en `docs/uso_ia_tfm.md`.
- Los cambios relevantes en la arquitectura, ETL o herramientas analíticas deben reflejarse también en los archivos de contexto del proyecto.