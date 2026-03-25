# 00_resumen_proyecto

## Nombre de trabajo
SGP-CuentasMaestras

## Objetivo
Diseñar e implementar una base de datos relacional unificada que integre la información de Cuentas Maestras y Cuentas Maestras Pagadoras del Sistema General de Participaciones, con el fin de mejorar la trazabilidad de los recursos, facilitar el seguimiento de su uso y servir como base para la detección de operaciones no permitidas y el análisis de concentración de mercado.

## Problema que resuelve
Actualmente, la información asociada a las Cuentas Maestras y Cuentas Maestras Pagadoras del SGP se encuentra distribuida en archivos planos, anexos técnicos, validaciones operativas y estructuras no homogéneas. Esta fragmentación dificulta la consolidación de los datos, incrementa la dependencia de procesos manuales de homologación y limita la capacidad para realizar análisis oportunos, comparables y trazables sobre el uso de los recursos públicos. Como consecuencia, se reduce la posibilidad de identificar de forma temprana inconsistencias, patrones atípicos y situaciones de riesgo relevantes para el seguimiento técnico y analítico.

## Componentes principales
- Modelo relacional normalizado en SQL Server.
- Tablas DIM, FACT y REF para consolidar cuentas, titulares, beneficiarios, movimientos y correspondencias normativas.
- ETL y validaciones en Python para la ingesta, homologación, limpieza y carga de archivos `.txt`.
- Diccionario de datos, glosario y artefactos de contexto para documentar reglas, estructuras y definiciones del modelo.
- Herramientas analíticas orientadas a trazabilidad, detección de operaciones no permitidas y análisis de concentración.
- Marco normativo consolidado para Cuentas Maestras, Cuentas Maestras para Pagos Electrónicos - PSE y Programa de Alimentación Escolar - PAE.

## Resultados esperados
- Base de datos consolidada, consistente y preparada para consultas analíticas.
- Integración homogénea de diferentes estructuras normativas de reporte en un modelo unificado.
- Soporte técnico para automatizar la carga y validación de información reportada por las entidades financieras.
- Insumos confiables para el análisis del uso adecuado de los recursos del SGP.
- Base metodológica y técnica para desarrollar herramientas de detección de riesgos y para sustentar la memoria del TFM.
