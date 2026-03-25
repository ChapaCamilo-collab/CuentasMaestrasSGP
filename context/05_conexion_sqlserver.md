# 05_conexion_sqlserver

## Motor de base de datos
SQL Server

## Base y esquema actuales
- Base de datos: `TFM_SGP`
- Esquema principal: `sgp`

## Acceso desde Python
- Librerías utilizadas o sugeridas:
  - `sqlalchemy`
  - `pyodbc`
  - `pandas`
  - `openpyxl` para lectura de archivos Excel de soporte
- Driver observado en la implementación actual:
  - `ODBC Driver 18 for SQL Server`
- Driver compatible alternativo:
  - `ODBC Driver 17 for SQL Server`

## Modalidad actual de desarrollo
En la implementación actual de los notebooks ETL, la conexión utilizada corresponde a un entorno local de desarrollo en SQL Server con autenticación integrada de Windows.

### Parámetros observados en los notebooks
- Base de datos: `TFM_SGP`
- Esquema: `sgp`
- Instancia: `localhost\TFM`
- Driver: `ODBC Driver 18 for SQL Server`
- Modo de autenticación: `trusted_connection=yes`
- Opción adicional: `TrustServerCertificate=yes`

### Observación importante
Aunque los notebooks actuales usan conexión local embebida en el código para facilitar el desarrollo y las pruebas, la recomendación para una versión más mantenible del proyecto es migrar esta configuración a variables de entorno o a un archivo de configuración centralizado.

## Variables de entorno esperadas para proximos ambientes
- `DB_SERVER`
- `DB_DATABASE`
- `DB_USERNAME`
- `DB_PASSWORD`
- `DB_DRIVER`
