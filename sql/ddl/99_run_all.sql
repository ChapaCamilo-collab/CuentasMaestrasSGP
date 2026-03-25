/*
99_run_all.sql
Ejecución completa del modelo (DDL + cargas iniciales donde aplica).

IMPORTANTE (SSMS):
1) Abrir este archivo en SQL Server Management Studio.
2) Activar: Query > SQLCMD Mode
3) Ejecutar.
*/
USE TFM_SGP;
GO

:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\00_schema.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\DIM_TIPO_CM.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\DIM_TIPO_CMP.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\DIM_TIPO_CUENTA_BANCARIA.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\DIM_TIPO_ID_BENEFICIARIO.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\DIM_TIPO_MOVIMIENTO.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\DIM_TITULARES.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\DIM_BANCOS.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\DIM_ENTIDADES_TERRITORIALES.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\DIM_RESGUARDOS.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\REF_TIPOS_CM_CMP.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\REF_TIPO_MOV_TIPO_CM.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\DIM_BENEFICIARIOS.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\DIM_CUENTAS_CM.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\DIM_CUENTAS_CMP.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\FACT_MOVIMIENTOS_CM.sql"
:r "C:\Users\camil\OneDrive\MASTER\TFM\Data\Tablas_BD\SQL\FACT_MOVIMIENTOS_CMP.sql"

GO
