-- Tabla: DIM_TIPO_ID_BENEFICIARIO
CREATE TABLE sgp.[DIM_TIPO_ID_BENEFICIARIO] (
  [TIPO_IDENTIFICACION_BENEFICIARIO] varchar(2) NOT NULL,
  [TIPO_ID_BENEFICIARIO] varchar(240) NOT NULL,
  CONSTRAINT PK_DIM_TIPO_ID_BENEFICIARIO PRIMARY KEY ([TIPO_IDENTIFICACION_BENEFICIARIO])
);
GO

-- Datos iniciales (cargar solo si la tabla está vacía)
IF NOT EXISTS (SELECT 1 FROM sgp.[DIM_TIPO_ID_BENEFICIARIO])
BEGIN
    INSERT INTO sgp.[DIM_TIPO_ID_BENEFICIARIO] ([TIPO_IDENTIFICACION_BENEFICIARIO], [TIPO_ID_BENEFICIARIO])
    VALUES
      (N'NI', N'Número de Identificación Tributaria'),
      (N'CC', N'Cedula de Ciudadanía'),
      (N'CE', N'Cedula de Extranjería'),
      (N'EP', N'Permiso Especial de Permanencia');
END
GO
