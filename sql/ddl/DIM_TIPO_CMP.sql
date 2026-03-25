-- Tabla: DIM_TIPO_CMP
CREATE TABLE sgp.[DIM_TIPO_CMP] (
  [TIPO_CMP] varchar(2) NOT NULL,
  [TIPO_CM] varchar(2) NOT NULL,
  [NOMBRE_TIPO_CMP] varchar(150) NOT NULL,
  CONSTRAINT PK_DIM_TIPO_CMP PRIMARY KEY ([TIPO_CMP]),
  CONSTRAINT FK_DIM_TIPO_CMP_TIPO_CM__DIM_TIPO_CM_TIPO_CM FOREIGN KEY ([TIPO_CM]) REFERENCES sgp.[DIM_TIPO_CM] ([TIPO_CM])
);
GO

-- Datos iniciales (cargar solo si la tabla está vacía)
IF NOT EXISTS (SELECT 1 FROM sgp.[DIM_TIPO_CMP])
BEGIN
    INSERT INTO sgp.[DIM_TIPO_CMP] ([TIPO_CMP], [TIPO_CM], [NOMBRE_TIPO_CMP])
    VALUES
      (N'AB', N'AP', N'Agua Potable y Saneamiento Básico'),
      (N'CD', N'CA', N'Cancelaciones'),
      (N'CG', N'GR', N'Gratuidad'),
      (N'LP', N'AE', N'Alimentación Escolar'),
      (N'MA', N'MT', N'Calidad Matrícula'),
      (N'NP', N'PI', N'Atención Integral a la Primera Infancia'),
      (N'PD', N'ED', N'Propósito General Entidades Descentralizadas'),
      (N'PE', N'SO', N'Prestación del Servicio distintos a Nómina Docente, Directiva Docente y Administrativa'),
      (N'PN', N'SN', N'Prestación del Servicio de Nómina Docente, Directiva Docente y Administrativa'),
      (N'PP', N'PG', N'Propósito General'),
      (N'PR', N'MR', N'Municipios Ribereños'),
      (N'PT', N'RI', N'Resguardos Indígenas Certificados'),
      (N'RP', N'RI', N'Resguardos Indígenas');
END
GO
