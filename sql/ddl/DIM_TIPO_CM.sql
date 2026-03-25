-- Tabla: DIM_TIPO_CM
CREATE TABLE sgp.[DIM_TIPO_CM] (
  [TIPO_CM] varchar(2) NOT NULL,
  [NOMBRE_TIPO_CM] varchar(150) NOT NULL,
  CONSTRAINT PK_DIM_TIPO_CM PRIMARY KEY ([TIPO_CM])
);
GO

-- Datos iniciales (cargar solo si la tabla está vacía)
IF NOT EXISTS (SELECT 1 FROM sgp.[DIM_TIPO_CM])
BEGIN
    INSERT INTO sgp.[DIM_TIPO_CM] ([TIPO_CM], [NOMBRE_TIPO_CM])
    VALUES
      (N'AE', N'Alimentación Escolar'),
      (N'AP', N'Agua Potable y Saneamiento Básico'),
      (N'CA', N'Cancelaciones'),
      (N'ED', N'Propósito General Entidades Descentralizadas'),
      (N'GR', N'Gratuidad'),
      (N'MR', N'Municipios Ribereños'),
      (N'MT', N'Calidad Matrícula'),
      (N'PG', N'Propósito General'),
      (N'PI', N'Atención Integral a la Primera Infancia'),
      (N'RI', N'Resguardos Indígenas'),
      (N'SN', N'Prestación del Servicio de Nómina Docente, Directiva Docente y Administrativa'),
      (N'SO', N'Prestación del Servicio distintos a Nómina Docente, Directiva Docente y Administrativa');
END
GO
