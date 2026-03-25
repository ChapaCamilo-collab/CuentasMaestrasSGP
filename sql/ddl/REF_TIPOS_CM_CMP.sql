-- Tabla: REF_TIPOS_CM_CMP
-- Nota: Tabla de referencia normativa. Puede omitirse si el mapeo se gestiona exclusivamente con DIM_TIPO_CM y DIM_TIPO_CMP.
CREATE TABLE sgp.[REF_TIPOS_CM_CMP] (
  [SECTOR] varchar(240) NOT NULL,
  [NOMBRE_SECTOR] varchar(240) NOT NULL,
  [TIPO_CM] varchar(2) NOT NULL,
  [ASIGNACION] varchar(240) NOT NULL,
  [TIPO_CMP] varchar(2) NOT NULL
);
GO

-- Datos iniciales (cargar solo si la tabla está vacía)
IF NOT EXISTS (SELECT 1 FROM sgp.[REF_TIPOS_CM_CMP])
BEGIN
    INSERT INTO sgp.[REF_TIPOS_CM_CMP] ([SECTOR], [NOMBRE_SECTOR], [TIPO_CM], [ASIGNACION], [TIPO_CMP])
    VALUES
      (N'MEN', N'Ministerio de Educación Nacional', N'GR', N'Gratuidad', N'CG'),
      (N'MEN', N'Ministerio de Educación Nacional', N'SN', N'Prestación del Servicio de Nómina Docente, Directiva Docente y Administrativa', N'PN'),
      (N'MEN', N'Ministerio de Educación Nacional', N'CA', N'Cancelaciones', N'CD'),
      (N'MEN', N'Ministerio de Educación Nacional', N'MT', N'Calidad Matrícula', N'MA'),
      (N'MEN', N'Ministerio de Educación Nacional', N'SO', N'Prestación del Servicio distintos a Nómina Docente, Directiva Docente y Administrativa', N'PE'),
      (N'MHCP', N'Ministerio de Hacienda y Crédito Público', N'AE', N'Alimentación Escolar', N'LP'),
      (N'MHCP', N'Ministerio de Hacienda y Crédito Público', N'MR', N'Municipios Ribereños', N'PR'),
      (N'MHCP', N'Ministerio de Hacienda y Crédito Público', N'ED', N'Propósito General Entidades Descentralizadas', N'PD'),
      (N'MHCP', N'Ministerio de Hacienda y Crédito Público', N'PI', N'Atención Integral a la Primera Infancia', N'NP'),
      (N'MHCP', N'Ministerio de Hacienda y Crédito Público', N'PG', N'Propósito General', N'PP'),
      (N'MHCP', N'Ministerio de Hacienda y Crédito Público', N'RI', N'Resguardos Indígenas', N'RP'),
      (N'MHCP', N'Ministerio de Hacienda y Crédito Público', N'RI', N'Resguardos Indígenas', N'PT'),
      (N'MVCT', N'Ministerio de Vivienda Ciudad y Territorio', N'AP', N'Agua Potable y Saneamiento Básico', N'AB');
END
GO
