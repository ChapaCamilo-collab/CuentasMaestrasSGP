-- =========================================================
-- Tabla: sgp.DIM_TIPO_MOVIMIENTO (VERSIÓN ACTUALIZADA)
-- =========================================================
CREATE TABLE sgp.[DIM_TIPO_MOVIMIENTO] (
  [TIPO_MOVIMIENTO] varchar(3) NOT NULL,
  [DESCRIPCION_MOVIMIENTO] varchar(240) NULL,
  [CATEGORIA] varchar(240) NOT NULL,
  CONSTRAINT PK_DIM_TIPO_MOVIMIENTO PRIMARY KEY ([TIPO_MOVIMIENTO])
);
GO

-- Datos iniciales (cargar solo si la tabla está vacía)
IF NOT EXISTS (SELECT 1 FROM sgp.[DIM_TIPO_MOVIMIENTO])
BEGIN
    INSERT INTO sgp.[DIM_TIPO_MOVIMIENTO] ([TIPO_MOVIMIENTO], [DESCRIPCION_MOVIMIENTO], [CATEGORIA])
    VALUES
      (100, N'Ingreso', N'INGRESO'),
      (110, N'Rendimietos Financieros', N'INGRESO'),
      (120, N'Reintegros bancarios', N'INGRESO'),
      (210, N'Egreso Resguardos certificados', N'EGRESO'),
      (260, N'Egreso por costos o gastos bancarios Resguardos certificados', N'EGRESO'),
      (270, N'Egresos por medidas cautelares de embargo', N'EGRESO'),
      (310, N'Egreso Libre Inversión', N'EGRESO'),
      (311, N'Egreso Recreación y Deporte', N'EGRESO'),
      (312, N'Egreso Cultura', N'EGRESO'),
      (313, N'Egreso Libre Destinación', N'EGRESO'),
      (320, N'Egreso Asignación Alimentación Escolar, Municipios Ribereños, Resguardos Indígenas, Primera Infancia', N'EGRESO'),
      (321, N'Egreso Programa de Alimentación Escolar -  PAE.', N'EGRESO'),
      (360, N'Egreso por costos o gastos bancarios, cuando haya lugar, en este caso el beneficiario es el mismo banco', N'EGRESO'),
      (410, N'Egresos Gastos de Inversión en Acueducto, Alcantarillado y Aseo', N'EGRESO'),
      (420, N'Egresos para el pago de subsidios en Acueducto, Alcantarillado y Aseo', N'EGRESO'),
      (430, N'Egresos transferencias al PDA', N'EGRESO'),
      (440, N'Egresos para el servicio a la deuda', N'EGRESO'),
      (450, N'Egreso por costos o gastos bancarios, cuando haya lugar, en este caso el beneficiario es el mismo banco', N'EGRESO'),
      (460, N'Otros Egresos', N'EGRESO'),
      (470, N'Impuestos', N'EGRESO'),
      (480, N'Embargos', N'EGRESO'),
      (510, N'Egreso Prestación del servicio Nómina', N'EGRESO'),
      (520, N'Egreso contratación del servicio educativo', N'EGRESO'),
      (530, N'Egreso contratación de aseo y vigilancia', N'EGRESO'),
      (540, N'Egreso Gastos administrativos distintos de Nómina', N'EGRESO'),
      (550, N'Egreso NEE', N'EGRESO'),
      (560, N'Egreso Conectividad', N'EGRESO'),
      (570, N'Egreso Internados', N'EGRESO'),
      (580, N'Egreso mejoramiento de la Calidad', N'EGRESO'),
      (590, N'Egresos por medidas cautelares de embargo', N'EGRESO'),
      (500, N'Saldo Inicial ', N'SALDO INICIAL'),
      (600, N'Saldo Final', N'SALDO FINAL');
END
GO