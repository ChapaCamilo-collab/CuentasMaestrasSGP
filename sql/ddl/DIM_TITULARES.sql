-- Tabla: DIM_TITULARES
CREATE TABLE sgp.DIM_TITULARES (
  TIPO_TITULAR       varchar(240) NOT NULL,
  NIT                varchar(9)   NOT NULL,
  DV                 varchar(1)   NOT NULL,
  RAZON_SOCIAL       varchar(240) NOT NULL,
  FECHA_CREACION     date         NOT NULL,
  FECHA_ACTUALIZACION date        NOT NULL,
  CONSTRAINT PK_DIM_TITULARES PRIMARY KEY (NIT)
);
GO