IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'sgp')
EXEC('CREATE SCHEMA sgp');
GO
