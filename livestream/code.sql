--FOR DATA FACTORY PIPELINE
SELECT S.name AS SchemaName,
		T.name AS TableName
FROM sys.tables T
INNER JOIN sys.schemas S ON S.schema_id = T.schema_id
WHERE S.name = 'SalesLT'

