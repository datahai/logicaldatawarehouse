DROP VIEW LDW.vwFactSales;
GO

CREATE VIEW LDW.vwFactSales
AS
SELECT *,
CAST(fct.filepath(3) AS DATE) AS SalesOrderPathDate
 FROM 
OPENROWSET 
(
    BULK 'conformed/facts/factsales/*/*/*/*.parquet',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'Parquet'
) AS fct