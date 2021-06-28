--Manual CETAS to load new Sales Order data
CREATE EXTERNAL TABLE STG.FactSales
WITH 
(
  LOCATION = 'conformed/facts/factsales/incremental/2021-04-18',
  DATA_SOURCE = ExternalDataSourceDataLake,
  FILE_FORMAT = SynapseParquetFormat
) 
AS
SELECT  
  --Surrogate Keys 
    DC.CustomerKey,
    --CAST(FORMAT(SO.OrderDate,'yyyyMMdd') AS INT) as OrderDateKey,
    SO.OrderDate,
    DSI.StockItemKey,
    DS.SupplierKey,
    --Degenerate Dimensions
    CAST(SO.OrderID AS INT) AS OrderID,
    CAST(SOL.OrderLineID AS INT) AS OrderLineID,  
    --Measure
    CAST(SOL.Quantity AS INT) AS SalesOrderQuantity, 
    CAST(SOL.UnitPrice AS DECIMAL(18,2)) AS SalesOrderUnitPrice
FROM LDW.vwSalesOrdersLines SOL
INNER JOIN LDW.vwSalesOrders SO ON SOL.OrderID = SO.OrderID
LEFT JOIN LDW.vwDimCustomer DC ON DC.CustomerID = SO.CustomerID
LEFT JOIN LDW.vwDimStockItem DSI ON DSI.StockItemID = SOL.StockItemID
LEFT JOIN LDW.vwStockItems SI ON SI.StockItemID = DSI.StockItemID
LEFT JOIN LDW.vwDimSupplier DS ON DS.SupplierID = SI.SupplierID
WHERE SOL.FilePathDate = '2021-04-18' AND SO.FilePathDate = '2021-04-18';

--Dynamic SQL with a Stored Procedure to load Sales Data
CREATE PROCEDURE STG.FactSalesLoad @ProcessDate DATE
WITH ENCRYPTION
AS

BEGIN

DECLARE @location varchar(100)

IF OBJECT_ID('STG.FactSales') IS NOT NULL 
  DROP EXTERNAL TABLE STG.FactSales

SET @location = CONCAT('conformed/facts/factsales/incremental/',FORMAT (@ProcessDate, 'yyyy/MM/dd') )

DECLARE @CreateExternalTableString NVARCHAR(2000)

SET @CreateExternalTableString = 
'CREATE EXTERNAL TABLE STG.FactSales
WITH 
(
  LOCATION = ''' + @location + ''',                                      
  DATA_SOURCE = ExternalDataSourceDataLake,
  FILE_FORMAT = SynapseParquetFormat
)
AS
SELECT  
--Surrogate Keys 
DC.CustomerKey,
CAST(FORMAT(SO.OrderDate,''yyyyMMdd'') AS INT) as OrderDateKey,
DSI.StockItemKey,
DS.SupplierKey,
--Degenerate Dimensions
CAST(SO.OrderID AS INT) AS OrderID,
CAST(SOL.OrderLineID AS INT) AS OrderLineID,  
--Measure
CAST(SOL.Quantity AS INT) AS SalesOrderQuantity, 
CAST(SOL.UnitPrice AS DECIMAL(18,2)) AS SalesOrderUnitPrice
FROM LDW.vwSalesOrdersLines SOL
INNER JOIN LDW.vwSalesOrders SO ON SOL.OrderID = SO.OrderID
LEFT JOIN LDW.vwDimCustomer DC ON DC.CustomerID = SO.CustomerID
LEFT JOIN LDW.vwDimStockItem DSI ON DSI.StockItemID = SOL.StockItemID
LEFT JOIN LDW.vwStockItems SI ON SI.StockItemID = DSI.StockItemID
LEFT JOIN LDW.vwDimSupplier DS ON DS.SupplierID = SI.SupplierID
WHERE SOL.FilePathDate = ''' + CAST(@ProcessDate AS CHAR(10)) + '''  AND SO.FilePathDate = ''' + CAST(@ProcessDate AS CHAR(10)) + ''''

EXEC sp_executesql @CreateExternalTableString

END

--Run Procedure
EXEC STG.FactSalesLoad '2021-04-19';

--Select and Load the Supplier Data Changes
CREATE VIEW LDW.vwIncrementalSuppliers
AS
SELECT fct.*,
fct.filepath(1) AS FilePathDate
FROM 
OPENROWSET 
(
    BULK 'sourcedatasystem/ChangedData/*/Purchasing_Suppliers/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
) AS fct

--load
DECLARE @MaxKey TINYINT
SELECT @MaxKey = MAX(SupplierKey) FROM LDW.vwDimSupplier

IF OBJECT_ID('STG.DimSupplier') IS NOT NULL 
    DROP EXTERNAL TABLE STG.DimSupplier;

CREATE EXTERNAL TABLE STG.DimSupplier
WITH 
(
  LOCATION = 'conformed/dimensions/dimsupplier/02',
  DATA_SOURCE = ExternalDataSourceDataLake,
  FILE_FORMAT = SynapseParquetFormat
) 
AS
SELECT CAST(ROW_NUMBER() OVER(ORDER BY S.SupplierID) AS TINYINT) + @MaxKey AS SupplierKey,
S.SupplierID,
S.SupplierName,
SC.SupplierCategoryName,
CAST(S.ValidFrom AS DATE) AS ValidFromDate
FROM LDW.vwIncrementalSuppliers S
LEFT JOIN LDW.vwSupplierCategories SC ON SC.SupplierCategoryID = S.SupplierCategoryID
WHERE S.FilePathDate = '2021-06-22'
ORDER BY S.SupplierID;

--Selecting data from the Supplier Dimension
SELECT * 
FROM LDW.vwDimSupplier
WHERE SupplierID IN (5,14)
ORDER BY SupplierID;

--Create View to construct a complete SCD Type 2 Dimension
CREATE VIEW LDW.vwDimSupplierSCD
AS
SELECT SupplierKey,
        SupplierID,
        SupplierName,
        SupplierCategoryName,
        ValidFromDate,
        ISNULL(DATEADD(DAY,-1,LEAD(ValidFromDate,1) OVER (PARTITION BY SupplierID ORDER BY SupplierKey)),'2099-01-01') AS ValidToDate,
        CASE ROW_NUMBER() OVER(PARTITION BY SupplierID ORDER BY SupplierKey DESC) WHEN 1 THEN 'Y' ELSE 'N' END AS ActiveMember
FROM LDW.vwDimSupplier

--select from scd dimension view
SELECT * 
FROM LDW.vwDimSupplierSCD
WHERE SupplierID IN (1,5,14)
ORDER BY SupplierID,SupplierKey

--Amend Fact Loading Stored Procedure
CREATE PROCEDURE STG.FactSalesLoadCSD @ProcessDate DATE
WITH ENCRYPTION
AS

BEGIN

DECLARE @location varchar(100)

IF OBJECT_ID('STG.FactSales') IS NOT NULL 
  DROP EXTERNAL TABLE STG.FactSales

SET @location = CONCAT('conformed/facts/factsales/incremental/',FORMAT (@ProcessDate, 'yyyy/MM/dd') )

DECLARE @CreateExternalTableString NVARCHAR(2000)

SET @CreateExternalTableString = 
'CREATE EXTERNAL TABLE STG.FactSales
WITH 
(
  LOCATION = ''' + @location + ''',                                      
  DATA_SOURCE = ExternalDataSourceDataLake,
  FILE_FORMAT = SynapseParquetFormat
)
AS
SELECT  
--Surrogate Keys 
DC.CustomerKey,
CAST(FORMAT(SO.OrderDate,''yyyyMMdd'') AS INT) as OrderDateKey,
DSI.StockItemKey,
DS.SupplierKey,
--Degenerate Dimensions
CAST(SO.OrderID AS INT) AS OrderID,
CAST(SOL.OrderLineID AS INT) AS OrderLineID,  
--Measure
CAST(SOL.Quantity AS INT) AS SalesOrderQuantity, 
CAST(SOL.UnitPrice AS DECIMAL(18,2)) AS SalesOrderUnitPrice
FROM LDW.vwSalesOrdersLines SOL
INNER JOIN LDW.vwSalesOrders SO ON SOL.OrderID = SO.OrderID
LEFT JOIN LDW.vwDimCustomer DC ON DC.CustomerID = SO.CustomerID
LEFT JOIN LDW.vwDimStockItem DSI ON DSI.StockItemID = SOL.StockItemID
LEFT JOIN LDW.vwStockItems SI ON SI.StockItemID = DSI.StockItemID
LEFT JOIN LDW.vwDimSupplierSCD  DS ON DS.SupplierID = SI.SupplierID AND SO.OrderDate BETWEEN DS.ValidFromDate AND DS.ValidToDate
WHERE SOL.FilePathDate = ''' + CAST(@ProcessDate AS CHAR(10)) + '''  AND SO.FilePathDate = ''' + CAST(@ProcessDate AS CHAR(10)) + ''''

EXEC sp_executesql @CreateExternalTableString

END
