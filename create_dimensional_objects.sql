CREATE SCHEMA STG AUTHORIZATION dbo;

--Create Parquet file format
CREATE EXTERNAL FILE FORMAT SynapseParquetFormat
WITH ( 
        FORMAT_TYPE = PARQUET
     );

--Customer
CREATE EXTERNAL TABLE STG.DimCustomer
WITH 
(
  LOCATION = 'conformed/dimensions/dimcustomer/01',
  DATA_SOURCE = ExternalDataSourceDataLake,
  FILE_FORMAT = SynapseParquetFormat
) 
AS
SELECT CAST(ROW_NUMBER() OVER(ORDER BY C.CustomerID) AS INT) AS CustomerKey,
        CAST(C.CustomerID AS INT) AS CustomerID,
        C.CustomerName,
        CC.CustomerCategoryName,
        BG.BuyingGroupName,
        DM.DeliveryMethodName,
        DC.CityName AS DeliveryCityName,
        DSP.StateProvinceName AS DeliveryStateProvinceName,
        DSP.SalesTerritory AS DeliverySalesTerritory,
        DCO.Country AS DeliveryCountry,
        DCO.Continent AS DeliveryContinent,
        DCO.Region AS DeliveryRegion,
        DCO.Subregion AS DeliverySubregion,
        CAST('2013-01-01' AS DATE) AS ValidFromDate
FROM LDW.vwCustomers C
LEFT JOIN LDW.vwCustomerCategories CC On CC.CustomerCategoryID = C.CustomerCategoryID
LEFT JOIN LDW.vwCities DC ON DC.CityID = C.DeliveryCityID
LEFT JOIN LDW.vwStateProvinces DSP ON DSP.StateProvinceID = DC.StateProvinceID
LEFT JOIN LDW.vwCountries DCO ON DCO.CountryID = DSP.CountryID
LEFT JOIN LDW.vwBuyingGroups BG ON BG.BuyingGroupID = C.BuyingGroupID
LEFT JOIN LDW.vwDeliveryMethods DM ON DM.DeliveryMethodID = C.DeliveryMethodID
ORDER BY C.CustomerID

--StockItem
CREATE EXTERNAL TABLE STG.DimStockItem
WITH 
(
  LOCATION = 'conformed/dimensions/dimstockitem/01',
  DATA_SOURCE = ExternalDataSourceDataLake,
  FILE_FORMAT = SynapseParquetFormat
) 
AS
SELECT CAST(ROW_NUMBER() OVER(ORDER BY SI.StockItemID) AS SMALLINT) AS StockItemKey,
CAST(SI.StockItemID AS SMALLINT) AS StockItemID,
SI.StockItemName,
SI.LeadTimeDays,
C.ColorName,
OP.PackageTypeName AS OuterPackageTypeName,
CAST('2013-01-01' AS DATE) AS ValidFromDate
FROM LDW.vwStockItems SI
LEFT JOIN LDW.vwColors C ON C.ColorID = SI.ColorID
LEFT JOIN LDW.vwPackageTypes OP ON OP.PackageTypeID = SI.OuterPackageID
ORDER BY SI.StockItemID

--Supplier
CREATE EXTERNAL TABLE STG.DimSupplier
WITH 
(
  LOCATION = 'conformed/dimensions/dimsupplier/01',
  DATA_SOURCE = ExternalDataSourceDataLake,
  FILE_FORMAT = SynapseParquetFormat
) 
AS
SELECT CAST(ROW_NUMBER() OVER(ORDER BY S.SupplierID) AS TINYINT) AS SupplierKey,
CAST(S.SupplierID AS TINYINT) AS SupplierID,
S.SupplierName,
SC.SupplierCategoryName,
CAST('2013-01-01' AS DATE) AS ValidFromDate
FROM LDW.vwSuppliers S
LEFT JOIN LDW.vwSupplierCategories SC ON SC.SupplierCategoryID = S.SupplierCategoryID
ORDER BY S.SupplierID;

--Date
CREATE EXTERNAL TABLE STG.DimDate
WITH 
(
  LOCATION = 'conformed/dimensions/dimdate',
  DATA_SOURCE = ExternalDataSourceDataLake,
  FILE_FORMAT = SynapseParquetFormat
) 
AS
SELECT CAST(DateKey AS INT) AS DateKey,
        CAST(Date AS DATE) AS Date,
        CAST(Day AS TINYINT) AS Day,
        CAST(WeekDay AS TINYINT) AS WeekDay,
        WeekDayName,
        CAST(Month AS TINYINT) AS Month,
        MonthName,
        CAST(Quarter AS TINYINT) AS Quarter,
        CAST(Year AS SMALLINT) AS Year
FROM
OPENROWSET 
(
    BULK 'sourcedatadim/datedim/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
) AS fct


--Customer
CREATE VIEW LDW.vwDimCustomer
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'conformed/dimensions/dimcustomer/*/',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'Parquet'
) AS fct

--StockItem
CREATE VIEW LDW.vwDimStockItem
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'conformed/dimensions/dimstockitem/*/',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'Parquet'
) AS fct

--Supplier
CREATE VIEW LDW.vwDimSupplier
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'conformed/dimensions/dimsupplier/*/',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'Parquet'
) AS fct

--Date
CREATE VIEW LDW.vwDimDate
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'conformed/dimensions/dimdate',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'Parquet'
) AS fct


CREATE EXTERNAL TABLE STG.FactSales
WITH 
(
  LOCATION = 'conformed/facts/factsales/initial',
  DATA_SOURCE = ExternalDataSourceDataLake,
  FILE_FORMAT = SynapseParquetFormat
) 
AS
SELECT  
  --Surrogate Keys 
    DC.CustomerKey,
    CAST(FORMAT(SO.OrderDate,'yyyyMMdd') AS INT) as OrderDateKey,
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
LEFT JOIN LDW.vwDimSupplier DS ON DS.SupplierID = SI.SupplierID;


CREATE VIEW LDW.vwFactSales
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'conformed/facts/factsales/initial',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'Parquet'
) AS fct


--Group Sales by Date
SELECT DD.[Year] AS SalesYear,
    DD.[Month] AS SalesMonth,
SUM(FS.Quantity) AS SalesOrderQuantity,
SUM(FS.UnitPrice) AS SalesOrderUnitPrice,
COUNT(DISTINCT FS.OrderID) AS SalesOrderTotal
FROM LDW.vwFactSales FS
INNER JOIN LDW.vwDimDate DD ON DD.DateKey = FS.OrderDateKey
GROUP BY DD.[Year],
         DD.[Month]
ORDER BY DD.[Year],
         DD.[Month];

--Group Sales by Customer
SELECT DC.DeliverySalesTerritory,
SUM(FS.Quantity) AS SalesOrderQuantity,
SUM(FS.UnitPrice) AS SalesOrderUnitPrice,
COUNT(DISTINCT OrderID) AS SalesOrderTotal
FROM LDW.vwFactSales FS
INNER JOIN LDW.vwDimCustomer DC ON DC.CustomerKey = FS.CustomerKey
GROUP BY DC.DeliverySalesTerritory
ORDER BY SUM(FS.Quantity) DESC;

--Group Sales by Supplier
--Note that multiple Suppliers can be linked to a single Sales Order
SELECT DS.SupplierName,
SUM(FS.Quantity) AS SalesOrderQuantity,
SUM(FS.UnitPrice) AS SalesOrderUnitPrice
FROM LDW.vwFactSales FS
INNER JOIN LDW.vwDimSupplier DS ON DS.SupplierKey = FS.SupplierKey
GROUP BY DS.SupplierName
ORDER BY SUM(FS.Quantity) DESC;


EXEC sp_describe_first_result_set N'SELECT * FROM LDW.vwDimDate';

EXEC sp_describe_first_result_set N'SELECT * FROM LDW.vwDimSupplier';

EXEC sp_describe_first_result_set N'SELECT * FROM LDW.vwDimStockItem';

EXEC sp_describe_first_result_set N'SELECT * FROM LDW.vwDimCustomer';

EXEC sp_describe_first_result_set N'SELECT * FROM LDW.vwFactSales';
