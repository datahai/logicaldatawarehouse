
--Create Database and Data Sources

CREATE DATABASE sqllogicaldw;

CREATE EXTERNAL DATA SOURCE ExternalDataSourceDataLake
	WITH (
		LOCATION   = 'https://<storageaccountname>.dfs.core.windows.net/datalakehouse' 
	    );

CREATE SCHEMA LDW authorization dbo;

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<REALLY_STRING_PASSWORD!>';

CREATE DATABASE SCOPED CREDENTIAL SynapseUserIdentity 
WITH IDENTITY = 'User Identity';

ALTER DATABASE sqllogicaldw COLLATE Latin1_General_100_BIN2_UTF8;

/*
CREATE DATABASE SCOPED CREDENTIAL [SasToken]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
     SECRET = '<SAS_TOKEN_FROM_STORAGE_ACCOUNT>';
GO

CREATE EXTERNAL DATA SOURCE ExternalDataSourceDataLakeSAS
WITH (    LOCATION   = 'https://storsynapsedemo.dfs.core.windows.net/datalakehouse',
          CREDENTIAL = SasToken
)
*/

--Create Views

CREATE VIEW LDW.vwSalesOrders
AS
SELECT *,
CAST(REPLACE(fct.filepath(1),'OrderDatePartition=','') AS DATE) AS FilePathDate
FROM 
OPENROWSET 
(
    BULK 'sourcedatapartitionsalesorder/*/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
) AS fct

CREATE VIEW LDW.vwSalesOrdersLines
AS
SELECT *,
CAST(REPLACE(fct.filepath(1),'OrderDate=','') AS DATE) AS FilePathDate
FROM 
OPENROWSET 
(
    BULK 'sourcedatapartitionsalesorderline/*/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
) AS fct

--Data Related to Sales Orders

CREATE VIEW LDW.vwCustomers
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'sourcedatasystem/Sales_Customers/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
) AS fct


CREATE VIEW LDW.vwCities
AS
SELECT CityID,
        CityName,
        StateProvinceID,
        LatestRecordedPopulation
FROM 
OPENROWSET 
(
    BULK 'sourcedatasystem/Application_Cities/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
) AS fct


CREATE VIEW LDW.vwStateProvinces
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'sourcedatasystem/Application_StateProvinces/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
)
WITH
(
    StateProvinceID TINYINT,
    StateProvinceCode CHAR(2),
    StateProvinceName VARCHAR(30),
    CountryID TINYINT,
    SalesTerritory VARCHAR(14),
    LatestRecordedPopulation INT
) AS fct


CREATE VIEW LDW.vwCountries
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'sourcedatasystem/Application_Countries/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2,
    FIELDTERMINATOR ='|'
)
WITH
(
    CountryID TINYINT 1,
    Country VARCHAR(50) 2,
    IsoCode3 CHAR(3) 4,
    CountryType VARCHAR(50) 6,
    LatestRecordedPopulation INT 7,
    Continent VARCHAR(50) 8,
    Region VARCHAR(50) 9,
    Subregion VARCHAR(50) 10
) AS fct


CREATE VIEW LDW.vwBuyingGroups
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'sourcedatasystem/Sales_BuyingGroups/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
) AS fct


CREATE VIEW LDW.vwDeliveryMethods
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'sourcedatasystem/Application_DeliveryMethods/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
) AS fct


CREATE VIEW LDW.vwCustomerCategories
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'sourcedatasystem/Sales_CustomerCategories/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
) AS fct


CREATE VIEW LDW.vwPeople
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'sourcedatasystem/Application_People/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
) AS fct

--Data Related to Sales Order Lines

CREATE VIEW LDW.vwSuppliers
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'sourcedatasystem/Purchasing_Suppliers/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
) AS fct


CREATE VIEW LDW.vwSupplierCategories
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'sourcedatasystem/Purchasing_SupplierCategories/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
) AS fct


CREATE VIEW LDW.vwStockItems
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'sourcedatasystem/Warehouse_StockItems/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
) AS fct


CREATE VIEW LDW.vwColors
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'sourcedatasystem/Warehouse_Colors/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
) AS fct


CREATE VIEW LDW.vwPackageTypes
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'sourcedatasystem/Warehouse_PackageTypes/*.csv',
    DATA_SOURCE = 'ExternalDataSourceDataLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR ='|'
) AS fct

--Querying Source Data Views

--Aggregate Queries

SELECT YEAR(SO.OrderDate) AS OrderDateYear,
        COUNT(SO.OrderDate) AS TotalOrderCount
FROM LDW.vwSalesOrders SO
GROUP BY YEAR(SO.OrderDate);

SELECT ISNULL(C.ColorName,'No Colour') AS ColourName,
    SUM(SOL.Quantity) AS TotalOrderLineQuantity,
    SUM(SOL.UnitPrice) AS TotalOrderLineUnitPrice
FROM LDW.vwSalesOrdersLines SOL
INNER JOIN LDW.vwStockItems SI ON SI.StockItemID = SOL.StockItemID
LEFT JOIN LDW.vwColors C ON C.ColorID = SI.ColorID
GROUP BY ISNULL(C.ColorName,'No Colour');

SELECT 
    YEAR(SO.OrderDate) AS OrderDateYear,
    SC.SupplierCategoryName,
    SUM(SOL.Quantity) AS TotalOrderLineQuantity,
    SUM(SOL.UnitPrice) AS TotalOrderLineUnitPrice
FROM LDW.vwSalesOrdersLines SOL
INNER JOIN LDW.vwSalesOrders SO ON SO.OrderID = SOL.OrderID
INNER JOIN LDW.vwStockItems SI ON SI.StockItemID = SOL.StockItemID
INNER JOIN LDW.vwSuppliers S ON SI.SupplierID = S.SupplierID
INNER JOIN LDW.vwSupplierCategories SC ON SC.SupplierCategoryID = S.SupplierCategoryID
GROUP BY YEAR(SO.OrderDate),
        SC.SupplierCategoryName;


--Filtering and Manual Statistics Creation

SELECT COUNT(SO.OrderID) AS TotalOrderCount
FROM LDW.vwSalesOrders SO
WHERE SO.OrderDate = '2017-02-16'


EXEC sys.sp_create_openrowset_statistics N'
    SELECT OrderDate
FROM 
OPENROWSET 
(
    BULK ''sourcedatapartitionsalesorder/*/*.csv'',
    DATA_SOURCE = ''ExternalDataSourceDataLake'',
    FORMAT = ''CSV'',
    PARSER_VERSION = ''2.0'',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR =''|''
) AS fct
'

--Pushing Filters down to the Folder

SELECT YEAR(SO.OrderDate) AS OrderDateYear,
        COUNT(SO.OrderDate) AS TotalOrderCount
FROM LDW.vwSalesOrders SO
WHERE SO.FilePathDate = '2017-02-16'
GROUP BY YEAR(SO.OrderDate)

--Creating Views for Analytical Queries

CREATE VIEW LDW.vwDimStockItems
AS
SELECT  SI.StockItemID,
        SI.StockItemName,
        SI.LeadTimeDays,
        SI.TaxRate,
        SI.UnitPrice,
        SI.SearchDetails,
        PTUnit.PackageTypeName AS PackageTypeNameUnit,
        PTOut.PackageTypeName AS PackageTypeNameOuter,
        C.ColorName,
        S.SupplierName,
        S.PaymentDays,
        SC.SupplierCategoryName
FROM LDW.vwStockItems SI
LEFT JOIN LDW.vwPackageTypes PTUnit ON PTUnit.PackageTypeID = SI.UnitPackageID
LEFT JOIN LDW.vwPackageTypes PTOut ON PTOut.PackageTypeID = SI.OuterPackageID
LEFT JOIN LDW.vwColors C ON C.ColorID = SI.ColorID
LEFT JOIN LDW.vwSuppliers S ON S.SupplierID = SI.SupplierID
LEFT JOIN LDW.vwSupplierCategories SC ON SC.SupplierCategoryID = S.SupplierCategoryID


CREATE VIEW LDW.vwDimCustomers
AS
SELECT  C.CustomerID,
        C.CustomerName,
        C.AccountOpenedDate,
        C.CreditLimit,
        C.PaymentDays,
        CT.CityName AS CityNameDelivery,
        SP.StateProvinceCode AS StateProvinceCodeDelivery,
        SP.StateProvinceName AS StateProvinceNameDelivery,
        SP.SalesTerritory AS SalesTerritoryDelivery,
        CR.Country AS CountryDelivery,
        CR.Continent AS ContinentDelivery,
        CR.Region AS RegionDelivery,
        CR.Subregion AS SubregionDelivery,
        P.FullName AS PrimaryContactPersonName,
        CC.CustomerCategoryName,
        BG.BuyingGroupName,
        DM.DeliveryMethodName
FROM LDW.vwCustomers C
LEFT JOIN LDW.vwCities CT ON CT.CityID = C.DeliveryCityID
LEFT JOIN LDW.vwStateProvinces SP ON SP.StateProvinceID = CT.StateProvinceID
LEFT JOIN LDW.vwCountries CR ON CR.CountryID = SP.CountryID
LEFT JOIN LDW.vwPeople P ON P.PersonID = C.PrimaryContactPersonID
LEFT JOIN LDW.vwCustomerCategories CC ON CC.CustomerCategoryID = C.CustomerCategoryID
LEFT JOIN LDW.vwBuyingGroups BG ON BG.BuyingGroupID = C.BuyingGroupID
LEFT JOIN LDW.vwDeliveryMethods DM ON DM.DeliveryMethodID = C.DeliveryMethodID

--Using Analytical Views

SELECT DC.CustomerCategoryName,
        DS.PackageTypeNameUnit,
        SUM(SOL.Quantity) AS TotalOrderLineQuantity,
        SUM(SOL.UnitPrice) AS TotalOrderLineUnitPrice
FROM LDW.vwSalesOrdersLines SOL
INNER JOIN LDW.vwSalesOrders SO ON SO.OrderID = SOL.OrderID
INNER JOIN LDW.vwDimCustomers DC ON DC.CustomerID = SO.CustomerID
INNER JOIN LDW.vwDimStockItems DS ON DS.StockItemID = SOL.StockItemID
GROUP BY DC.CustomerCategoryName,
        DS.PackageTypeNameUnit
