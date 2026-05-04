-- ============================================================
--  AdventureWorks Data Warehouse  --  MySQL Workbench v3 (Final)
--  Improvements over v1:
--   • OrderDateKey INT joins (no DATE_FORMAT overhead)
--   • Surrogate key indexes (CustomerSK, ProductSK, ResellerSK)
--   • Composite indexes for frequent query patterns
--   • Geo hierarchy bridge view
--   • Window-function rankings on product + reseller views
--   • CTE-based vw_kpi_summary (no repeated subqueries)
--   • vw_customer_lifetime_value (CLV + CLV_Tier + CLV_Rank)
--   • vw_rfm_analysis (RFM segments + RecommendedAction)
--   • vw_channel_comparison (Internet vs Reseller pivot)
--   • vw_discount_impact (discount risk flags)
--   • vw_product_sales_velocity (avg monthly revenue + tier)
--   • vw_powerbi_executive (flat table for Power BI)
--   • vw_powerbi_products  (product deep-dive flat table)
--   • Grain + Degenerate Dimension notes on fact tables
--   • Extended data dictionary
--   [v3] • Corrected MODIFY column widths (RFM_Segment 30, ProductSegment 30, Country 100)
--   [v3] • LoyaltySegment, IncomeBand, PriceBand VARCHAR fixes added
--   [v3] • FiscalYear INT conversion note added
--   [v3] • All MODIFY statements in own section before indexes
-- ============================================================

CREATE DATABASE IF NOT EXISTS adventureworks_dw
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE adventureworks_dw;

-- ================================================================
-- SECTION 1  PRE-INDEX: FiscalYear CONVERSION
-- ================================================================
-- This section only handles the FiscalYear INT conversion.
-- ALL VARCHAR column fixes are in Section 2 (Step 2a) alongside
-- the indexes, so everything is in one place and runs in order.
-- ----------------------------------------------------------------

SET SQL_SAFE_UPDATES = 0;

-- Strip 'FY' prefix so FiscalYear becomes a clean integer (2011, 2012...)
-- ETL produces: 'FY2011', 'FY2012' etc — this turns them into 2011, 2012
UPDATE dim_date
SET    FiscalYear = REPLACE(FiscalYear, 'FY', '')
WHERE  FiscalYear LIKE 'FY%';

ALTER TABLE dim_date
    MODIFY COLUMN FiscalYear INT;
    -- Result: 2010, 2011, 2012, 2013, 2014, 2015  (integer, sorts correctly)
    -- FiscalQuarter stays VARCHAR: FQ1, FQ2, FQ3, FQ4
    -- Power BI note: FiscalYear displays as a number.
    --   To show "FY2013" as a label, add a calculated column in Power BI:
    --   FiscalYearLabel = "FY" & TEXT([FiscalYear], "0")

-- ================================================================
-- SECTION 2  INDEXES
-- ================================================================
-- TWO KNOWN ERRORS this section prevents:
--
-- Error 1068 "Multiple primary key defined"
--   Cause : pandas to_sql leaves no PK, but InnoDB sometimes auto-creates
--           a hidden clustered index. DROP PRIMARY KEY clears it first.
--   Fix   : every table runs DROP PRIMARY KEY before ADD PRIMARY KEY.
--           If a table truly has no PK, MySQL returns "Can't DROP PRIMARY
--           KEY; check that key exists" — comment out that one DROP line.
--
-- Error 1170 "BLOB/TEXT column used in key without key length"
--   Cause : pandas loads all string columns as TEXT (not VARCHAR).
--           MySQL cannot index a TEXT column without a prefix length.
--   Fix   : MODIFY every indexed text column to VARCHAR before indexing.
--           All VARCHAR sizes are set to actual max data length + margin.
-- ----------------------------------------------------------------

-- ── Step 2a: MODIFY text columns to VARCHAR on all tables ────────
-- This must run before the ADD INDEX statements below.
-- Sizes are based on actual maximum values in the dataset.

ALTER TABLE dim_product
    MODIFY COLUMN Category       VARCHAR(20),   -- max 11 chars (Accessories)
    MODIFY COLUMN Subcategory    VARCHAR(30),   -- max 17 chars (Mountain Frames)
    MODIFY COLUMN Color          VARCHAR(20),   -- max 12 chars
    MODIFY COLUMN Model          VARCHAR(50),   -- max 27 chars
    MODIFY COLUMN ProductTier    VARCHAR(20),   -- max 8  chars (Flagship)
    MODIFY COLUMN ProductSegment VARCHAR(30),   -- max 21 chars
    MODIFY COLUMN PriceBand      VARCHAR(25);   -- max 16 chars

ALTER TABLE dim_customer
    MODIFY COLUMN AgeGroup        VARCHAR(10),  -- max 5  chars (36-45)
    MODIFY COLUMN CustomerSegment VARCHAR(20),  -- max 8  chars (Standard)
    MODIFY COLUMN LoyaltySegment  VARCHAR(25),  -- max 16 chars (Regular 2-5 Yr)
    MODIFY COLUMN IncomeBand      VARCHAR(25),  -- max 17 chars (Very High 120K+)
    MODIFY COLUMN RFM_Segment     VARCHAR(30),  -- max 18 chars (Potential Loyalist)
    MODIFY COLUMN Occupation      VARCHAR(20),  -- max 14 chars (Skilled Manual)
    MODIFY COLUMN Gender          VARCHAR(10),  -- max 6  chars (Female)
    MODIFY COLUMN MaritalStatus   VARCHAR(10);  -- max 7  chars (Married)

ALTER TABLE dim_geography
    MODIFY COLUMN Country         VARCHAR(100), -- max 14 chars, 100 for safety
    MODIFY COLUMN City            VARCHAR(100), -- max 21 chars
    MODIFY COLUMN State           VARCHAR(100); -- max 19 chars

ALTER TABLE dim_reseller
    MODIFY COLUMN BusinessType    VARCHAR(30),  -- max 20 chars (Specialty Bike Shop)
    MODIFY COLUMN ResellerName    VARCHAR(100); -- max 41 chars

ALTER TABLE dim_sales_territory
    MODIFY COLUMN SalesTerritoryRegion  VARCHAR(20),  -- max 14 chars
    MODIFY COLUMN SalesTerritoryCountry VARCHAR(20),  -- max 14 chars
    MODIFY COLUMN SalesTerritoryGroup   VARCHAR(20);  -- max 13 chars

-- ── Step 2b: Safe DROP + ADD using stored procedures ────────────
-- Uses IF EXISTS logic via a helper procedure so the script is safe
-- on the very first run (indexes don't exist yet) AND on re-runs
-- (indexes already exist). No manual commenting needed.
-- ----------------------------------------------------------------

-- Helper procedure: drops an index only if it exists
DROP PROCEDURE IF EXISTS drop_index_if_exists;
DELIMITER $$
CREATE PROCEDURE drop_index_if_exists(
    IN tbl  VARCHAR(100),
    IN idx  VARCHAR(100)
)
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE  table_schema = DATABASE()
        AND    table_name   = tbl
        AND    index_name   = idx
    ) THEN
        SET @sql = CONCAT('ALTER TABLE `', tbl, '` DROP INDEX `', idx, '`');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END$$
DELIMITER ;

-- Helper procedure: drops PK only if it exists
DROP PROCEDURE IF EXISTS drop_pk_if_exists;
DELIMITER $$
CREATE PROCEDURE drop_pk_if_exists(IN tbl VARCHAR(100))
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE  table_schema = DATABASE()
        AND    table_name   = tbl
        AND    index_name   = 'PRIMARY'
    ) THEN
        SET @sql = CONCAT('ALTER TABLE `', tbl, '` DROP PRIMARY KEY');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END$$
DELIMITER ;

-- ── dim_date ─────────────────────────────────────────────────────
CALL drop_pk_if_exists('dim_date');
CALL drop_index_if_exists('dim_date', 'idx_dd_year');
CALL drop_index_if_exists('dim_date', 'idx_dd_month');
CALL drop_index_if_exists('dim_date', 'idx_dd_fiscal');
ALTER TABLE dim_date
    ADD PRIMARY KEY (DateKey),
    ADD INDEX idx_dd_year   (Year),
    ADD INDEX idx_dd_month  (Month),
    ADD INDEX idx_dd_fiscal (FiscalYear);

-- ── dim_customer ─────────────────────────────────────────────────
CALL drop_pk_if_exists('dim_customer');
CALL drop_index_if_exists('dim_customer', 'uk_cust_nk');
CALL drop_index_if_exists('dim_customer', 'idx_cust_geo');
CALL drop_index_if_exists('dim_customer', 'idx_cust_agegroup');
CALL drop_index_if_exists('dim_customer', 'idx_cust_segment');
CALL drop_index_if_exists('dim_customer', 'idx_cust_rfm');
CALL drop_index_if_exists('dim_customer', 'idx_cust_current');
ALTER TABLE dim_customer
    ADD PRIMARY KEY  (CustomerSK),
    ADD UNIQUE KEY   uk_cust_nk   (CustomerKey),
    ADD INDEX idx_cust_geo        (GeographyKey),
    ADD INDEX idx_cust_agegroup   (AgeGroup),
    ADD INDEX idx_cust_segment    (CustomerSegment),
    ADD INDEX idx_cust_rfm        (RFM_Segment),
    ADD INDEX idx_cust_current    (IsCurrent);

-- ── dim_product ──────────────────────────────────────────────────
CALL drop_pk_if_exists('dim_product');
CALL drop_index_if_exists('dim_product', 'uk_prod_nk');
CALL drop_index_if_exists('dim_product', 'idx_prod_category');
CALL drop_index_if_exists('dim_product', 'idx_prod_tier');
CALL drop_index_if_exists('dim_product', 'idx_prod_current');
ALTER TABLE dim_product
    ADD PRIMARY KEY  (ProductSK),
    ADD UNIQUE KEY   uk_prod_nk   (ProductKey),
    ADD INDEX idx_prod_category   (Category),
    ADD INDEX idx_prod_tier       (ProductTier),
    ADD INDEX idx_prod_current    (IsCurrent);

-- ── dim_reseller ─────────────────────────────────────────────────
CALL drop_pk_if_exists('dim_reseller');
CALL drop_index_if_exists('dim_reseller', 'uk_res_nk');
ALTER TABLE dim_reseller
    ADD PRIMARY KEY  (ResellerSK),
    ADD UNIQUE KEY   uk_res_nk    (ResellerKey);

-- ── dim_geography ────────────────────────────────────────────────
CALL drop_pk_if_exists('dim_geography');
CALL drop_index_if_exists('dim_geography', 'uk_geo_nk');
CALL drop_index_if_exists('dim_geography', 'idx_geo_country');
ALTER TABLE dim_geography
    ADD PRIMARY KEY  (GeographySK),
    ADD UNIQUE KEY   uk_geo_nk    (GeographyKey),
    ADD INDEX idx_geo_country     (Country);

-- ── dim_sales_territory ──────────────────────────────────────────
CALL drop_pk_if_exists('dim_sales_territory');
ALTER TABLE dim_sales_territory
    ADD PRIMARY KEY (SalesTerritoryKey);

-- ── dim_employee ─────────────────────────────────────────────────
CALL drop_pk_if_exists('dim_employee');
ALTER TABLE dim_employee
    ADD PRIMARY KEY (EmployeeKey);

-- ── fact_internet_sales ──────────────────────────────────────────
-- Grain  : One row per Sales Order Line
-- Degen. : SalesOrderNumber is a Degenerate Dimension
-- Join   : OrderDateKey INT → dim_date.DateKey (no DATE_FORMAT overhead)
CALL drop_pk_if_exists('fact_internet_sales');
CALL drop_index_if_exists('fact_internet_sales', 'idx_fis_productsk');
CALL drop_index_if_exists('fact_internet_sales', 'idx_fis_customersk');
CALL drop_index_if_exists('fact_internet_sales', 'idx_fis_product');
CALL drop_index_if_exists('fact_internet_sales', 'idx_fis_customer');
CALL drop_index_if_exists('fact_internet_sales', 'idx_fis_territory');
CALL drop_index_if_exists('fact_internet_sales', 'idx_fis_datekey');
CALL drop_index_if_exists('fact_internet_sales', 'idx_fis_year');
CALL drop_index_if_exists('fact_internet_sales', 'idx_fis_pd');
ALTER TABLE fact_internet_sales
    ADD INDEX idx_fis_productsk  (ProductSK),
    ADD INDEX idx_fis_customersk (CustomerSK),
    ADD INDEX idx_fis_product    (ProductKey),
    ADD INDEX idx_fis_customer   (CustomerKey),
    ADD INDEX idx_fis_territory  (SalesTerritoryKey),
    ADD INDEX idx_fis_datekey    (OrderDateKey),
    ADD INDEX idx_fis_year       (OrderYear),
    ADD INDEX idx_fis_pd         (ProductKey, OrderDateKey);

-- ── fact_reseller_sales ──────────────────────────────────────────
-- Grain  : One row per Sales Order Line
-- Degen. : SalesOrderNumber is a Degenerate Dimension
CALL drop_pk_if_exists('fact_reseller_sales');
CALL drop_index_if_exists('fact_reseller_sales', 'idx_frs_productsk');
CALL drop_index_if_exists('fact_reseller_sales', 'idx_frs_resellersk');
CALL drop_index_if_exists('fact_reseller_sales', 'idx_frs_product');
CALL drop_index_if_exists('fact_reseller_sales', 'idx_frs_reseller');
CALL drop_index_if_exists('fact_reseller_sales', 'idx_frs_territory');
CALL drop_index_if_exists('fact_reseller_sales', 'idx_frs_datekey');
CALL drop_index_if_exists('fact_reseller_sales', 'idx_frs_year');
CALL drop_index_if_exists('fact_reseller_sales', 'idx_frs_pd');
ALTER TABLE fact_reseller_sales
    ADD INDEX idx_frs_productsk  (ProductSK),
    ADD INDEX idx_frs_resellersk (ResellerSK),
    ADD INDEX idx_frs_product    (ProductKey),
    ADD INDEX idx_frs_reseller   (ResellerKey),
    ADD INDEX idx_frs_territory  (SalesTerritoryKey),
    ADD INDEX idx_frs_datekey    (OrderDateKey),
    ADD INDEX idx_frs_year       (OrderYear),
    ADD INDEX idx_frs_pd         (ProductKey, OrderDateKey);

-- Cleanup helper procedures (no longer needed after indexes are set)
DROP PROCEDURE IF EXISTS drop_index_if_exists;
DROP PROCEDURE IF EXISTS drop_pk_if_exists;


-- ================================================================
-- SECTION 3  GEO HIERARCHY BRIDGE VIEW
-- ================================================================
DROP VIEW IF EXISTS vw_dim_geo_hierarchy;
CREATE VIEW vw_dim_geo_hierarchy AS
SELECT
    g.GeographySK,
    g.GeographyKey,
    g.City,
    g.State,
    g.Country,
    t.SalesTerritoryKey,
    t.SalesTerritoryRegion   AS Territory,
    t.SalesTerritoryCountry  AS TerritoryCountry,
    t.SalesTerritoryGroup    AS Region
FROM dim_geography       g
JOIN dim_sales_territory t ON g.SalesTerritoryKey = t.SalesTerritoryKey;


-- ================================================================
-- SECTION 4  CORE VIEWS
-- ================================================================

-- View 1: Sales Summary (uses OrderDateKey for fast joins)
DROP VIEW IF EXISTS vw_sales_summary;
CREATE VIEW vw_sales_summary AS
SELECT
    'Internet'                             AS SalesChannel,
    d.Year, d.Quarter, d.QuarterName,
    d.FiscalYear,
    gh.Territory,
    gh.TerritoryCountry                    AS Country,
    gh.Region,
    p.Category, p.Subcategory, p.ProductTier,
    COUNT(DISTINCT f.SalesOrderNumber)     AS TotalOrders,
    SUM(f.SalesAmount)                     AS TotalRevenue,
    SUM(f.TotalProductCost)                AS TotalCost,
    SUM(f.Profit)                          AS TotalProfit,
    ROUND(SUM(f.Profit)/NULLIF(SUM(f.SalesAmount),0)*100,2) AS ProfitMargin,
    SUM(f.DiscountAmount)                  AS TotalDiscount,
    ROUND(AVG(f.UnitPrice),2)              AS AvgUnitPrice
FROM fact_internet_sales     f
JOIN dim_date                d  ON f.OrderDateKey      = d.DateKey
JOIN dim_product             p  ON f.ProductKey        = p.ProductKey
JOIN dim_customer            c  ON f.CustomerKey       = c.CustomerKey
JOIN dim_geography           g  ON c.GeographyKey      = g.GeographyKey
JOIN vw_dim_geo_hierarchy    gh ON g.GeographyKey      = gh.GeographyKey
GROUP BY d.Year, d.Quarter, d.QuarterName, d.FiscalYear,
         gh.Territory, gh.TerritoryCountry, gh.Region,
         p.Category, p.Subcategory, p.ProductTier

UNION ALL

SELECT
    'Reseller',
    d.Year, d.Quarter, d.QuarterName, d.FiscalYear,
    gh.Territory, gh.TerritoryCountry, gh.Region,
    p.Category, p.Subcategory, p.ProductTier,
    COUNT(DISTINCT f.SalesOrderNumber),
    SUM(f.SalesAmount), SUM(f.TotalProductCost), SUM(f.Profit),
    ROUND(SUM(f.Profit)/NULLIF(SUM(f.SalesAmount),0)*100,2),
    SUM(f.DiscountAmount), ROUND(AVG(f.UnitPrice),2)
FROM fact_reseller_sales     f
JOIN dim_date                d  ON f.OrderDateKey      = d.DateKey
JOIN dim_product             p  ON f.ProductKey        = p.ProductKey
JOIN dim_reseller            r  ON f.ResellerKey       = r.ResellerKey
JOIN dim_geography           g  ON r.GeographyKey      = g.GeographyKey
JOIN vw_dim_geo_hierarchy    gh ON g.GeographyKey      = gh.GeographyKey
GROUP BY d.Year, d.Quarter, d.QuarterName, d.FiscalYear,
         gh.Territory, gh.TerritoryCountry, gh.Region,
         p.Category, p.Subcategory, p.ProductTier;


-- View 2: Territory Performance
DROP VIEW IF EXISTS vw_territory_performance;
CREATE VIEW vw_territory_performance AS
SELECT
    t.SalesTerritoryKey,
    t.SalesTerritoryRegion   AS Territory,
    t.SalesTerritoryCountry  AS Country,
    t.SalesTerritoryGroup    AS Region,
    SUM(CASE WHEN src='Internet' THEN Revenue ELSE 0 END)  AS InternetRevenue,
    SUM(CASE WHEN src='Reseller' THEN Revenue ELSE 0 END)  AS ResellerRevenue,
    SUM(Revenue)                                           AS TotalRevenue,
    SUM(Profit)                                            AS TotalProfit,
    ROUND(SUM(Profit)/NULLIF(SUM(Revenue),0)*100,2)       AS ProfitMargin,
    SUM(TotalOrders)                                       AS TotalOrders
FROM (
    SELECT SalesTerritoryKey,'Internet' AS src,
           SUM(SalesAmount) AS Revenue, SUM(Profit) AS Profit,
           COUNT(DISTINCT SalesOrderNumber) AS TotalOrders
    FROM fact_internet_sales GROUP BY SalesTerritoryKey
    UNION ALL
    SELECT SalesTerritoryKey,'Reseller',
           SUM(SalesAmount), SUM(Profit),
           COUNT(DISTINCT SalesOrderNumber)
    FROM fact_reseller_sales GROUP BY SalesTerritoryKey
) x
JOIN dim_sales_territory t ON x.SalesTerritoryKey = t.SalesTerritoryKey
GROUP BY t.SalesTerritoryKey, t.SalesTerritoryRegion,
         t.SalesTerritoryCountry, t.SalesTerritoryGroup;


-- View 3: Product Performance (window function rank, correct line count)
DROP VIEW IF EXISTS vw_product_performance;
CREATE VIEW vw_product_performance AS
SELECT
    p.ProductKey, p.ProductSK, p.Product,
    p.Category, p.Subcategory, p.Color, p.Model,
    p.PriceBand, p.ProductTier, p.ProductSegment,
    ROUND(p.AvgUnitPrice,2)                                        AS AvgUnitPrice,
    SUM(CASE WHEN src='Internet' THEN Revenue ELSE 0 END)          AS InternetRevenue,
    SUM(CASE WHEN src='Reseller' THEN Revenue ELSE 0 END)          AS ResellerRevenue,
    SUM(Revenue)                                                   AS TotalRevenue,
    SUM(Profit)                                                    AS TotalProfit,
    ROUND(SUM(Profit)/NULLIF(SUM(Revenue),0)*100,2)               AS ProfitMargin,
    SUM(LineCount)                                                 AS TotalLinesSold,
    DENSE_RANK() OVER (ORDER BY SUM(Revenue) DESC)                AS RevenueRank
FROM (
    SELECT ProductKey,'Internet' AS src,
           SUM(SalesAmount) AS Revenue, SUM(Profit) AS Profit,
           COUNT(*) AS LineCount
    FROM fact_internet_sales GROUP BY ProductKey
    UNION ALL
    SELECT ProductKey,'Reseller',
           SUM(SalesAmount), SUM(Profit), COUNT(*)
    FROM fact_reseller_sales GROUP BY ProductKey
) x
JOIN dim_product p ON x.ProductKey = p.ProductKey
GROUP BY p.ProductKey, p.ProductSK, p.Product, p.Category,
         p.Subcategory, p.Color, p.Model, p.PriceBand,
         p.ProductTier, p.ProductSegment, p.AvgUnitPrice;


-- View 4: Monthly Trend
DROP VIEW IF EXISTS vw_monthly_trend;
CREATE VIEW vw_monthly_trend AS
SELECT
    d.Year, d.Month, d.MonthName, d.MonthShort,
    d.YearMonth, d.FiscalYear, d.FiscalQuarter,
    'Internet'                              AS Channel,
    SUM(f.SalesAmount)                      AS Revenue,
    SUM(f.Profit)                           AS Profit,
    ROUND(SUM(f.Profit)/NULLIF(SUM(f.SalesAmount),0)*100,2) AS ProfitMargin,
    COUNT(DISTINCT f.SalesOrderNumber)      AS Orders,
    ROUND(AVG(f.UnitPrice),2)               AS AvgUnitPrice
FROM fact_internet_sales f
JOIN dim_date d ON f.OrderDateKey = d.DateKey
GROUP BY d.Year, d.Month, d.MonthName, d.MonthShort,
         d.YearMonth, d.FiscalYear, d.FiscalQuarter

UNION ALL

SELECT
    d.Year, d.Month, d.MonthName, d.MonthShort,
    d.YearMonth, d.FiscalYear, d.FiscalQuarter,
    'Reseller',
    SUM(f.SalesAmount), SUM(f.Profit),
    ROUND(SUM(f.Profit)/NULLIF(SUM(f.SalesAmount),0)*100,2),
    COUNT(DISTINCT f.SalesOrderNumber), ROUND(AVG(f.UnitPrice),2)
FROM fact_reseller_sales f
JOIN dim_date d ON f.OrderDateKey = d.DateKey
GROUP BY d.Year, d.Month, d.MonthName, d.MonthShort,
         d.YearMonth, d.FiscalYear, d.FiscalQuarter;


-- View 5: Customer Segments
DROP VIEW IF EXISTS vw_customer_segments;
CREATE VIEW vw_customer_segments AS
SELECT
    c.CustomerKey, c.CustomerSK, c.CustomerName,
    c.Gender, c.AgeGroup, c.Occupation,
    c.IncomeBand, c.CustomerSegment, c.LoyaltySegment,
    c.TenureYears, c.RFM_Segment, c.RFM_Score,
    g.City, g.State, g.Country,
    t.SalesTerritoryRegion AS Territory,
    t.SalesTerritoryGroup  AS Region,
    COUNT(DISTINCT f.SalesOrderNumber)  AS TotalOrders,
    SUM(f.SalesAmount)                  AS TotalRevenue,
    SUM(f.Profit)                       AS TotalProfit,
    ROUND(AVG(f.SalesAmount),2)         AS AvgOrderValue,
    MIN(f.OrderDate)                    AS FirstOrderDate,
    MAX(f.OrderDate)                    AS LastOrderDate
FROM dim_customer           c
LEFT JOIN fact_internet_sales  f  ON c.CustomerKey        = f.CustomerKey
LEFT JOIN dim_geography        g  ON c.GeographyKey       = g.GeographyKey
LEFT JOIN dim_sales_territory  t  ON g.SalesTerritoryKey  = t.SalesTerritoryKey
GROUP BY c.CustomerKey, c.CustomerSK, c.CustomerName, c.Gender,
         c.AgeGroup, c.Occupation, c.IncomeBand, c.CustomerSegment,
         c.LoyaltySegment, c.TenureYears, c.RFM_Segment, c.RFM_Score,
         g.City, g.State, g.Country,
         t.SalesTerritoryRegion, t.SalesTerritoryGroup;


-- View 6: Reseller Performance (with rank)
DROP VIEW IF EXISTS vw_reseller_performance;
CREATE VIEW vw_reseller_performance AS
SELECT
    r.ResellerKey, r.ResellerSK, r.ResellerName, r.BusinessType,
    g.City, g.State, g.Country,
    t.SalesTerritoryRegion AS Territory,
    t.SalesTerritoryGroup  AS Region,
    COUNT(DISTINCT f.SalesOrderNumber)  AS TotalOrders,
    SUM(f.SalesAmount)                  AS TotalRevenue,
    SUM(f.Profit)                       AS TotalProfit,
    ROUND(SUM(f.Profit)/NULLIF(SUM(f.SalesAmount),0)*100,2) AS ProfitMargin,
    ROUND(AVG(f.SalesAmount),2)         AS AvgOrderValue,
    SUM(f.DiscountAmount)               AS TotalDiscount,
    DENSE_RANK() OVER (ORDER BY SUM(f.SalesAmount) DESC) AS RevenueRank
FROM fact_reseller_sales    f
JOIN dim_reseller            r ON f.ResellerKey       = r.ResellerKey
JOIN dim_geography           g ON r.GeographyKey      = g.GeographyKey
JOIN dim_sales_territory     t ON f.SalesTerritoryKey = t.SalesTerritoryKey
GROUP BY r.ResellerKey, r.ResellerSK, r.ResellerName, r.BusinessType,
         g.City, g.State, g.Country,
         t.SalesTerritoryRegion, t.SalesTerritoryGroup;


-- View 7: KPI Summary (CTE, no repeated subqueries)
DROP VIEW IF EXISTS vw_kpi_summary;
CREATE VIEW vw_kpi_summary AS
WITH combined AS (
    SELECT 'Internet' AS channel, SalesAmount, Profit,
           SalesOrderNumber, CustomerKey, ProductKey
    FROM fact_internet_sales
    UNION ALL
    SELECT 'Reseller', SalesAmount, Profit,
           SalesOrderNumber, NULL, ProductKey
    FROM fact_reseller_sales
),
agg AS (
    SELECT
        SUM(SalesAmount)                            AS TotalRevenue,
        SUM(Profit)                                 AS TotalProfit,
        COUNT(DISTINCT SalesOrderNumber)            AS TotalOrders,
        COUNT(DISTINCT CASE WHEN channel='Internet' THEN CustomerKey END) AS UniqueCustomers,
        COUNT(DISTINCT ProductKey)                  AS UniqueProducts,
        SUM(CASE WHEN channel='Internet' THEN SalesAmount ELSE 0 END) AS InternetRevenue,
        SUM(CASE WHEN channel='Reseller' THEN SalesAmount ELSE 0 END) AS ResellerRevenue
    FROM combined
)
SELECT
    TotalRevenue,
    TotalProfit,
    ROUND(TotalProfit/NULLIF(TotalRevenue,0)*100,2)       AS ProfitMarginPct,
    TotalOrders,
    ROUND(TotalRevenue/NULLIF(TotalOrders,0),2)           AS AvgOrderValue,
    UniqueCustomers,
    ROUND(TotalRevenue/NULLIF(UniqueCustomers,0),2)       AS RevenuePerCustomer,
    UniqueProducts,
    InternetRevenue,
    ResellerRevenue,
    ROUND(InternetRevenue/NULLIF(TotalRevenue,0)*100,2)   AS InternetSharePct,
    ROUND(ResellerRevenue/NULLIF(TotalRevenue,0)*100,2)   AS ResellerSharePct
FROM agg;


-- ================================================================
-- SECTION 5  ADVANCED VIEWS
-- ================================================================

-- View 8: Customer Lifetime Value
DROP VIEW IF EXISTS vw_customer_lifetime_value;
CREATE VIEW vw_customer_lifetime_value AS
SELECT
    c.CustomerKey, c.CustomerSK, c.CustomerName,
    c.AgeGroup, c.CustomerSegment, c.RFM_Segment,
    COUNT(DISTINCT f.SalesOrderNumber)                             AS TotalOrders,
    SUM(f.SalesAmount)                                             AS TotalRevenue,
    SUM(f.Profit)                                                  AS TotalProfit,
    ROUND(AVG(f.SalesAmount),2)                                    AS AvgOrderValue,
    MIN(f.OrderDate)                                               AS FirstOrderDate,
    MAX(f.OrderDate)                                               AS LastOrderDate,
    DATEDIFF(MAX(f.OrderDate),MIN(f.OrderDate))                    AS CustomerLifetimeDays,
    ROUND(SUM(f.SalesAmount)/NULLIF(DATEDIFF(MAX(f.OrderDate),MIN(f.OrderDate)),0),4) AS RevenuePerDay,
    CASE
        WHEN SUM(f.SalesAmount) >= 10000 THEN 'Platinum'
        WHEN SUM(f.SalesAmount) >= 5000  THEN 'Gold'
        WHEN SUM(f.SalesAmount) >= 1000  THEN 'Silver'
        ELSE 'Bronze'
    END AS CLV_Tier,
    DENSE_RANK() OVER (ORDER BY SUM(f.SalesAmount) DESC) AS CLV_Rank
FROM dim_customer        c
JOIN fact_internet_sales f ON c.CustomerKey = f.CustomerKey
GROUP BY c.CustomerKey, c.CustomerSK, c.CustomerName,
         c.AgeGroup, c.CustomerSegment, c.RFM_Segment;


-- View 9: RFM with Recommended Actions
DROP VIEW IF EXISTS vw_rfm_analysis;
CREATE VIEW vw_rfm_analysis AS
SELECT
    c.CustomerKey, c.CustomerName, c.AgeGroup,
    c.Occupation, c.CustomerSegment,
    c.Recency, c.Frequency,
    ROUND(c.Monetary,2)     AS Monetary,
    c.RFM_Score, c.RFM_Segment,
    g.Country,
    t.SalesTerritoryRegion  AS Territory,
    CASE c.RFM_Segment
        WHEN 'Champions'          THEN 'Reward & upsell'
        WHEN 'Loyal'              THEN 'Loyalty program'
        WHEN 'Potential Loyalist' THEN 'Engage & nurture'
        WHEN 'At Risk'            THEN 'Win-back campaign'
        WHEN 'Lost'               THEN 'Re-activation or accept churn'
        ELSE 'Classify'
    END AS RecommendedAction
FROM dim_customer          c
LEFT JOIN dim_geography    g ON c.GeographyKey       = g.GeographyKey
LEFT JOIN dim_sales_territory t ON g.SalesTerritoryKey = t.SalesTerritoryKey
WHERE c.RFM_Segment IS NOT NULL;


-- View 10: Channel Comparison (Internet vs Reseller pivot)
DROP VIEW IF EXISTS vw_channel_comparison;
CREATE VIEW vw_channel_comparison AS
SELECT
    Year,
    SUM(CASE WHEN SalesChannel='Internet' THEN TotalRevenue ELSE 0 END) AS InternetRevenue,
    SUM(CASE WHEN SalesChannel='Reseller' THEN TotalRevenue ELSE 0 END) AS ResellerRevenue,
    SUM(TotalRevenue)                                                    AS CombinedRevenue,
    SUM(CASE WHEN SalesChannel='Internet' THEN TotalProfit  ELSE 0 END) AS InternetProfit,
    SUM(CASE WHEN SalesChannel='Reseller' THEN TotalProfit  ELSE 0 END) AS ResellerProfit,
    ROUND(SUM(CASE WHEN SalesChannel='Internet' THEN TotalProfit ELSE 0 END)/
          NULLIF(SUM(CASE WHEN SalesChannel='Internet' THEN TotalRevenue ELSE 0 END),0)*100,2) AS InternetMarginPct,
    ROUND(SUM(CASE WHEN SalesChannel='Reseller' THEN TotalProfit ELSE 0 END)/
          NULLIF(SUM(CASE WHEN SalesChannel='Reseller' THEN TotalRevenue ELSE 0 END),0)*100,2) AS ResellerMarginPct,
    SUM(CASE WHEN SalesChannel='Internet' THEN TotalOrders ELSE 0 END) AS InternetOrders,
    SUM(CASE WHEN SalesChannel='Reseller' THEN TotalOrders ELSE 0 END) AS ResellerOrders
FROM vw_sales_summary
GROUP BY Year
ORDER BY Year;


-- View 11: Discount Impact Analysis
DROP VIEW IF EXISTS vw_discount_impact;
CREATE VIEW vw_discount_impact AS
SELECT
    p.Category, p.Subcategory,
    COUNT(*)                                                    AS OrderLines,
    SUM(f.SalesAmount)                                          AS Revenue,
    SUM(f.DiscountAmount)                                       AS TotalDiscount,
    ROUND(AVG(f.DiscountRate),2)                               AS AvgDiscountRate,
    SUM(f.Profit)                                               AS TotalProfit,
    ROUND(SUM(f.Profit)/NULLIF(SUM(f.SalesAmount),0)*100,2)   AS ProfitMarginPct,
    ROUND(SUM(f.DiscountAmount)/NULLIF(SUM(f.SalesAmount),0)*100,2) AS DiscountToRevenuePct,
    CASE WHEN ROUND(SUM(f.DiscountAmount)/NULLIF(SUM(f.SalesAmount),0)*100,2) > 5
         THEN 'High Discount Risk'
         ELSE 'Normal'
    END AS DiscountFlag
FROM fact_internet_sales f
JOIN dim_product          p ON f.ProductKey = p.ProductKey
GROUP BY p.Category, p.Subcategory
ORDER BY DiscountToRevenuePct DESC;


-- View 12: Product Sales Velocity
DROP VIEW IF EXISTS vw_product_sales_velocity;
CREATE VIEW vw_product_sales_velocity AS
WITH monthly_sales AS (
    SELECT f.ProductKey, d.YearMonth,
           SUM(f.SalesAmount) AS MonthlyRevenue
    FROM fact_internet_sales f
    JOIN dim_date d ON f.OrderDateKey = d.DateKey
    GROUP BY f.ProductKey, d.YearMonth
),
vel AS (
    SELECT ProductKey,
           ROUND(AVG(MonthlyRevenue),2)    AS AvgMonthlyRevenue,
           ROUND(MAX(MonthlyRevenue),2)    AS PeakMonthlyRevenue,
           ROUND(STDDEV(MonthlyRevenue),2) AS RevenueStdDev,
           COUNT(YearMonth)                AS ActiveMonths
    FROM monthly_sales
    GROUP BY ProductKey
)
SELECT
    p.ProductKey, p.Product, p.Category, p.Subcategory, p.ProductTier,
    v.AvgMonthlyRevenue, v.PeakMonthlyRevenue, v.RevenueStdDev, v.ActiveMonths,
    CASE
        WHEN v.AvgMonthlyRevenue >= 50000 THEN 'Fast Mover'
        WHEN v.AvgMonthlyRevenue >= 10000 THEN 'Steady Mover'
        WHEN v.AvgMonthlyRevenue >= 1000  THEN 'Slow Mover'
        ELSE 'Low Velocity'
    END AS VelocityTier,
    DENSE_RANK() OVER (ORDER BY v.AvgMonthlyRevenue DESC) AS VelocityRank
FROM vel v
JOIN dim_product p ON v.ProductKey = p.ProductKey;


-- ================================================================
-- SECTION 6  POWER BI FLAT TABLES
-- ================================================================

-- View 13: Executive Flat Table
DROP VIEW IF EXISTS vw_powerbi_executive;
CREATE VIEW vw_powerbi_executive AS
SELECT
    d.FullDate AS Date, d.Year, d.Month, d.MonthName,
    d.Quarter, d.FiscalYear, d.YearMonth, d.IsWeekend, d.Season,
    'Internet'                  AS SalesChannel,
    t.SalesTerritoryCountry     AS Country,
    t.SalesTerritoryRegion      AS Territory,
    t.SalesTerritoryGroup       AS Region,
    p.Category, p.Subcategory, p.ProductTier, p.PriceBand,
    f.SalesAmount AS Revenue,   f.Profit,
    f.ProfitMargin,             f.DiscountAmount,
    f.DiscountRate,             f.UnitPrice,
    f.SalesOrderNumber,
    c.AgeGroup, c.Occupation,   c.CustomerSegment,
    c.IncomeBand,               c.RFM_Segment
FROM fact_internet_sales     f
JOIN dim_date                d  ON f.OrderDateKey      = d.DateKey
JOIN dim_product             p  ON f.ProductKey        = p.ProductKey
JOIN dim_customer            c  ON f.CustomerKey       = c.CustomerKey
JOIN dim_sales_territory     t  ON f.SalesTerritoryKey = t.SalesTerritoryKey

UNION ALL

SELECT
    d.FullDate, d.Year, d.Month, d.MonthName,
    d.Quarter, d.FiscalYear, d.YearMonth, d.IsWeekend, d.Season,
    'Reseller',
    t.SalesTerritoryCountry, t.SalesTerritoryRegion, t.SalesTerritoryGroup,
    p.Category, p.Subcategory, p.ProductTier, p.PriceBand,
    f.SalesAmount, f.Profit, f.ProfitMargin, f.DiscountAmount,
    f.DiscountRate, f.UnitPrice, f.SalesOrderNumber,
    NULL, NULL, NULL, NULL, NULL
FROM fact_reseller_sales     f
JOIN dim_date                d  ON f.OrderDateKey      = d.DateKey
JOIN dim_product             p  ON f.ProductKey        = p.ProductKey
JOIN dim_sales_territory     t  ON f.SalesTerritoryKey = t.SalesTerritoryKey;


-- View 14: Product Deep Dive Flat Table
DROP VIEW IF EXISTS vw_powerbi_products;
CREATE VIEW vw_powerbi_products AS
SELECT
    p.ProductKey, p.Product, p.Category, p.Subcategory,
    p.Color, p.Model, p.PriceBand, p.ProductTier, p.ProductSegment,
    ROUND(p.AvgUnitPrice,2)                                       AS AvgUnitPrice,
    SUM(f.SalesAmount)                                            AS Revenue,
    SUM(f.Profit)                                                 AS Profit,
    ROUND(SUM(f.Profit)/NULLIF(SUM(f.SalesAmount),0)*100,2)      AS ProfitMargin,
    COUNT(DISTINCT f.SalesOrderNumber)                            AS OrderCount,
    COUNT(*)                                                      AS LinesSold,
    SUM(f.DiscountAmount)                                         AS TotalDiscount,
    ROUND(SUM(f.DiscountAmount)/NULLIF(SUM(f.SalesAmount),0)*100,2) AS DiscountPct,
    DENSE_RANK() OVER (ORDER BY SUM(f.SalesAmount) DESC)          AS RevenueRank
FROM fact_internet_sales f
JOIN dim_product          p ON f.ProductKey = p.ProductKey
GROUP BY p.ProductKey, p.Product, p.Category, p.Subcategory,
         p.Color, p.Model, p.PriceBand, p.ProductTier,
         p.ProductSegment, p.AvgUnitPrice;


-- ================================================================
-- SECTION 7  ANALYTICAL QUERIES
-- ================================================================

-- Q1: YoY Growth with LAG window function
WITH yr AS (
    SELECT Year, SUM(TotalRevenue) AS Revenue FROM vw_sales_summary GROUP BY Year
)
SELECT Year,
       ROUND(Revenue/1e6,2) AS RevenueMillion,
       ROUND((Revenue - LAG(Revenue) OVER (ORDER BY Year)) /
              NULLIF(LAG(Revenue) OVER (ORDER BY Year),0)*100,2) AS YoYGrowthPct
FROM yr ORDER BY Year;


-- Q2: Top 10 Products
SELECT RevenueRank, Product, Category, ProductTier,
       ROUND(TotalRevenue/1000,1) AS RevenueK, ProfitMargin
FROM   vw_product_performance WHERE RevenueRank <= 10 ORDER BY RevenueRank;


-- Q3: Territory Risk (high revenue, low margin)
SELECT Territory, Country,
       ROUND(TotalRevenue/1e6,2) AS RevenueMillion, ProfitMargin,
       CASE WHEN ProfitMargin < 20 AND TotalRevenue > 1e6 THEN 'Priority Fix'
            WHEN ProfitMargin < 30 THEN 'Watch'
            ELSE 'Healthy' END AS RiskFlag
FROM   vw_territory_performance ORDER BY TotalRevenue DESC;


-- Q4: Channel comparison by year
SELECT * FROM vw_channel_comparison;


-- Q5: Discount impact by category
SELECT Category, Subcategory, AvgDiscountRate,
       ProfitMarginPct, DiscountToRevenuePct, DiscountFlag
FROM   vw_discount_impact ORDER BY DiscountToRevenuePct DESC;


-- Q6: Top 20 CLV customers
SELECT CLV_Rank, CustomerName, CustomerSegment, CLV_Tier,
       ROUND(TotalRevenue,0) AS Revenue, TotalOrders,
       CustomerLifetimeDays, ROUND(RevenuePerDay,2) AS RevPerDay
FROM   vw_customer_lifetime_value WHERE CLV_Rank <= 20 ORDER BY CLV_Rank;


-- Q7: RFM segment summary
SELECT RFM_Segment, RecommendedAction,
       COUNT(*) AS Customers,
       ROUND(SUM(Monetary)/1e6,2) AS RevenueMillion
FROM   vw_rfm_analysis
GROUP  BY RFM_Segment, RecommendedAction ORDER BY RevenueMillion DESC;


-- Q8: Product sales velocity
SELECT VelocityRank, Product, Category, VelocityTier,
       ROUND(AvgMonthlyRevenue,0) AS AvgMonthlyRev, ActiveMonths
FROM   vw_product_sales_velocity ORDER BY VelocityRank LIMIT 20;


-- Q9: Age group analysis
SELECT AgeGroup,
       COUNT(DISTINCT CustomerKey) AS Customers,
       ROUND(SUM(TotalRevenue)/1e6,2) AS RevenueMillion,
       ROUND(AVG(AvgOrderValue),2) AS AvgOrderValue
FROM   vw_customer_segments GROUP BY AgeGroup ORDER BY RevenueMillion DESC;


-- Q10: Monthly trend
SELECT Year, Month, MonthShort,
       SUM(CASE WHEN Channel='Internet' THEN Revenue ELSE 0 END) AS Internet,
       SUM(CASE WHEN Channel='Reseller' THEN Revenue ELSE 0 END) AS Reseller
FROM   vw_monthly_trend GROUP BY Year, Month, MonthShort ORDER BY Year, Month;


-- ================================================================
-- SECTION 8  DATA DICTIONARY
-- ================================================================
DROP VIEW IF EXISTS vw_data_dictionary;
CREATE VIEW vw_data_dictionary AS
SELECT 'fact_internet_sales' AS TableName,'Profit'         AS Column,'SalesAmount - TotalProductCost'                AS Definition,'Derived' AS Type UNION ALL
SELECT 'fact_internet_sales',             'ProfitMargin',            'Profit / SalesAmount * 100',                   'Derived' UNION ALL
SELECT 'fact_internet_sales',             'DiscountRate',            'DiscountAmount / SalesAmount * 100',           'Derived' UNION ALL
SELECT 'fact_internet_sales',             'UnitPrice',               'SalesAmount / SalesOrderLineNumber',           'Derived' UNION ALL
SELECT 'fact_internet_sales',             'OrderDateKey',            'YYYYMMDD INT for fast DimDate join',           'Derived' UNION ALL
SELECT 'fact_internet_sales',             'SalesOrderNumber',        'Degenerate Dimension — order ref on fact',    'Source'  UNION ALL
SELECT 'fact_reseller_sales',             'ResellerMargin',          'Profit / TotalProductCost * 100',             'Derived' UNION ALL
SELECT 'dim_customer',                    'CustomerSK',              'Surrogate key — warehouse internal PK',       'Derived' UNION ALL
SELECT 'dim_customer',                    'AgeGroup',                '18-25 | 26-35 | 36-45 | 46-55 | 55+',       'Derived' UNION ALL
SELECT 'dim_customer',                    'CustomerSegment',         'Budget | Standard | Premium | Luxury',       'Derived' UNION ALL
SELECT 'dim_customer',                    'LoyaltySegment',          'New | Regular | Loyal',                      'Derived' UNION ALL
SELECT 'dim_customer',                    'RFM_Segment',             'Champions | Loyal | Potential | At Risk | Lost', 'Derived' UNION ALL
SELECT 'dim_customer',                    'IsCurrent',               '1=active SCD2 row | 0=historical',          'SCD2'    UNION ALL
SELECT 'dim_product',                     'PriceBand',               'Low <$200 | Mid $200-$1000 | High >$1000',  'Derived' UNION ALL
SELECT 'dim_product',                     'ProductTier',             'Entry | Standard | Premium | Flagship',     'Derived' UNION ALL
SELECT 'dim_date',                        'FiscalYear',              'FY Jul-Jun (FY2013=Jul13-Jun14)',            'Derived' UNION ALL
SELECT 'dim_date',                        'IsHoliday',               '1=US public holiday',                       'Derived' UNION ALL
SELECT 'dim_date',                        'IsWeekend',               '1=Sat/Sun | 0=weekday',                     'Derived';


-- Schema overview
SELECT table_name AS `Table`, table_type AS Type,
       COALESCE(table_rows,0) AS ApproxRows
FROM   information_schema.tables
WHERE  table_schema = 'adventureworks_dw'
ORDER  BY table_type, table_name;
