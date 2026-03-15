/*
=============================================================
DDL Script: Create Gold Views

Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema) 
    These views are built on top of Silver tables and provide 
    cleaned, standardized, and business-ready data for analytics 
    and reporting.
Warning:
    ⚠️ Running this script will DROP and RECREATE several views.
    ⚠️ All existing views with the same names will be permanently deleted.
    Make sure to back up data before execution in production environments.
Usage:
    These views can be queried directly for analytics and reporting or 
    can be used as sources for further transformations.
=============================================================
*/

-- =============================================
-- Create Dimension: gold.dim_products
-- =============================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT
    p.[sku_id]
    ,p.[product_name]
    ,p.[category]
    ,p.[sub_category]
    ,p.[brand]
    ,p.[product_type]
    ,p.[size_label]
    ,p.[launch_date]
    ,p.[shelf_life_months]
    ,p.[parent_sku]
    ,p.[default_price]
    ,s.[supplier_id]
    ,s.[supplier_name]
    ,p.[is_active]
    ,p.[country_of_origin]
    ,p.[online_only]
    ,p.[avg_rating] AS [average_rating]
    ,p.[rating_count]
    ,p.[is_discontinued]
FROM silver.products p
LEFT JOIN silver.suppliers s
    ON p.[primary_supplier_id] = s.[supplier_id];
GO

-- =============================================
-- Create Dimension: gold.dim_suppliers
-- =============================================
IF OBJECT_ID('gold.dim_suppliers', 'V') IS NOT NULL
    DROP VIEW gold.dim_suppliers;
GO
CREATE VIEW gold.dim_suppliers AS
SELECT
    s.supplier_id
    ,s.[supplier_name]
    ,s.[region] AS [supplier_contry]
    ,s.[default_shipping_mode]
    ,s.[status] AS [supplier_status]
    ,s.[lead_time_category]
    ,s.[min_order_qty] AS [minimum_order_quantity]
    ,t.[total_purchase]
    ,s.[contract_start_date]
FROM silver.suppliers s
LEFT JOIN
(SELECT
    supplier_id
    ,COUNT(po_id) AS total_purchase
FROM silver.purchase_orders
GROUP BY supplier_id
)t
ON t.supplier_id = s.supplier_id;
GO

-- =============================================
-- Create Dimension: gold.dim_date
-- =============================================
IF OBJECT_ID('gold.dim_date', 'V') IS NOT NULL
    DROP VIEW gold.dim_date;
GO
CREATE VIEW gold.dim_date AS
SELECT
    d.[snapshot_date] AS [full_date]
    ,YEAR(d.[snapshot_date]) AS [year]
    ,CASE 
        WHEN DATEPART(QUARTER, d.[snapshot_date]) IN (1, 2) THEN 'H1'
        ELSE 'H2' END AS [half_year]
    ,DATEPART(QUARTER, d.[snapshot_date]) AS [quarter]
    ,MONTH(d.[snapshot_date]) AS [month]
    ,DATENAME(MONTH, d.[snapshot_date]) AS [month_name]
    ,DAY(d.[snapshot_date]) AS [day]
    ,DATEPART(WEEKDAY, d.[snapshot_date]) AS [day_of_week]
    ,DATENAME(WEEKDAY, d.[snapshot_date]) AS [day_of_week_name]
    ,CASE
        WHEN DATEPART(WEEKDAY, d.[snapshot_date]) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday' END AS [week_part]
FROM silver.daily_inventory d;
GO

-- =============================================
-- Create Fact Table: gold.fact_purchase_orders
-- =============================================
IF OBJECT_ID('gold.fact_purchase_orders', 'V') IS NOT NULL
    DROP VIEW gold.fact_purchase_orders;
GO
CREATE VIEW gold.fact_purchase_orders AS
SELECT
    po.[po_id] AS [purchase_order_id]
    ,po.[sku_id]
    ,po.[supplier_id]
    ,s.[supplier_name]
    ,po.[po_date] AS [purchase_order_date]
    ,po.[promised_delivery_date]
    ,po.[delivery_date]
    ,CASE WHEN po.[delivery_date] < po.[promised_delivery_date] THEN 'Early'
        WHEN po.[delivery_date] = po.[promised_delivery_date] THEN 'On Time'
        WHEN po.[delivery_date] > po.[promised_delivery_date] THEN 'Late'
        ELSE 'Unknown' END AS [delivery_status]
    ,po.[order_qty] AS [order_quantity]
    ,po.[unit_cost]
    ,po.[shipping_mode]
    ,CASE WHEN po.[shipping_mode] = s.[default_shipping_mode] THEN 1
        ELSE 0 END AS [is_default_shipping_mode]
    ,po.[status] AS [purchase_order_status]
    ,po.[incoterm]
    ,po.[currency]
    ,po.[freight_cost]
    ,po.[duty_cost]
    ,po.[unit_cost] * po.[order_qty] AS [total_unit_cost]
    ,po.[unit_cost] * po.[order_qty]
        + COALESCE(po.[freight_cost], 0)
        + COALESCE(po.[duty_cost], 0)
        AS [total_landed_cost]
FROM silver.purchase_orders po
LEFT JOIN silver.suppliers s
    ON po.[supplier_id] = s.[supplier_id];
GO

-- =============================================
-- Create Fact Table: gold.fact_sales
-- =============================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT
    s.[sale_id]
    ,s.[order_id]
    ,s.[sku_id]
    ,p.[product_name]
    ,p.[category]
    ,p.[sub_category]
    ,s.[channel] AS [sales_channel]
    ,s.[quantity] AS [quantity_sold]
    ,s.[unit_price]
    ,s.[promo_flag]
    ,s.[discount_pct] AS [discount_percentage]
    ,s.[event_name]
    ,s.[customer_segment_id]
    ,s.[customer_segment]
    ,s.[device_type]
    ,s.[payment_method]
    ,s.[shipping_fee]
    ,s.[voucher_amount]
    ,s.[net_revenue]
    ,s.[returned_flag]
    ,s.[date] AS [order_date]
    ,s.[quarter_bucket]
    ,s.[month] AS [sales_month]
FROM silver.sales s
LEFT JOIN silver.products p
    ON s.[sku_id] = p.[sku_id]
WHERE p.[is_active] = 1;
GO

-- =============================================
-- Create Fact Table: gold.fact_daily_inventory
-- =============================================
IF OBJECT_ID('gold.fact_daily_inventory', 'V') IS NOT NULL
    DROP VIEW gold.fact_daily_inventory;
GO
CREATE VIEW gold.fact_daily_inventory AS
SELECT
    di.[snapshot_date]
    ,di.[sku_id]
    ,p.[product_name]
    ,p.[category]
    ,p.[sub_category]
    ,di.[warehouse_stock]
    ,di.[retail_stock]
    ,di.[amazon_allocated]
    ,di.[tiktokshop_allocated]
    ,di.[zalora_allocated]
    ,di.[reorder_point]
    ,di.[safety_stock]
FROM silver.daily_inventory di
LEFT JOIN silver.products p
    ON di.[sku_id] = p.[sku_id]
WHERE p.[is_active] = 1;
GO