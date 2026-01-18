/*
=============================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=============================================================
Script Purpose:
  This stored procedure loads data from the Bronze layer tables into the Silver layer tables.
  It performs the following steps for each table:
    1. Truncates the target Silver table to remove existing data.
    2. Loads data from the corresponding Bronze table into the Silver table.
    3. Logs the start time, end time, and duration of each load operation.
    4. Handles errors gracefully and logs error details if any operation fails.

Parameters: None
Return Value: None

Usage Example:
  EXEC silver.load_silver;
*/


CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @startTime DATETIME2(0),
        @endTime DATETIME2(0),
        @batchStartTime DATETIME2(0),
        @batchEndTime DATETIME2(0),
        @duration INT;

    BEGIN TRY
        SET @batchStartTime = GETDATE();

            PRINT '================================';
            PRINT 'Loading data into Silver layer';
            PRINT '================================';
            PRINT 'Silver layer loading started at ' + CONVERT(NVARCHAR, @batchStartTime, 120);
            PRINT '----------------';

            -- Load products table
            SET @startTime = GETDATE();
                PRINT 'Loading silver.products started at ' + CONVERT(NVARCHAR, @startTime, 120);

                -- Truncate the products table
                TRUNCATE TABLE silver.products;

                INSERT INTO silver.products (
                    [sku_id]
                    ,[product_name]
                    ,[category]
                    ,[sub_category]
                    ,[brand]
                    ,[product_type]
                    ,[size_label]
                    ,[launch_date]
                    ,[shelf_life_months]
                    ,[parent_sku]
                    ,[default_price]
                    ,[primary_supplier_id]
                    ,[is_active]
                    ,[country_of_origin]
                    ,[online_only]
                    ,[avg_rating]
                    ,[rating_count]
                    ,[is_discontinued]
                )
                SELECT
                    TRIM([sku_id]) AS [sku_id]
                    ,TRIM([product_name]) AS [product_name]
                    ,TRIM([category]) AS [category]
                    ,TRIM([sub_category]) AS [sub_category]
                    ,TRIM([brand]) AS [brand]
                    ,TRIM([product_type]) AS [product_type]
                    ,TRIM([size_label]) AS [size_label]
                    ,CASE 
                        WHEN CAST([launch_date] AS DATE) NOT BETWEEN '2000-01-01' AND GETDATE() THEN NULL
                        ELSE CAST([launch_date] AS DATE)
                    END AS [launch_date]
                    ,[shelf_life_months]
                    ,[parent_sku]
                    ,CASE 
                        WHEN [default_price] < 0 THEN NULL
                        ELSE [default_price]
                    END AS [default_price]
                    ,[primary_supplier_id]
                    ,CASE
                        WHEN TRIM(UPPER([is_active])) = 'TRUE' THEN 1
                        WHEN TRIM(UPPER([is_active])) = 'FALSE' THEN 0
                        ELSE NULL
                    END AS [is_active]
                    ,[country_of_origin]
                    ,CASE
                        WHEN TRIM(UPPER([online_only])) = 'TRUE' THEN 1
                        WHEN TRIM(UPPER([online_only])) = 'FALSE' THEN 0
                        ELSE NULL
                    END AS [online_only]
                    ,CASE 
                        WHEN [avg_rating] < 1 THEN NULL
                        ELSE [avg_rating]
                    END AS [avg_rating]
                    ,CASE 
                        WHEN [rating_count] < 0 THEN NULL
                        ELSE [rating_count]
                    END AS [rating_count]
                    ,CASE
                        WHEN TRIM(UPPER([is_discontinued])) = 'TRUE' THEN 1
                        WHEN TRIM(UPPER([is_discontinued])) = 'FALSE' THEN 0
                        ELSE NULL
                    END AS [is_discontinued]
                FROM bronze.products;

            SET @endTime = GETDATE();
                SET @duration = DATEDIFF(SECOND, @startTime, @endTime);
                PRINT 'Loading silver.products completed at '
                    + CONVERT(NVARCHAR, @endTime, 120)
                    + ' (Duration: ' + CAST(@duration AS NVARCHAR) + ' seconds)';
                PRINT '----------------';

            -- Load suppliers table
            SET @startTime = GETDATE();
                PRINT 'Loading silver.suppliers started at ' + CONVERT(NVARCHAR, @startTime, 120);

                -- Truncate the suppliers table
                TRUNCATE TABLE silver.suppliers;

                INSERT INTO silver.suppliers (
                    [supplier_id]
                    ,[supplier_name]
                    ,[region]
                    ,[default_shipping_mode]
                    ,[status]
                    ,[lead_time_category]
                    ,[min_order_qty]
                    ,[contract_start_date]
                )
                SELECT
                    TRIM([supplier_id]) AS [supplier_id]
                    ,TRIM([supplier_name]) AS [supplier_name]
                    ,TRIM([region]) AS [region]
                    ,TRIM([default_shipping_mode]) AS [default_shipping_mode]
                    ,TRIM([status]) AS [status]
                    ,TRIM([lead_time_category]) AS [lead_time_category]
                    ,CASE 
                        WHEN [min_order_qty] < 0 THEN NULL
                        ELSE [min_order_qty]
                    END AS [min_order_qty]
                    ,CASE 
                        WHEN CAST([contract_start_date] AS DATE) NOT BETWEEN '2000-01-01' AND GETDATE() THEN NULL
                        ELSE CAST([contract_start_date] AS DATE)
                    END AS [contract_start_date]
                FROM bronze.suppliers;

            SET @endTime = GETDATE();
                SET @duration = DATEDIFF(SECOND, @startTime, @endTime);
                PRINT 'Loading silver.suppliers completed at '
                    + CONVERT(NVARCHAR, @endTime, 120)
                    + ' (Duration: ' + CAST(@duration AS NVARCHAR) + ' seconds)';
                PRINT '----------------';
        
            -- Load sales table
            SET @startTime = GETDATE();
                PRINT 'Loading silver.sales started at ' + CONVERT(NVARCHAR, @startTime, 120);
                
                -- Truncate the sales table
                TRUNCATE TABLE silver.sales;

                INSERT INTO silver.sales (
                    [sale_id]
                    ,[order_id]
                    ,[date]
                    ,[sku_id]
                    ,[channel]
                    ,[quantity]
                    ,[unit_price]
                    ,[promo_flag]
                    ,[discount_pct]
                    ,[event_name]
                    ,[customer_segment_id]
                    ,[customer_segment]
                    ,[device_type]
                    ,[payment_method]
                    ,[shipping_fee]
                    ,[voucher_amount]
                    ,[net_revenue]
                    ,[returned_flag]
                    ,[quarter_bucket]
                    ,[month]
                )
                SELECT
                    [sale_id]
                    ,TRIM([order_id]) AS [order_id]
                    ,CASE 
                        WHEN CAST([date] AS DATE) NOT BETWEEN '2000-01-01' AND GETDATE() THEN NULL
                        ELSE CAST([date] AS DATE)
                    END AS [date]
                    ,TRIM([sku_id]) AS [sku_id]
                    ,TRIM([channel]) AS [channel]
                    ,CASE 
                        WHEN [quantity] < 0 THEN NULL
                        ELSE [quantity]
                    END AS [quantity]
                    ,CASE 
                        WHEN [unit_price] < 0 THEN NULL
                        ELSE [unit_price]
                    END AS [unit_price]
                    ,[promo_flag]
                    ,[discount_pct]
                    ,CASE [event_name]
                        WHEN '' THEN NULL
                        ELSE TRIM([event_name])
                    END AS [event_name]
                    ,CASE
                        WHEN [customer_segment_id] NOT IN (0, 1, 2) THEN NULL
                        ELSE [customer_segment_id]
                    END AS [customer_segment_id]
                    ,TRIM([customer_segment]) AS [customer_segment]
                    ,TRIM([device_type]) AS [device_type]
                    ,TRIM([payment_method]) AS [payment_method]
                    ,CASE 
                        WHEN [shipping_fee] < 0 THEN NULL
                        ELSE [shipping_fee]
                    END AS [shipping_fee]
                    ,CASE 
                        WHEN [voucher_amount] < 0 THEN NULL
                        ELSE [voucher_amount]
                    END AS [voucher_amount]
                    ,CASE 
                        WHEN [net_revenue] < 0 THEN NULL
                        ELSE [net_revenue]
                    END AS [net_revenue]
                    ,CASE 
                        WHEN [returned_flag] NOT IN (0, 1) THEN NULL
                        ELSE [returned_flag]
                    END AS [returned_flag]
                    ,CASE 
                        WHEN [quarter_bucket] NOT LIKE '____Q_' THEN NULL
                        ELSE [quarter_bucket]
                    END AS [quarter_bucket]
                    ,CASE 
                        WHEN CAST([month] AS DATE) NOT BETWEEN '2000-01-01' AND GETDATE() THEN NULL
                        ELSE CAST([month] AS DATE)
                    END AS [month]
                FROM bronze.sales;
            SET @endTime = GETDATE();
                SET @duration = DATEDIFF(SECOND, @startTime, @endTime);
                PRINT 'Loading silver.sales completed at '
                    + CONVERT(NVARCHAR, @endTime, 120)
                    + ' (Duration: ' + CAST(@duration AS NVARCHAR) + ' seconds)';
                PRINT '----------------';

            -- Load purchase_orders table
            SET @startTime = GETDATE();
                PRINT 'Loading silver.purchase_orders started at ' + CONVERT(NVARCHAR, @startTime, 120);

                -- Truncate the purchase_orders table
                TRUNCATE TABLE silver.purchase_orders;

                INSERT INTO silver.purchase_orders (
                    [po_id]
                    ,[sku_id]
                    ,[supplier_id]
                    ,[po_date]
                    ,[promised_delivery_date]
                    ,[delivery_date]
                    ,[order_qty]
                    ,[unit_cost]
                    ,[shipping_mode]
                    ,[status]
                    ,[incoterm]
                    ,[currency]
                    ,[freight_cost]
                    ,[duty_cost]
                )

                SELECT
                    TRIM([po_id]) AS [po_id]
                    ,TRIM([sku_id]) AS [sku_id]
                    ,[supplier_id]
                    ,CASE 
                        WHEN CAST([po_date] AS DATE) NOT BETWEEN '2000-01-01' AND GETDATE() THEN NULL
                        ELSE CAST([po_date] AS DATE)
                    END AS [po_date]
                    ,CASE 
                        WHEN CAST([promised_delivery_date] AS DATE) NOT BETWEEN '2000-01-01' AND GETDATE() THEN NULL
                        ELSE CAST([promised_delivery_date] AS DATE)
                    END AS [promised_delivery_date]
                    ,CASE 
                        WHEN CAST([delivery_date] AS DATE) NOT BETWEEN '2000-01-01' AND GETDATE() THEN NULL
                        ELSE CAST([delivery_date] AS DATE)
                    END AS [delivery_date]
                    ,CASE 
                        WHEN [order_qty] < 0 THEN NULL
                        ELSE [order_qty]
                    END AS [order_qty]
                    ,CASE 
                        WHEN [unit_cost] < 0 THEN NULL
                        ELSE [unit_cost]
                    END AS [unit_cost]
                    ,TRIM([shipping_mode]) AS [shipping_mode]
                    ,TRIM([status]) AS [status]
                    ,TRIM([incoterm]) AS [incoterm]
                    ,TRIM([currency]) AS [currency]
                    ,CASE 
                        WHEN [freight_cost] < 0 THEN NULL
                        ELSE [freight_cost]
                    END AS [freight_cost]
                    ,CASE 
                        WHEN [duty_cost] < 0 THEN NULL
                        ELSE [duty_cost]
                    END AS [duty_cost]
                FROM bronze.purchase_orders;

            SET @endTime = GETDATE();
                SET @duration = DATEDIFF(SECOND, @startTime, @endTime);
                PRINT 'Loading silver.purchase_orders completed at '
                    + CONVERT(NVARCHAR, @endTime, 120)
                    + ' (Duration: ' + CAST(@duration AS NVARCHAR) + ' seconds)';
                PRINT '----------------';

            -- Load daily_inventory table
            SET @startTime = GETDATE();
                PRINT 'Loading silver.daily_inventory started at ' + CONVERT(NVARCHAR, @startTime, 120);
                
                -- Truncate the daily_inventory table
                TRUNCATE TABLE silver.daily_inventory;

                INSERT INTO silver.daily_inventory (
                    [snapshot_date]
                    ,[sku_id]
                    ,[current_stock]
                    ,[daily_sales]
                    ,[incoming_stock]
                    ,[warehouse_stock]
                    ,[retail_stock]
                    ,[amazon_allocated]
                    ,[tiktokshop_allocated]
                    ,[zalora_allocated]
                    ,[reorder_point]
                    ,[safety_stock]
                )

                SELECT
                    CASE 
                        WHEN CAST([snapshot_date] AS DATE) NOT BETWEEN '2000-01-01' AND GETDATE() THEN NULL
                        ELSE CAST([snapshot_date] AS DATE)
                    END AS [snapshot_date]
                    ,TRIM([sku_id]) AS [sku_id]
                    ,CASE 
                        WHEN [current_stock] < 0 THEN NULL
                        ELSE [current_stock]
                    END AS [current_stock]
                    ,CASE 
                        WHEN [daily_sales] < 0 THEN NULL
                        ELSE [daily_sales]
                    END AS [daily_sales]
                    ,CASE 
                        WHEN [incoming_stock] < 0 THEN NULL
                        ELSE [incoming_stock]
                    END AS [incoming_stock]
                    ,CASE 
                        WHEN [warehouse_stock] < 0 THEN NULL
                        ELSE [warehouse_stock]
                    END AS [warehouse_stock]
                    ,CASE 
                        WHEN [retail_stock] < 0 THEN NULL
                        ELSE [retail_stock]
                    END AS [retail_stock]
                    ,CASE 
                        WHEN [amazon_allocated] < 0 THEN NULL
                        ELSE [amazon_allocated]
                    END AS [amazon_allocated]
                    ,CASE 
                        WHEN [tiktokshop_allocated] < 0 THEN NULL
                        ELSE [tiktokshop_allocated]
                    END AS [tiktokshop_allocated]
                    ,CASE 
                        WHEN [zalora_allocated] < 0 THEN NULL
                        ELSE [zalora_allocated]
                    END AS [zalora_allocated]
                    ,CASE 
                        WHEN [reorder_point] < 0 THEN NULL
                        ELSE [reorder_point]
                    END AS [reorder_point]
                    ,CASE 
                        WHEN [safety_stock] < 0 THEN NULL
                        ELSE [safety_stock]
                    END AS [safety_stock]
                FROM bronze.daily_inventory;

            SET @endTime = GETDATE();
                SET @duration = DATEDIFF(SECOND, @startTime, @endTime);
                PRINT 'Loading silver.daily_inventory completed at '
                    + CONVERT(NVARCHAR, @endTime, 120)
                    + ' (Duration: ' + CAST(@duration AS NVARCHAR) + ' seconds)';
                PRINT '----------------';

            -- Load inventory_snapshot table
            SET @startTime = GETDATE();
                PRINT 'Loading silver.inventory_snapshot started at ' + CONVERT(NVARCHAR, @startTime, 120);
                
                -- Truncate the inventory_snapshot table
                TRUNCATE TABLE silver.inventory_snapshot;
                
                INSERT INTO silver.inventory_snapshot (
                    [snapshot_date]
                    ,[sku_id]
                    ,[current_stock]
                    ,[incoming_stock]
                    ,[stock_age_days]
                    ,[warehouse_stock]
                    ,[retail_stock]
                    ,[amazon_allocated]
                    ,[tiktokshop_allocated]
                    ,[zalora_allocated]
                    ,[reorder_point]
                    ,[safety_stock]
                    ,[backorder_qty]
                    ,[opening_buffer]
                )

                SELECT
                    CASE 
                        WHEN CAST([snapshot_date] AS DATE) NOT BETWEEN '2000-01-01' AND GETDATE() THEN NULL
                        ELSE CAST([snapshot_date] AS DATE)
                    END AS [snapshot_date]
                    ,TRIM([sku_id]) AS [sku_id]
                    ,CASE 
                        WHEN [current_stock] < 0 THEN NULL
                        ELSE [current_stock]
                    END AS [current_stock]
                    ,CASE 
                        WHEN [incoming_stock] < 0 THEN NULL
                        ELSE [incoming_stock]
                    END AS [incoming_stock]
                    ,CASE 
                        WHEN [stock_age_days] < 0 THEN NULL
                        ELSE [stock_age_days]
                    END AS [stock_age_days]
                    ,CASE 
                        WHEN [warehouse_stock] < 0 THEN NULL
                        ELSE [warehouse_stock]
                    END AS [warehouse_stock]
                    ,CASE 
                        WHEN [retail_stock] < 0 THEN NULL
                        ELSE [retail_stock]
                    END AS [retail_stock]
                    ,CASE 
                        WHEN [amazon_allocated] < 0 THEN NULL
                        ELSE [amazon_allocated]
                    END AS [amazon_allocated]
                    ,CASE 
                        WHEN [tiktokshop_allocated] < 0 THEN NULL
                        ELSE [tiktokshop_allocated]
                    END AS [tiktokshop_allocated]
                    ,CASE 
                        WHEN [zalora_allocated] < 0 THEN NULL
                        ELSE [zalora_allocated]
                    END AS [zalora_allocated]
                    ,CASE 
                        WHEN [reorder_point] < 0 THEN NULL
                        ELSE [reorder_point]
                    END AS [reorder_point]
                    ,CASE 
                        WHEN [safety_stock] < 0 THEN NULL
                        ELSE [safety_stock]
                    END AS [safety_stock]
                    ,CASE 
                        WHEN [backorder_qty] < 0 THEN NULL
                        ELSE [backorder_qty]
                    END AS [backorder_qty]
                    ,CASE 
                        WHEN [opening_buffer] < 0 THEN NULL
                        ELSE [opening_buffer]
                    END AS [opening_buffer]
                FROM bronze.inventory_snapshot;

            SET @endTime = GETDATE();
                SET @duration = DATEDIFF(SECOND, @startTime, @endTime);
                PRINT 'Loading silver.inventory_snapshot completed at '
                    + CONVERT(NVARCHAR, @endTime, 120)
                    + ' (Duration: ' + CAST(@duration AS NVARCHAR) + ' seconds)';
                PRINT '----------------';

        SET @batchEndTime = GETDATE();
            SET @duration = DATEDIFF(SECOND, @batchStartTime, @batchEndTime);
            PRINT '================================';
            PRINT 'Silver layer loading completed at '
                + CONVERT(NVARCHAR, @batchEndTime, 120)
                + ' (Total Duration: ' + CAST(@duration AS NVARCHAR) + ' seconds)';
            PRINT '================================';

    END TRY
    BEGIN CATCH
        PRINT '----------------------------------------------------------------';
        PRINT 'ERROR OCCURRED WHILE LOADING DATA INTO BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
        PRINT '----------------------------------------------------------------';
        PRINT ERROR_MESSAGE();
        THROW;
     END CATCH
END;