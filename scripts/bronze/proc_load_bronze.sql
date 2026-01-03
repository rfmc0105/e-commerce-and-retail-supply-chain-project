/*
=============================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=============================================================
Script Purpose:
  This stored procedure loads data from source CSV files into the Bronze layer tables.
  It performs the following steps for each table:
    1. Truncates the target Bronze table to remove existing data.
    2. Loads data from the corresponding CSV file into the Bronze table using BULK INSERT.
    3. Logs the start time, end time, and duration of each load operation.
    4. Handles errors gracefully and logs error details if any operation fails.

Parameters: None
Return Value: None

Usage Example:
  EXEC bronze.load_bronze;
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
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
            PRINT 'Loading data into Bronze layer';
            PRINT '================================';
            PRINT 'Bronze layer loading started at ' + CONVERT(NVARCHAR, @batchStartTime, 120);
        
            -- Load products table
                SET @startTime = GETDATE();
                    PRINT 'Loading bronze.products started at ' + CONVERT(NVARCHAR, @startTime, 120);

                    -- Truncate the products table
                    TRUNCATE TABLE bronze.products;

                    -- Load data from CSV file into products table
                    BULK INSERT bronze.products
                    FROM 'D:\LocalDocs\KaggleData\datasets\rajhkumarr\e-commerce-and-retail-supply-chain\products.csv'
                    WITH (
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '0x0a',
                        TABLOCK
                    );

                SET @endTime = GETDATE();
                    SET @duration = DATEDIFF(SECOND, @startTime, @endTime);
                    PRINT 'Loading bronze.products completed at '
                        + CONVERT(NVARCHAR, @endTime, 120)
                        + ' (Duration: ' + CAST(@duration AS NVARCHAR) + ' seconds)';
                    PRINT '----------------';

            -- Load suppliers table
                SET @startTime = GETDATE();
                    PRINT 'Loading bronze.suppliers started at ' + CONVERT(NVARCHAR, @startTime, 120);

                    -- Truncate the suppliers table
                    TRUNCATE TABLE bronze.suppliers;

                    -- Load data from CSV file into suppliers table
                    BULK INSERT bronze.suppliers
                    FROM 'D:\LocalDocs\KaggleData\datasets\rajhkumarr\e-commerce-and-retail-supply-chain\suppliers.csv'
                    WITH (
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '0x0a',
                        TABLOCK
                    );

                SET @endTime = GETDATE();
                    SET @duration = DATEDIFF(SECOND, @startTime, @endTime);
                    PRINT 'Loading bronze.suppliers completed at '
                        + CONVERT(NVARCHAR, @endTime, 120)
                        + ' (Duration: ' + CAST(@duration AS NVARCHAR) + ' seconds)';
                    PRINT '----------------';
        
            -- Load sales table
                SET @startTime = GETDATE();
                    PRINT 'Loading bronze.sales started at ' + CONVERT(NVARCHAR, @startTime, 120);
                    
                    -- Truncate the sales table
                    TRUNCATE TABLE bronze.sales;

                    -- Load data from CSV file into sales table
                    BULK INSERT bronze.sales
                    FROM 'D:\LocalDocs\KaggleData\datasets\rajhkumarr\e-commerce-and-retail-supply-chain\sales.csv'
                    WITH (
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '0x0a',
                        TABLOCK
                    );

                SET @endTime = GETDATE();
                    SET @duration = DATEDIFF(SECOND, @startTime, @endTime);
                    PRINT 'Loading bronze.sales completed at '
                        + CONVERT(NVARCHAR, @endTime, 120)
                        + ' (Duration: ' + CAST(@duration AS NVARCHAR) + ' seconds)';
                    PRINT '----------------';

            -- Load purchase_orders table
                SET @startTime = GETDATE();
                    PRINT 'Loading bronze.purchase_orders started at ' + CONVERT(NVARCHAR, @startTime, 120);

                    -- Truncate the purchase_orders table
                    TRUNCATE TABLE bronze.purchase_orders;

                    -- Load data from CSV file into purchase_orders table
                    BULK INSERT bronze.purchase_orders
                    FROM 'D:\LocalDocs\KaggleData\datasets\rajhkumarr\e-commerce-and-retail-supply-chain\purchase_orders.csv'
                    WITH (
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '0x0a',
                        TABLOCK
                    );

                SET @endTime = GETDATE();
                    SET @duration = DATEDIFF(SECOND, @startTime, @endTime);
                    PRINT 'Loading bronze.purchase_orders completed at '
                        + CONVERT(NVARCHAR, @endTime, 120)
                        + ' (Duration: ' + CAST(@duration AS NVARCHAR) + ' seconds)';
                    PRINT '----------------';

            -- Load daily_inventory table
                SET @startTime = GETDATE();
                    PRINT 'Loading bronze.daily_inventory started at ' + CONVERT(NVARCHAR, @startTime, 120);
                    
                    -- Truncate the daily_inventory table
                    TRUNCATE TABLE bronze.daily_inventory;

                -- Load data from CSV file into daily_inventory table
                BULK INSERT bronze.daily_inventory
                FROM 'D:\LocalDocs\KaggleData\datasets\rajhkumarr\e-commerce-and-retail-supply-chain\daily_inventory.csv'
                WITH (
                    FIRSTROW = 2,
                    FIELDTERMINATOR = ',',
                    ROWTERMINATOR = '0x0a',
                    TABLOCK
                );

                SET @endTime = GETDATE();
                    SET @duration = DATEDIFF(SECOND, @startTime, @endTime);
                    PRINT 'Loading bronze.daily_inventory completed at '
                        + CONVERT(NVARCHAR, @endTime, 120)
                        + ' (Duration: ' + CAST(@duration AS NVARCHAR) + ' seconds)';
                    PRINT '----------------';

            -- Load inventory_snapshot table
                SET @startTime = GETDATE();
                    PRINT 'Loading bronze.inventory_snapshot started at ' + CONVERT(NVARCHAR, @startTime, 120);
                    
                    -- Truncate the inventory_snapshot table
                    TRUNCATE TABLE bronze.inventory_snapshot;
                    
                    -- Load data from CSV file into inventory_snapshot table
                    BULK INSERT bronze.inventory_snapshot
                    FROM 'D:\LocalDocs\KaggleData\datasets\rajhkumarr\e-commerce-and-retail-supply-chain\inventory_snapshot.csv'
                    WITH (
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '0x0a',
                        TABLOCK
                    );

                SET @endTime = GETDATE();
                    SET @duration = DATEDIFF(SECOND, @startTime, @endTime);
                    PRINT 'Loading bronze.inventory_snapshot completed at '
                        + CONVERT(NVARCHAR, @endTime, 120)
                        + ' (Duration: ' + CAST(@duration AS NVARCHAR) + ' seconds)';
                    PRINT '----------------';

        SET @batchEndTime = GETDATE();
            SET @duration = DATEDIFF(SECOND, @batchStartTime, @batchEndTime);
            PRINT '================================';
            PRINT 'Bronze layer loading completed at '
                + CONVERT(NVARCHAR, @batchEndTime, 120)
                + ' (Total Duration: ' + CAST(@duration AS NVARCHAR) + ' seconds)';

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
 