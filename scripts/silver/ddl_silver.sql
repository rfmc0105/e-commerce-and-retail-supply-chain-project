/*
===============================================================================
DDL Script: Silver Layer Initialization
===============================================================================
Description:
    This script defines the schema for the silver layer, following the 
    Medallion Architecture. 

    The Silver layer serves as the cleansed and integrated zone. Tables 
    defined here enforce data integrity, standardize formats (dates, numeric 
    values), and resolve data quality issues identified in the Bronze layer.

    Key transformations at this stage:
    - Data type casting (e.g., NVARCHAR to INT/DECIMAL/DATE).
    - Handling NULL values and duplicates.
    - Standardizing categorical fields.

Usage:
    - Run this script to establish the structural foundation for the 
      Transformation (ETL) process.
    - This script is designed for initialization and environment setup.

Warning:
    - ⚠️ CAUTION: Executing this script will DROP all existing tables in the 'silver' schema.
    - Permanent data loss will occur. Ensure all logic is version-controlled.
===============================================================================
*/

-- Drop the products table if it already exists
IF OBJECT_ID('silver.products', 'U') IS NOT NULL
    DROP TABLE silver.products;

-- Create the products table in the silver layer
CREATE TABLE silver.products (
    [sku_id] NVARCHAR(20) PRIMARY KEY              -- Stock Keeping Unit ID
    ,[product_name] NVARCHAR(100) NOT NULL         -- Name of the product
    ,[category] NVARCHAR(50)                       -- Product category
    ,[sub_category] NVARCHAR(50)                   -- Product sub-category
    ,[brand] NVARCHAR(50)                          -- Brand name
    ,[product_type] NVARCHAR(50)                   -- Type of product
    ,[size_label] NVARCHAR(20)                     -- Size label (e.g., Small, Medium, Large)
    ,[launch_date] DATE                            -- Date when the product was launched
    ,[shelf_life_months] DECIMAL(5,2)
        CONSTRAINT CK_products_shelf_life_months
        CHECK ([shelf_life_months] >= 0)           -- Shelf life in months
    ,[parent_sku] NVARCHAR(20)                     -- Parent SKU ID
    ,[default_price] DECIMAL(10,2)
        CONSTRAINT CK_products_default_price
        CHECK ([default_price] >= 0)               -- Default price of the product
    ,[primary_supplier_id] NVARCHAR(20) NOT NULL   -- Primary supplier ID
    ,[is_active] INT                               -- Active status (1 for True, 0 for False)
    ,[country_of_origin] NVARCHAR(10)              -- Country where the product is made (e.g., 'US', 'UK')
    ,[online_only] INT                             -- Online only product (1 for True, 0 for False)
    ,[avg_rating] DECIMAL(3,2)
        CONSTRAINT CK_products_avg_rating
        CHECK ([avg_rating] BETWEEN 1 AND 5)       -- Average rating
    ,[rating_count] INT
        CONSTRAINT CK_products_rating_count
        CHECK ([rating_count] >= 0)                -- Number of ratings
    ,[is_discontinued] INT                         -- Discontinued status (1 for True, 0 for False)
    ,[dwh_create_date] DATETIME2 DEFAULT GETDATE() -- Record creation timestamp
);

-- Drop the suppliers table if it already exists
IF OBJECT_ID('silver.suppliers', 'U') IS NOT NULL
    DROP TABLE silver.suppliers;

-- Create the suppliers table in the silver layer
CREATE TABLE silver.suppliers (
    [supplier_id] NVARCHAR(20) PRIMARY KEY         -- Unique supplier ID
    ,[supplier_name] NVARCHAR(100) NOT NULL        -- Name of the supplier
    ,[region] NVARCHAR(10) NOT NULL                -- Region where the supplier is located (e.g., 'UK', 'DE')
    ,[default_shipping_mode] NVARCHAR(50) NOT NULL -- Default shipping mode for the supplier (e.g., Air, Sea, Land)
    ,[status] NVARCHAR(20) NOT NULL
        CONSTRAINT CK_suppliers_status
        CHECK ([status] IN ('active', 'inactive')) -- Status of the supplier (e.g., active, inactive)
    ,[lead_time_category] NVARCHAR(50) NOT NULL
        CONSTRAINT CK_suppliers_lead_time_category
        CHECK ([lead_time_category]
        IN ('long', 'medium', 'short'))            -- Lead time category (e.g., long, medium, short)
    ,[min_order_qty] INT
        CHECK ([min_order_qty] >= 0)               -- Minimum order quantity
    ,[contract_start_date] DATE                    -- Date when the contract started
    ,[dwh_create_date] DATETIME2 DEFAULT GETDATE() -- Record creation timestamp
);

-- Drop the sales table if it already exists
IF OBJECT_ID('silver.sales', 'U') IS NOT NULL
    DROP TABLE silver.sales;

-- Create the sales table in the silver layer
CREATE TABLE silver.sales (
    [sale_id] INT PRIMARY KEY                      -- Unique sale ID
    ,[order_id] NVARCHAR(20) NOT NULL              -- Order ID
    ,[date] DATE NOT NULL                          -- Date of the sale
    ,[sku_id] NVARCHAR(20) NOT NULL                -- Stock Keeping Unit ID
    ,[channel] NVARCHAR(50) NOT NULL               -- Sales channel (e.g., TikTok Shop, Zalora)
    ,[quantity] INT CHECK ([quantity] > 0)         -- Quantity sold
    ,[unit_price] DECIMAL(10,2)
        CONSTRAINT CK_sales_unit_price
        CHECK ([unit_price] >= 0)                  -- Unit price at the time of sale
    ,[promo_flag] INT NOT NULL                     -- Promotion applied (1 for True, 0 for False)
    ,[discount_pct] DECIMAL(5,2)
        CONSTRAINT CK_sales_discount_pct
        CHECK ([discount_pct] BETWEEN 0 AND 100)   -- Discount percentage
    ,[event_name] NVARCHAR(100) 
    ,[customer_segment_id] INT
        CONSTRAINT CK_sales_customer_segment_id
        CHECK ([customer_segment_id] IN (0, 1, 2)) -- Customer segment ID (e.g., 0, 1, 2)
    ,[customer_segment] NVARCHAR(100)
        CONSTRAINT CK_sales_customer_segment
        CHECK ([customer_segment]
        IN ('budget', 'value', 'premium'))         -- Customer segment information
    ,[device_type] NVARCHAR(50)                    -- Device type used for the purchase
    ,[payment_method] NVARCHAR(50)                 -- Payment method used
    ,[shipping_fee] DECIMAL(10,2)
        CONSTRAINT CK_sales_shipping_fee
        CHECK ([shipping_fee] >= 0)                -- Shipping fee applied
    ,[voucher_amount] DECIMAL(10,2)
        CONSTRAINT CK_sales_voucher_amount
        CHECK ([voucher_amount] >= 0)              -- Voucher amount applied
    ,[net_revenue] DECIMAL(12,2)
        CONSTRAINT CK_sales_net_revenue
        CHECK ([net_revenue] >= 0)                 -- Net revenue from the sale
    ,[returned_flag] INT NOT NULL                  -- Returned status (1 for True, 0 for False)
    ,[quarter_bucket] NVARCHAR(10)                 -- Quarter bucket (e.g., 2019Q2)
    ,[month] DATE NOT NULL                         -- Month of the sale (1-12)
    ,[dwh_create_date] DATETIME2 DEFAULT GETDATE() -- Record creation timestamp
);

-- Drop the purchase_orders table if it already exists
IF OBJECT_ID('silver.purchase_orders', 'U') IS NOT NULL
    DROP TABLE silver.purchase_orders;

-- Create the purchase_orders table in the silver layer
CREATE TABLE silver.purchase_orders (
    [po_id] NVARCHAR(20) PRIMARY KEY                        -- Purchase Order ID
    ,[sku_id] NVARCHAR(20) NOT NULL                -- Stock Keeping Unit ID
    ,[supplier_id] NVARCHAR(20) NOT NULL           -- Supplier ID
    ,[po_date] DATE NOT NULL                       -- Purchase order date
    ,[promised_delivery_date] DATE                 -- Promised delivery date
    ,[delivery_date] DATE                          -- Actual delivery date
    ,[order_qty] INT
        CONSTRAINT CK_po_order_qty
        CHECK ([order_qty] > 0)                    -- Quantity ordered
    ,[unit_cost] DECIMAL(10,2)
        CONSTRAINT CK_po_unit_cost
        CHECK ([unit_cost] >= 0)                   -- Unit cost of the product
    ,[shipping_mode] NVARCHAR(10) NOT NULL         -- Shipping mode (e.g., Air, Sea, Land)
    ,[status] NVARCHAR(20) NOT NULL
        CONSTRAINT CK_po_status
        CHECK ([status]
        IN ('delivered', 'pending'))               -- Status of the purchase order (e.g., delivered, pending)
    ,[incoterm] NVARCHAR(10)                       -- Incoterms (e.g., FOB, CIF)
    ,[currency] NVARCHAR(10)                       -- Currency used for the transaction
    ,[freight_cost] DECIMAL(10,2)
        CONSTRAINT CK_po_freight_cost
        CHECK ([freight_cost] >= 0)                -- Freight cost
    ,[duty_cost] DECIMAL(10,2)
        CONSTRAINT CK_po_duty_cost
        CHECK ([duty_cost] >= 0)                   -- Duty cost
    ,[dwh_create_date] DATETIME2 DEFAULT GETDATE() -- Record creation timestamp
);

-- Drop the daily_inventory table if it already exists
IF OBJECT_ID('silver.daily_inventory', 'U') IS NOT NULL
    DROP TABLE silver.daily_inventory;

-- Create the daily_inventory table in the silver layer
CREATE TABLE silver.daily_inventory (
    [snapshot_date] DATE NOT NULL                  -- Date of the inventory snapshot
    ,[sku_id] NVARCHAR(20) NOT NULL                -- Stock Keeping Unit ID
    ,[current_stock] INT
        CONSTRAINT CK_di_current_stock
        CHECK ([current_stock] >= 0)               -- Current stock level
    ,[daily_sales] INT
        CONSTRAINT CK_di_daily_sales
        CHECK ([daily_sales] >= 0)                 -- Daily sales quantity
    ,[incoming_stock] INT
        CONSTRAINT CK_di_incoming_stock
        CHECK ([incoming_stock] >= 0)              -- Incoming stock quantity
    ,[warehouse_stock] INT
        CONSTRAINT CK_di_warehouse_stock
        CHECK ([warehouse_stock] >= 0)             -- Stock in warehouse
    ,[retail_stock] INT
        CONSTRAINT CK_di_retail_stock
        CHECK ([retail_stock] >= 0)                -- Stock in retail locations
    ,[amazon_allocated] INT
        CONSTRAINT CK_di_amazon_allocated
        CHECK ([amazon_allocated] >= 0)            -- Stock allocated for Amazon
    ,[tiktokshop_allocated] INT
        CONSTRAINT CK_di_tiktokshop_allocated
        CHECK ([tiktokshop_allocated] >= 0)        -- Stock allocated for TikTok Shop
    ,[zalora_allocated] INT
        CONSTRAINT CK_di_zalora_allocated
        CHECK ([zalora_allocated] >= 0)            -- Stock allocated for Zalora
    ,[reorder_point] INT
        CONSTRAINT CK_di_reorder_point
        CHECK ([reorder_point] >= 0)               -- Reorder point
    ,[safety_stock] INT
        CONSTRAINT CK_di_safety_stock
        CHECK ([safety_stock] >= 0)                -- Safety stock level
    ,[dwh_create_date] DATETIME2 DEFAULT GETDATE() -- Record creation timestamp
);

-- Drop the inventory_snapshot table if it already exists
IF OBJECT_ID('silver.inventory_snapshot', 'U') IS NOT NULL
    DROP TABLE silver.inventory_snapshot;

-- Create the inventory_snapshot table in the silver layer
CREATE TABLE silver.inventory_snapshot (
    [snapshot_date] DATE NOT NULL                  -- Date of the inventory snapshot
    ,[sku_id] NVARCHAR(20) NOT NULL                -- Stock Keeping Unit ID
    ,[current_stock] INT
        CONSTRAINT CK_is_current_stock
        CHECK ([current_stock] >= 0)               -- Current stock level
    ,[incoming_stock] INT
        CONSTRAINT CK_is_incoming_stock
        CHECK ([incoming_stock] >= 0)              -- Incoming stock quantity
    ,[stock_age_days] INT
        CONSTRAINT CK_is_stock_age_days
        CHECK ([stock_age_days] >= 0)              -- Age of the stock in days
    ,[warehouse_stock] INT
        CONSTRAINT CK_is_warehouse_stock
        CHECK ([warehouse_stock] >= 0)             -- Stock in warehouse
    ,[retail_stock] INT
        CONSTRAINT CK_is_retail_stock
        CHECK ([retail_stock] >= 0)                -- Stock in retail locations
    ,[amazon_allocated] INT
        CONSTRAINT CK_is_amazon_allocated
        CHECK ([amazon_allocated] >= 0)            -- Stock allocated for Amazon
    ,[tiktokshop_allocated] INT
        CONSTRAINT CK_is_tiktokshop_allocated
        CHECK ([tiktokshop_allocated] >= 0)        -- Stock allocated for TikTok
    ,[zalora_allocated] INT
        CONSTRAINT CK_is_zalora_allocated
        CHECK ([zalora_allocated] >= 0)            -- Stock allocated for Zalora
    ,[reorder_point] INT
        CONSTRAINT CK_is_reorder_point
        CHECK ([reorder_point] >= 0)               -- Reorder point
    ,[safety_stock] INT
        CONSTRAINT CK_is_safety_stock
        CHECK ([safety_stock] >= 0)                -- Safety stock level
    ,[backorder_qty] INT
        CONSTRAINT CK_is_backorder_qty
        CHECK ([backorder_qty] >= 0)               -- Backorder quantity
    ,[opening_buffer] INT
        CONSTRAINT CK_is_opening_buffer
        CHECK ([opening_buffer] >= 0)              -- Opening buffer quantity
    ,[dwh_create_date] DATETIME2 DEFAULT GETDATE() -- Record creation timestamp
);