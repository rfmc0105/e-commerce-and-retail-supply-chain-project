/*
===============================================================================
DDL Script: Bronze Layer Initialization
===============================================================================
Description:
    This script defines the schema for the 'bronze' layer, serving as the 
    Initial Landing Zone for raw data ingestion. 

    The design philosophy follows the Medallion Architecture, ensuring that 
    data is persisted in its original format (Source-of-truth) before any 
    downstream transformations or business logic applications.

Usage:
    - Execute this script to set up the staging environment for CSV ingestion.
    - This script is designed to be idempotent (checks for existing objects).

Warning:
    - CAUTION: This operation will DROP existing tables in the 'bronze' schema.
    - All non-persisted data in the staging area will be permanently lost.
    - Intended for development and ETL initialization purposes only.
===============================================================================
*/

-- Drop the products table if it already exists
IF OBJECT_ID('bronze.products', 'U') IS NOT NULL
    DROP TABLE bronze.products;

-- Create the products table
CREATE TABLE bronze.products (
    [sku_id] NVARCHAR(20),              -- Stock Keeping Unit ID
    [product_name] NVARCHAR(100),       -- Name of the product
    [category] NVARCHAR(50),            -- Product category
    [sub_category] NVARCHAR(50),        -- Product sub-category
    [brand] NVARCHAR(50),               -- Brand name
    [product_type] NVARCHAR(50),        -- Type of product
    [size_label] NVARCHAR(20),          -- Size label (e.g., Small, Medium, Large)
    [launch_date] DATE,                 -- Date when the product was launched
    [shelf_life_months] NVARCHAR(10),   -- Shelf life in months
    [parent_sku] NVARCHAR(20),          -- Parent SKU ID
    [default_price] DECIMAL(10,2),      -- Default price of the product
    [primary_supplier_id] NVARCHAR(20), -- Primary supplier ID
    [is_active] NVARCHAR(10),           -- Active status (e.g., 'True', 'False')
    [country_of_origin] NVARCHAR(10),   -- Country where the product is made (e.g., 'US', 'UK')
    [online_only] NVARCHAR(10),         -- Online only product (e.g., 'True', 'False')
    [avg_rating] DECIMAL(3,2),          -- Average rating
    [rating_count] INT,                 -- Number of ratings
    [is_discontinued] NVARCHAR(10)      -- Discontinued status (e.g., 'True', 'False')
);


-- Drop the suppliers table if it already exists
IF OBJECT_ID('bronze.suppliers', 'U') IS NOT NULL
    DROP TABLE bronze.suppliers;

-- Create the suppliers table
CREATE TABLE bronze.suppliers (
    [supplier_id] NVARCHAR(20),          -- Unique supplier ID
    [supplier_name] NVARCHAR(100),       -- Name of the supplier
    [region] NVARCHAR(10),               -- Region where the supplier is located (e.g., 'NA', 'EU')
    [default_shipping_mode] NVARCHAR(50),-- Default shipping mode for the supplier (e.g., Air, Sea, Land)
    [status] NVARCHAR(20),               -- Status of the supplier (e.g., active, inactive)
    [lead_time_category] NVARCHAR(20),   -- Lead time category (e.g., long, medium, short)
    [min_order_qty] INT,                 -- Minimum order quantity for the supplier
    [contract_start_date] DATE           -- Start date of the contract with the supplier
);


-- Drop the sales table if it already exists
IF OBJECT_ID('bronze.sales', 'U') IS NOT NULL
    DROP TABLE bronze.sales;

-- Create the sales table
CREATE TABLE bronze.sales (
    [sale_id] INT,                       -- Unique sale ID
    [order_id] NVARCHAR(20),             -- Order ID
    [date] DATE,                         -- Date of the sale
    [sku_id] NVARCHAR(20),               -- Stock Keeping Unit ID
    [channel] NVARCHAR(20),              -- Channel through which the sale was made (e.g., Amazon, Zalora)
    [quantity] INT,                      -- Quantity of items sold
    [unit_price] DECIMAL(10,2),          -- Unit price of the item
    [promo_flag] BIT,                    -- Flag indicating if a promotion was applied (1 for true, 0 for false)
    [discount_pct] DECIMAL(5,1),         -- Discount percentage applied to the sale
    [event_name] NVARCHAR(100),          -- Name of any promotional event associated with the sale
    [customer_segment_id] INT,           -- Customer segment ID (foreign key to customer_segment table)
    [customer_segment] NVARCHAR(10),     -- Customer segment name (e.g., premium, value)
    [device_type] NVARCHAR(20),          -- Device type used for the purchase (e.g., mobile_app, mobile_web)
    [payment_method] NVARCHAR(20),       -- Payment method used for the transaction (e.g., e_wallet, debit_card)
    [shipping_fee] DECIMAL(10,2),        -- Shipping fee charged for the order
    [voucher_amount] DECIMAL(10,2),      -- Voucher amount applied to the order
    [net_revenue] DECIMAL(10,2),         -- Net revenue after discounts and fees
    [returned_flag] BIT,                 -- Flag indicating if the item was returned (1 for true, 0 for false)
    [quarter_bucket] NVARCHAR(10),       -- Quarter bucket (e.g., 2019Q2)
    [month] DATE                         -- Month of the sale
);


-- Drop the purchase_orders table if it already exists
IF OBJECT_ID('bronze.purchase_orders', 'U') IS NOT NULL
    DROP TABLE bronze.purchase_orders;

-- Create the purchase_orders table
CREATE TABLE bronze.purchase_orders (
    [po_id] NVARCHAR(20),                -- Purchase order ID
    [sku_id] NVARCHAR(20),               -- Product key (foreign key to product table)
    [supplier_id] INT,                   -- Supplier ID (foreign key to supplier table)
    [po_date] DATE,                      -- Order date
    [promised_delivery_date] DATE,       -- Promised delivery date
    [delivery_date] DATE,                -- Actual delivery date
    [order_qty] INT,                     -- Quantity ordered
    [unit_cost] DECIMAL(10,2),           -- Unit cost
    [shipping_mode] NVARCHAR(10),        -- Shipping mode (e.g., Air, Sea, Land)
    [status] NVARCHAR(20),               -- Status of the purchase order (e.g., delivered, pending)
    [incoterm] NVARCHAR(10),             -- Incoterms (e.g., FOB, CIF)
    [currency] NVARCHAR(10),             -- Currency used for the transaction
    [freight_cost] DECIMAL(10,2),        -- Freight cost
    [duty_cost] DECIMAL(10,2)            -- Duty cost
);


-- Drop the daily_inventory table if it already exists
IF OBJECT_ID('bronze.daily_inventory', 'U') IS NOT NULL
    DROP TABLE bronze.daily_inventory;

-- Create the daily_inventory table
CREATE TABLE bronze.daily_inventory (
    [snapshot_date] DATE,            -- Date of the inventory snapshot
    [sku_id] NVARCHAR(20),           -- Stock Keeping Unit ID
    [current_stock] INT,             -- Current stock level
    [daily_sales] INT,               -- Daily sales quantity
    [incoming_stock] INT,            -- Incoming stock quantity
    [warehouse_stock] INT,           -- Stock in warehouse
    [retail_stock] INT,              -- Stock in retail locations
    [amazon_allocated] INT,          -- Stock allocated for Amazon
    [tiktokshop_allocated] INT,      -- Stock allocated for TikTok Shop
    [zalora_allocated] INT,          -- Stock allocated for Zalora
    [reorder_point] INT,             -- Reorder point for the product
    [safety_stock] INT               -- Safety stock level
);


-- Drop the inventory_snapshot table if it already exists
IF OBJECT_ID('bronze.inventory_snapshot', 'U') IS NOT NULL
    DROP TABLE bronze.inventory_snapshot;

-- Create the inventory_snapshot table
CREATE TABLE bronze.inventory_snapshot (
    [snapshot_date] DATE,            -- Date of the inventory snapshot
    [sku_id] NVARCHAR(20),           -- Stock Keeping Unit ID
    [current_stock] INT,             -- Current stock level
    [incoming_stock] INT,            -- Incoming stock quantity
    [stock_age_days] INT,            -- Age of the stock in days
    [warehouse_stock] INT,           -- Stock in warehouse
    [retail_stock] INT,              -- Stock in retail locations
    [amazon_allocated] INT,          -- Stock allocated for Amazon
    [tiktokshop_allocated] INT,      -- Stock allocated for TikTok Shop
    [zalora_allocated] INT,          -- Stock allocated for Zalora
    [reorder_point] INT,             -- Reorder point for the product
    [safety_stock] INT,              -- Safety stock level
    [backorder_qty] INT,             -- Backorder quantity
    [opening_buffer] INT             -- Opening buffer quantity
);