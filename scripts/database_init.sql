/*
====================================================================================================
Create Database and Schemas
====================================================================================================

Script Purpose:
  This script creates a new database named 'Ecommerce_Retail_SupplyChain' after checking if it already exists.
  If the database exists, it drops the existing database and creates a new one.
  Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

Warning:
  This script will drop the existing 'Ecommerce_Retail_SupplyChain' database if it exists, resulting in the loss of all data within it.
  Ensure that you have backed up any important data before running this script.
*/

USE master;
GO

-- Check if the database exists and drop it to start fresh
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'Ecommerce_Retail_SupplyChain')
BEGIN
    ALTER DATABASE [Ecommerce_Retail_SupplyChain] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [Ecommerce_Retail_SupplyChain];
END
GO

-- Create the new database
CREATE DATABASE [Ecommerce_Retail_SupplyChain];
GO

USE [Ecommerce_Retail_SupplyChain];
GO

-- Create schemas for Medallion Architecture
CREATE SCHEMA [bronze];
GO

CREATE SCHEMA [silver];
GO

CREATE SCHEMA [gold];
GO

-- End of script
