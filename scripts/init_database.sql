/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This SQL script is designed to initialize and structure a database named 'DataWarehouse'. 
    Its primary function is to check for the database's existence, and if found, to remove and then re-create it, 
    effectively ensuring a clean slate. Following database creation, it proceeds to establish three 
    logical schemas: 'bronze', 'silver', and 'gold'. 

Warning:
    A critical warning is issued that executing this script 
    will result in the complete and irreversible deletion of all data within an existing 'DataWarehouse' database, 
    emphasizing the need for caution and prior backups.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
