-- This script initializes the database by creating a new database and setting up the necessary configurations and schemas.
-- The database 'master' was chosen as the starting point for this script since it is the default system database in SQL Server.
-- and has the necessary permissions to create new databases.
-- Also, it allows us to ensure that the new database is created in a clean state by dropping it if it already exists.
-- You can't drop a database while you are using it, so we switch to the 'master' database first.

USE master;
GO -- Go is used to signal the end of a batch of SQL statements in SQL Server.

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    -- single user mode kicks all users and connections out of the database to allow for a clean drop since you can't drop a database in use.
    -- ROLLBACK IMMEDIATE ensures that any open transactions are rolled back immediately
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

-- Check if the database and schemas were created successfully
SELECT name AS DatabaseName, state_desc AS State, create_date AS CreationDate
FROM sys.databases
WHERE name = 'DataWarehouse';

--check the schemas
SELECT s.name AS SchemaName, s.principal_id AS PrincipalID, s.schema_id AS SchemaID
FROM sys.schemas s
WHERE s.name IN ('bronze', 'silver', 'gold');