# catalogrefresh

This script automates the synchronization of Iceberg tables in Snowflake with an external catalog -- in this case, Fivetran Hosted Polaris. 
Use this script to ensure that Snowflake is synchronized with the Fivetran Hosted Polaris Catalog by automating schema and table creation, logging all actions for auditing, and detecting and optionally cleaning up orphan tables.

The functionality is as follows: 

### 1. Fetch External Catalog Information:
  - Retrieves a list of Iceberg tables from the external catalog using SYSTEM$LIST_ICEBERG_TABLES_FROM_CATALOG.
  - Generate SQL Statements:

### 2. Constructs SQL commands to:
  - Create schemas (CREATE SCHEMA) if they don’t already exist.
  - Create or replace Iceberg tables (CREATE OR REPLACE ICEBERG TABLE) with auto-refresh enabled.

### 3. Execute and Track SQL Statements:
  - Executes the generated SQL statements one by one.
  - Logs the executed statements and their execution timestamps for tracking purposes.
  - Combines the results into a unified DataFrame for easy visibility.

### 4. Identify Orphan Tables:
  - Compares tables listed in the external catalog with tables in the current Snowflake database.
  - Identifies "orphan" tables (i.e., tables present in Snowflake but not tracked in the external catalog).
  - Option to Drop Orphan Tables:
    - If specified (drop=True), the script will drop these orphan tables.
    - Otherwise, it simply prints the SQL commands needed to drop the orphan tables for manual execution.

### 5. Final Output:
  - Returns a sorted DataFrame showing the execution timestamps and SQL statements executed.











