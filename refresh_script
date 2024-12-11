import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit
import json
import datetime
import pandas as pd
from functools import reduce

CATALOG_INTEGRATION_NAME = 'fivetran_catalog_beheld_flier'
EXTERNAL_VOLUME_NAME     = 'fivetran_volume_beheld_flier'

def main(session: snowpark.Session):
    # Fetch a list of tables from the external catalog
    external_catalog_tables = [
        row
        for rs in session.sql(f"select SYSTEM$LIST_ICEBERG_TABLES_FROM_CATALOG('{CATALOG_INTEGRATION_NAME}','',0) as TABLE_LIST").collect()
        for row in json.loads(rs.as_dict()["TABLE_LIST"])
    ]

    # Convert the tables to the appropriate CREATE SCHEMA and CREATE ICEBERG TABLE statements
    statements = [
        sql
        for table in external_catalog_tables
        for sql in [create_schema(table), create_table(table)]
    ]

    # Execute each of the statements and merge the resulting dataframes into one combined dataframe to show the user
    results = reduce(lambda left, right: left.union_all(right), [
        exec(session, statement)
        for statement in statements
    ])

    # Identify any tables that exist in CURRENT_DATABASE() that are not tracked in the external catalog and optionally
    sync_dropped_tables(session, external_catalog_tables, drop=False)
    
    return results.sort(col('statement_timestamp'))

def create_schema(table):
    return f"""
CREATE SCHEMA if not exists {table['namespace']}
EXTERNAL_VOLUME = '{EXTERNAL_VOLUME_NAME}'
CATALOG='{CATALOG_INTEGRATION_NAME}'
"""

def create_table(table):
    return f"""
CREATE OR REPLACE ICEBERG TABLE {table['namespace']}.{table['name']}
EXTERNAL_VOLUME = '{EXTERNAL_VOLUME_NAME}'
CATALOG='{CATALOG_INTEGRATION_NAME}'
CATALOG_NAMESPACE= '{table['namespace']}'
CATALOG_TABLE_NAME = '{table['name']}'
AUTO_REFRESH=TRUE;
"""

def exec_and_aggregate_results(session: snowpark.Session, dataframe: snowpark.DataFrame, sql: str) -> snowpark.DataFrame:
    results = session.sql(sql)
    results = results.with_column('statement_timestamp', lit(datetime.datetime.now()))
    results = results.with_column('statement', lit(sql))
    
    return dataframe.union_all(results) if dataframe else results

def exec(session: snowpark.Session, sql: str) -> snowpark.DataFrame:
    results = session.sql(sql)
    results = results.with_column('statement_timestamp', lit(datetime.datetime.now()))
    results = results.with_column('statement', lit(sql))
    return results

def sync_dropped_tables(session: snowpark.Session, external_catalog_tables: list, drop=False):
    all_tables_df = session.sql("""
SELECT CONCAT(table_schema, '.', table_name) AS FQTN, table_schema, table_name  FROM INFORMATION_SCHEMA.TABLES WHERE table_catalog = CURRENT_DATABASE() and table_schema NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
""").toPandas()

    external_catalog_tables_df = pd.DataFrame.from_dict(external_catalog_tables) 
    external_catalog_tables_df['FQTN'] = external_catalog_tables_df["namespace"].str.upper() + "." + external_catalog_tables_df["name"].str.upper()

    tables_to_drop = all_tables_df.merge(external_catalog_tables_df, on='FQTN', how='left', indicator=True)
    tables_to_drop = tables_to_drop[tables_to_drop['_merge'] == 'left_only'].drop(columns=['_merge'])
    drop_statements = tables_to_drop["FQTN"].map(lambda fqtn: f"""DROP TABLE {fqtn}""")
    
    for sql in drop_statements:
        if drop:
            session.sql(sql)
            print(f"Dropped orphan table: {sql}")
        else:
            print("Orphan table detected. Run the following to drop it:")
            print(sql)
