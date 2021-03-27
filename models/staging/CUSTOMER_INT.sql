{{ config (materialized='stage_insert_bkp', odate=var('odate'), staging='CUSTOMER', intermediate='CUSTOMER_INT', stgdb='DBT_POC_ETL_DB', stgschema='DBT_POC_ETL_SCHEMA',audit='M_ETL_AUDIT_CHECK', keycolumns='C_CUSTKEY', bind=False) }}

SELECT
C_CUSTKEY,
C_NAME,
C_ADDRESS,
C_NATIONKEY,
C_PHONE,
C_ACCTBAL,
C_MKTSEGMENT,
C_COMMENT,
{{ hash('customer') }}
FROM {{ source('STG','CUSTOMER') }}
