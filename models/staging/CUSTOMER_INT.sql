{{ config (materialized='stage_insert', odate=var('odate'), staging='CUSTOMER', intermediate='CUSTOMER_INT', stgdb='DBT_POC_ETL_DB', stgschema='DBT_POC_ETL_SCHEMA',audit='M_ETL_AUDIT_CHECK', dupchkfl='Y', keycolumns='C_CUSTKEY', bind=False) }}

SELECT
C_CUSTKEY,
C_NAME,
C_ADDRESS,
C_NATIONKEY,
C_PHONE,
C_ACCTBAL,
C_MKTSEGMENT,
C_COMMENT,
CAST({{ tz_timestamp(current_timestamp()) }} AS TIMESTAMP) AS ETL_RECORDED_TS,
{{ hash_dynamic('DBT_POC_ETL_DB','DBT_POC_ETL_SCHEMA','CUSTOMER') }}
FROM {{ source('STG','CUSTOMER') }}
