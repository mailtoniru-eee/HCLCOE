{{ config (materialized='stage_to_integration', odate=var('odate'), stagingtable=['CUSTOMER','ORDERS'], integrationtable='CUSTOMER_ORDER_INT', stgdb='DBT_POC_ETL_DB', stgschema='DBT_POC_ETL_SCHEMA',audit='M_ETL_AUDIT_CHECK', dupchkfl='N', keycolumns='C_CUSTKEY', multiplesrctblfl='Y', bind=False) }}

SELECT
C_CUSTKEY,
C_NAME,
C_ADDRESS,
C_PHONE,
O_ORDERSTATUS,
O_TOTALPRICE,
O_ORDERDATE,
CAST({{ tz_timestamp(current_timestamp()) }} AS TIMESTAMP) AS ETL_RECORDED_TS,
{{ hash_dynamic('DBT_POC_ETL_DB','DBT_POC_ETL_SCHEMA','CUSTOMER_ORDER_INT') }}
FROM {{ source('STG','CUSTOMER') }} A INNER JOIN
{{ source('STG','ORDERS') }} B ON A.C_CUSTKEY=B.O_CUSTKEY
