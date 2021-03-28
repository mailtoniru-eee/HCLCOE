{% materialization stage_insert, default %}

/*-----Get all parameter values from Config file ------*/

{% set stagingtable = config.get('staging') %}
{% set intermediatetable = config.get('intermediate') %}
{% set keyfields = config.get('keycolumns') %}
{% set stgdatabase = config.get('stgdb') %}
{% set stgschema = config.get('stgschema') %}
{% set modelexestrttime = run_started_at.astimezone(modules.pytz.timezone("America/Chicago")) -%}
{% set odate = config.get('odate') %}
{% set stagingexist = adapter.get_relation(
       database = stgdatabase,
       schema = stgschema,
       identifier = stagingtable)
  %}

/*-----Check if staging table exists----------------*/

{% if  stagingexist is none %}

  {{ audit_insert(odate,'ABORTED',modelexestrttime, current_timestamp(), 0, 0, 0, 'STAGING TABLE DOESNOT EXIST', 'F' ) }}

  {{ exceptions.raise_compiler_error("Staging Table " ~stagingtable~" doesnot exist") }}

{% endif %}

{% set intermediateexist = adapter.get_relation(
       database = stgdatabase,
       schema = stgschema,
      identifier = intermediatetable)
  %}

/*-------Check if intermediate table exists---------*/

{% if  intermediateexist is none %}

    {{ audit_insert(odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'INTERMEDIATE TABLE DOESNOT EXIST', 'F' ) }}

    {{ exceptions.raise_compiler_error("Intermediate Table " ~intermediatetable~" doesnot exist") }}

{% endif %}

{% set intermediatetablerelattion = api.Relation.create(
       database = stgdatabase,
       schema = stgschema,
       identifier = intermediatetable) -%}
%}

{% set stagingtablerelattion = api.Relation.create(
       database = stgdatabase,
       schema = stgschema,
       identifier = stagingtable) -%}
%}

/*---------Check if there is mismatch between staging table and intermediate table without Hash Key-----*/

{% set col = adapter.get_missing_columns(intermediatetablerelattion, stagingtablerelattion) %}

{% if (col|length)  > 1 %}

  {{ audit_insert(odate, 'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'SCHEMA MISMATCH BETWEEN STG TABLE AND INT TABLE HAPPENED', 'F' ) }}

  {{ exceptions.raise_compiler_error("Column mismatch between Staging and Intermediate table. " ~col | map(attribute="name") | join(', ')~" doesnot exist in intermediate table") }}

{% endif %}

{% set columnlist = adapter.get_columns_in_relation(intermediatetablerelattion) %}

{% set columns = columnlist | map(attribute="name") | join(', ') %}

{% call statement() %}

  {{ create_temp_table (stgdatabase, stgschema, stagingtable, intermediatetable) }}

{% endcall %}

/*-------Purge if any data exists in intermediate table------------*/

{% call statement() %}

  {{ purge_data (stgdatabase, stgschema, intermediatetable) }}

{% endcall %}

/*---------Execute the actual insert statement. STG -> INT Table--------*/

{% call statement('main') -%}

  INSERT INTO {{stgdatabase}}.{{stgschema}}.{{intermediatetable}}
  ( {{ columns }} )
  {{ sql }}

{% endcall %}

/*------Check hash value between intermediate table and Temp table-----*/

{% set hash_check %}

  {{ check_hash (stgdatabase, stgschema, intermediatetable, intermediatetable~"_TMP") }}

{% endset %}

{% set results = run_query(hash_check) %}

{% if execute %}

  {% set hash_check_result = results.columns[0].values() %}

{% else %}

  {% set hash_check_result = [] %}

{% endif %}

{% if hash_check_result[0] != 0 %}

  {%- call statement() -%}

    {{ audit_insert(odate, 'ABORTED',modelexestrttime, current_timestamp(), 0, 0, 0, 'HASH VALUE CHECK FAILED', 'F' ) }}

  {%- endcall -%}

  {{ exceptions.raise_compiler_error("Hash value is not matching while loading. Try rerunning the load again") }}

{% endif %}

/*------Check if duplicate values exsists for passed key values------*/

{% set duplicate_check %}

  {{ check_duplicates (stgdatabase, stgschema, keyfields) }}

{% endset %}

{% set dupsresults = run_query(duplicate_check) %}

{% if execute %}

  {% set dups_check_result = dupsresults.columns[0].values() %}

{% else %}

  {% set dups_check_result = [] %}

{% endif %}

{% if dups_check_result|length %}

  {%- call statement() -%}

    {{ audit_insert(odate, 'ABORTED',modelexestrttime, current_timestamp(), 0, 0, 0, 'DUPLICATE VALUE EXISTS IN SOURCE', 'F' ) }}

  {%- endcall -%}

  {{ exceptions.raise_compiler_error("Duplicate values exists in source for the key columns "~keyfields~". Please check with source") }}

{% endif %}

{% endmaterialization %}
