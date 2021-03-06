{% materialization stage_insert, default %}

/*-----Get all parameter values from Config file ------*/

{% set stagingtable = config.get('staging') %}
{% set intermediatetable = config.get('intermediate') %}
{% set keyfields = config.get('keycolumns') %}
{% set stgdatabase = config.get('stgdb') %}
{% set stgschema = config.get('stgschema') %}
{% set dupchkfl = config.get('dupchkfl') %}
{% set modelexestrttime = run_started_at.astimezone(modules.pytz.timezone(var("time_zone"))) -%}
{% set odate = config.get('odate') %}
{% set stagingexist = adapter.get_relation(
       database = stgdatabase,
       schema = stgschema,
       identifier = stagingtable)
  %}

{% if dupchkfl == 'Y' %}

  {% if keyfields is none %}

    {{ exceptions.raise_compiler_error("Duplicate Check is enabled. But keycolumns are not defined in config.") }}

  {% endif %}

{% endif %}

/*-----Check if staging table exists----------------*/

{% if  stagingexist is none %}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- call statement() -%}

    {{ audit_insert(invocation_id,odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'STAGING TABLE DOESNOT EXIST', 'F' ) }}

  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ exceptions.raise_compiler_error("Staging Table " ~stagingtable~" doesnot exist") }}

{% endif %}

{% set intermediateexist = adapter.get_relation(
       database = stgdatabase,
       schema = stgschema,
      identifier = intermediatetable)
  %}

/*-------Check if intermediate table exists---------*/

{% if  intermediateexist is none %}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- call statement() -%}

    {{ audit_insert(invocation_id, odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'INTERMEDIATE TABLE DOESNOT EXIST', 'F' ) }}

  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

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

{% if (col|length)  > 2 %}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement() %}

    {{ audit_insert(invocation_id, odate, 'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'SCHEMA MISMATCH BETWEEN STG TABLE AND INT TABLE HAPPENED', 'F' ) }}

  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ exceptions.raise_compiler_error("Column mismatch between Staging and Intermediate table. " ~col | map(attribute="name") | join(', ')~" doesnot exist in intermediate table") }}

{% endif %}

{% if 'HASH_KEY' not in col | map(attribute="name") %}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement() %}

    {{ audit_insert(invocation_id, odate, 'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'COLUMN HASH_KEY NOT EXISTS IN INTERMEDIATE TABLE', 'F' ) }}

  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ exceptions.raise_compiler_error("Column HASH_KEY not exists in "~intermediatetable~". Please add the column and resubmit the model") }}

{% endif %}

{% if 'ETL_RECORDED_TS' not in col | map(attribute="name") %}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement() %}

    {{ audit_insert(invocation_id, odate, 'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'COLUMN ETL_RECORDED_TS NOT EXISTS IN INTERMEDIATE TABLE', 'F' ) }}

  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ exceptions.raise_compiler_error("Column ETL_RECORDED_TS not exists in "~intermediatetable~". Please add the column and resubmit the model") }}

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

{% call statement('main',fetch_result=true) -%}

  INSERT INTO {{stgdatabase}}.{{stgschema}}.{{intermediatetable}}
  ( {{ columns }} )
  {{ sql }}

{% endcall %}

/*---{{ adapter.commit() }}----*/

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

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- call statement() -%}

    {{ audit_insert(invocation_id, odate, 'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'HASH VALUE CHECK FAILED', 'F' ) }}

  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ exceptions.raise_compiler_error("Hash value is not matching while loading. Try rerunning the load again") }}

{% endif %}

/*------Check if duplicate values exsists for passed key values------*/

{% if dupchkfl == 'Y' %}

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

    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {%- call statement() -%}

      {{ audit_insert(invocation_id, odate, 'ABORTED',modelexestrttime, current_timestamp(), 0, 0, 0, 'DUPLICATE VALUE EXISTS IN SOURCE', 'F' ) }}

    {%- endcall -%}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {{ exceptions.raise_compiler_error("Duplicate values exists in source for the key columns "~keyfields~". Please check with source") }}

  {% endif %}

{% endif %}

/*--------Get Counts-------------------------------------------------*/

{% set insertcounts = load_result('main')['data'] %}

{% set insert_counts = insertcounts[0][0] %}

{%- call statement() -%}

  {{ audit_insert(invocation_id, odate, 'SUCCESS', modelexestrttime, current_timestamp(), insert_counts, 0, 0, 'ALL CONTROLS PASSED', 'P' ) }}

{%- endcall -%}

{% do adapter.commit() %}

{{ log("All the defined controls passed. Data got loaded successfully from "~stagingtable~" to "~intermediatetable~".", info=True ) }}

{{ return({'relations': [intermediatetablerelattion]}) }}

{% endmaterialization %}
