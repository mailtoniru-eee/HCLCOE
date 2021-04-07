{% macro create_temp_table (stgdatabase, stgschema, stgtable, inttable) %}

  CREATE  temporary table {{inttable}}_TMP
  AS SELECT {{ hash_dynamic(stgdatabase, stgschema, stgtable) }},
  A.* FROM {{stgdatabase}}.{{stgschema}}.{{stgtable}} A

{% endmacro %}
