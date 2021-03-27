{% macro check_duplicates(stgdatabase, stgschema, key_columns) %}

  SELECT {{ key_columns }},COUNT(*) FROM
  {{ stgdatabase }}.{{ stgschema }}.{{ this.table }}
  GROUP BY {{ key_columns }} HAVING COUNT(*) > 1

{% endmacro %}
