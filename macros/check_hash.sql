{% macro check_hash(stgdatabase, stgschema, sourcetable,targettable) %}

  SELECT COUNT(*) FROM {{ stgdatabase }}.{{ stgschema }}.{{ sourcetable }} A
  FULL OUTER JOIN  {{ targettable }} B
  ON A.HASH_KEY=B.HASH_KEY
  WHERE A.HASH_KEY IS NULL OR B.HASH_KEY IS NULL

{% endmacro %}
