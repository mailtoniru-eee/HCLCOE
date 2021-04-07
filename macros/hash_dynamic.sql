{% macro hash_dynamic(srcdatabase,srcschema,tablename) %}

{% set sourcetablerelation = api.Relation.create(
       database = srcdatabase,
       schema = srcschema,
       identifier = tablename) -%}

{% set columnlist = adapter.get_columns_in_relation(sourcetablerelation) %}

  {{- "MD5(" -}}
  {% for x in columnlist %}

    {% if x['dtype'] == 'NUMBER' or x['dtype'] =='INT' or x['dtype'] == 'INTEGER' or x['dtype'] =='BIGINT' or x['dtype'] == 'SMALLINT' or x['dtype'] == 'TINYINT' or x['dtype'] == 'BYTEINT' %}

      {% set length = 39 %}

    {% endif %}

    {% if x['dtype'] == 'DECIMAL' or x['dtype'] =='NUMERIC' %}

      {% set length = x['numeric_precision']+1 %}

    {% endif %}

    {% if x['dtype'] == 'FLOAT' or x['dtype'] =='FLOAT4' or x['dtype'] == 'FLOAT8' or x['dtype'] =='DOUBLE' or x['dtype'] == 'DOUBLEPRECISION' or x['dtype'] == 'REAL' %}

      {% set length = 20 %}

    {% endif %}

    {% if x['dtype'] == 'VARCHAR' or x['dtype'] =='CHAR' or x['dtype'] == 'STRING' or x['dtype'] =='TEXT' %}

      {% set length = 16777216 %}

    {% endif %}

    {% if x['dtype'] == 'BINARY' or x['dtype'] =='VARBINARY' %}

      {% set length = 8388608 %}

    {% endif %}

    {% if x['dtype'] == 'BOOLEAN' %}

      {% set length = 5 %}

    {% endif %}

    {% if x['dtype'] == 'DATE' or x['dtype'] =='DATETIME' or x['dtype'] == 'TIME' or x['dtype'] =='TIMESTAMP' or x['dtype'] == 'TIMESTAMP_LTZ' or x['dtype'] == 'TIMESTAMP_NTZ' or x['dtype'] =='TIMESTAMP_TZ' %}

      {% set length = 30 %}

    {% endif %}

    {{ "CAST("~x['column']~" AS VARCHAR("~length~"))"  }}
    {{ "||" if not loop.last }}

  {% endfor %}

{{ ") AS HASH_KEY" }}

{% endmacro %}
