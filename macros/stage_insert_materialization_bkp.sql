{% materialization stage_insert_bkp, default %}

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
{% set intermediatetablerelattion = api.Relation.create(
       database = stgdatabase,
       schema = stgschema,
       identifier = intermediatetable) -%}
%}
{% set columnlist = adapter.get_columns_in_relation(intermediatetablerelattion) %}

{% set columns = columnlist | map(attribute="name") | join(', ') %}

{% call statement('main') -%}

  INSERT INTO {{stgdatabase}}.{{stgschema}}.{{intermediatetable}}
  ( {{ columns }} )
  {{ sql }}

{% endcall %}


{% endmaterialization %}
