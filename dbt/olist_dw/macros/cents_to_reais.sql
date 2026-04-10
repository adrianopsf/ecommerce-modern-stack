{% macro cents_to_reais(column) %}
    round({{ column }}, 2)
{% endmacro %}
