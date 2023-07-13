{% macro limit_dev_data(column_name, days_of_data=3) %}
{% if target.name == 'default' %}
where {{column_name}} >= dateadd('day',-{{days_of_data}}, current_timestamp)
{% endif %}
{% endmacro %}