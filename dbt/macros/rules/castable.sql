{% test castable(model, column_name, to) %}
select {{ column_name }} as invalid_value, *
from {{ model }}
where {{ column_name }} is not null
  and not pg_input_is_valid({{ column_name }}::text, '{{ to }}')
{% endtest %}
