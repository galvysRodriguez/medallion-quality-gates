{% test matches_pattern(model, column_name, pattern) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }}::text !~ '{{ pattern }}'
{% endtest %}
