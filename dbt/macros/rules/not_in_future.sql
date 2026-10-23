{% test not_in_future(model, column_name, tolerance_minutes=5) %}
select *
from {{ model }}
where {{ column_name }} > now() + interval '{{ tolerance_minutes }} minutes'
{% endtest %}
