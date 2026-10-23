{#- A NULL result counts as passing: missing references are left to relationships tests. -#}
{% test expression_is_true(model, expression) %}
select *
from {{ model }}
where not ({{ expression }})
{% endtest %}
