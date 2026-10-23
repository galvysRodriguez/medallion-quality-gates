{#-
    Compares one aggregate across two models, e.g. gold revenue vs. silver order lines:
      expression: sum(net_revenue)   compare_model: ref('slv_x')   compare_expression: sum(amount)
-#}
{% test reconciles_with(model, expression, compare_model, compare_expression=none, tolerance=0.01,
                        filter=none, compare_filter=none) %}
with actual as (
    select coalesce({{ expression }}, 0) as value from {{ model }}
    {% if filter %} where {{ filter }} {% endif %}
),
expected as (
    select coalesce({{ compare_expression or expression }}, 0) as value from {{ compare_model }}
    {% if compare_filter %} where {{ compare_filter }} {% endif %}
)
select actual.value as actual_value, expected.value as expected_value,
       actual.value - expected.value as difference
from actual cross join expected
where abs(actual.value - expected.value) > {{ tolerance }}
{% endtest %}
