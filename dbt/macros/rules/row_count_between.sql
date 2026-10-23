{% test row_count_between(model, min_value=1, max_value=none) %}
select row_count
from (select count(*) as row_count from {{ model }}) counted
where row_count < {{ min_value }}
  {% if max_value is not none %} or row_count > {{ max_value }} {% endif %}
{% endtest %}
