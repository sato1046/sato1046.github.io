{% test valid_values_list(model, column_name, valid_values) %}

-- 許可されたカテゴリ以外を検出
select *
from {{ model }}
where {{ column_name }} not in (
    {% for value in valid_values %}
        '{{ value }}'{% if not loop.last %},{% endif %}
    {% endfor %}
)

{% endtest %}