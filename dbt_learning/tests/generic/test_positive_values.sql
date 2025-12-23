{% test positive_values(model, column_name) %}

-- 負の値やゼロを検出（売上などに使用）
select *
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}