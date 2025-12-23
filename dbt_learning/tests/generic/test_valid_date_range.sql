{% test valid_date_range(model, column_name, start_date, end_date) %}

-- 指定範囲外の日付を検出
select *
from {{ model }}
where {{ column_name }} < '{{ start_date }}'
   or {{ column_name }} > '{{ end_date }}'

{% endtest %}