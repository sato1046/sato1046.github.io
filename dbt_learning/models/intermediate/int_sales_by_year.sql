-- models/intermediate/int_sales_by_year.sql
-- 年別売上集計

SELECT
    EXTRACT(YEAR FROM order_date) AS year,
    NULL AS month,
    NULL AS week,
    NULL AS day,
    channel,
    customer_segment,
    product_category,
    SUM(amount) AS total_sales,
    COUNT(transaction_id) AS transaction_count
FROM {{ ref('stg_sales') }}
GROUP BY 1, 2, 3, 4, 5, 6, 7