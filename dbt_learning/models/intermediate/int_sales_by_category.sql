-- models/intermediate/int_sales_by_category.sql
-- 商品カテゴリ別売上集計

SELECT
    product_category,
    channel,
    customer_segment,
    SUM(amount) AS total_sales,
    COUNT(transaction_id) AS transaction_count,
    AVG(amount) AS avg_sales
FROM {{ ref('stg_sales') }}
GROUP BY 1, 2, 3