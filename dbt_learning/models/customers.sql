-- models/customers.sql
-- 顧客データの基本的な変換

SELECT
    customer_id,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) AS full_name,
    LOWER(email) AS email_lower
FROM {{ source('dbt_learning', 'raw_customers') }}