-- models/stg_customers_incremental.sql
-- 顧客データのincremental版

{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

SELECT
    customer_id,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) AS full_name,
    LOWER(email) AS email_lower,
    CURRENT_TIMESTAMP() AS processed_at
FROM {{ source('dbt_learning', 'raw_customers') }}

{% if is_incremental() %}
    -- 2回目以降：最終処理日時より新しいデータのみ
    WHERE CURRENT_TIMESTAMP() > (SELECT MAX(processed_at) FROM {{ this }})
{% endif %}