-- models/customer_summary.sql
-- 顧客情報のサマリー（ref使用の練習）

WITH customers AS (
    SELECT * FROM {{ ref('customers') }}
)

SELECT
    customer_id,
    full_name,
    email_lower,
    -- メールドメインを抽出
    SPLIT(email_lower, '@')[OFFSET(1)] AS email_domain,
    -- 名前の長さ
    LENGTH(full_name) AS name_length
FROM customers