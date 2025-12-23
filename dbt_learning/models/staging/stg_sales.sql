-- models/staging/stg_sales.sql
/*
# 売上データの基本整形

## 目的
- raw_salesテーブルから必要なカラムを抽出
- incrementalモデルで差分更新を実現
- 2018年以降のデータのみを対象

## 依存関係
- source: raw_sales

## 更新頻度
- 日次

## テスト
- transaction_id: unique, not_null
- order_date: not_null
- amount: not_null

## 注意事項
- transaction_idをunique_keyとして使用
- 差分更新のため、order_dateでフィルタ
*/

{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

-- 以下既存のSQLコード...

SELECT
    transaction_id,
    order_date,
    channel,
    customer_segment,
    product_category,
    amount,
    CURRENT_TIMESTAMP() AS processed_at
FROM {{ source('dbt_learning', 'raw_sales') }}
WHERE order_date >= '2018-01-01'

{% if is_incremental() %}
    -- 差分更新：前回処理以降のデータのみ
    AND order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}