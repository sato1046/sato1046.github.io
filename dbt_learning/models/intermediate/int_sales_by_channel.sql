-- models/intermediate/int_sales_by_channel.sql
/*
# チャネル別売上集計

## 目的
- オンライン/店舗別の売上パフォーマンス分析

## 集計項目
- 合計売上金額
- 取引件数
- 平均売上金額

## 依存関係
- stg_sales

## 更新頻度
- 日次
*/

SELECT
    channel,
    customer_segment,
    product_category,
    SUM(amount) AS total_sales,
    COUNT(transaction_id) AS transaction_count,
    AVG(amount) AS avg_sales
FROM {{ ref('stg_sales') }}
GROUP BY 1, 2, 3