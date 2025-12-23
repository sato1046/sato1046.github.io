-- models/marts/fct_sales_multi_grain.sql
/*
# 多粒度売上集計

## 目的
- 年月週日別の売上集計を1つのテーブルに統合
- BIツールでの柔軟な分析を可能に

## 依存関係
- int_sales_by_year
- int_sales_by_month
- int_sales_by_week
- int_sales_by_day

## 粒度
- 年別
- 年月別
- 年週別
- 年月日別

## 利用者
- マーケティングチーム
- 経営陣

## 更新頻度
- 日次
*/

-- 年別
SELECT * FROM {{ ref('int_sales_by_year') }}

UNION ALL

-- 月別
SELECT * FROM {{ ref('int_sales_by_month') }}

UNION ALL

-- 週別
SELECT * FROM {{ ref('int_sales_by_week') }}

UNION ALL

-- 日別
SELECT * FROM {{ ref('int_sales_by_day') }}