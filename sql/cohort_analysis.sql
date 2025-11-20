-- =====================================================
-- コホート分析 - 月次リテンション率
-- 新規顧客の継続率を月次で追跡
-- 対応DB: BigQuery, Presto SQL
-- 
-- 実務での実装内容を反映：
-- - 複数CTEによる段階的データ加工
-- - DATE_TRUNCによる効率的な日付集計
-- - Window関数による累積計算
-- - ピボットテーブル形式での出力
-- =====================================================

WITH first_purchase AS (
  -- 各顧客の初回購入月を特定
  SELECT
    customer_id,
    DATE_TRUNC(MIN(order_date), MONTH) AS cohort_month,
    MIN(order_date) AS first_order_date
  FROM
    orders
  WHERE
    order_status = 'completed'
    AND order_date >= '2023-01-01'
  GROUP BY
    customer_id
),

monthly_orders AS (
  -- 顧客の月次購入履歴
  SELECT
    o.customer_id,
    f.cohort_month,
    DATE_TRUNC(o.order_date, MONTH) AS order_month,
    SUM(o.order_amount) AS monthly_revenue,
    COUNT(DISTINCT o.order_id) AS order_count
  FROM
    orders o
  INNER JOIN
    first_purchase f ON o.customer_id = f.customer_id
  WHERE
    o.order_status = 'completed'
    AND o.order_date >= f.first_order_date
  GROUP BY
    1, 2, 3
),

cohort_data AS (
  -- コホート分析用データの準備
  SELECT
    cohort_month,
    order_month,
    -- コホート月からの経過月数を計算
    DATE_DIFF(order_month, cohort_month, MONTH) AS months_since_first,
    COUNT(DISTINCT customer_id) AS customers,
    SUM(monthly_revenue) AS revenue,
    AVG(monthly_revenue) AS avg_revenue_per_customer
  FROM
    monthly_orders
  GROUP BY
    1, 2, 3
),

cohort_sizes AS (
  -- 各コホートの初期サイズ（月0の顧客数）
  SELECT
    cohort_month,
    customers AS cohort_size
  FROM
    cohort_data
  WHERE
    months_since_first = 0
),

retention_matrix AS (
  -- リテンション率の計算
  SELECT
    c.cohort_month,
    c.months_since_first,
    c.customers,
    s.cohort_size,
    ROUND(c.customers * 100.0 / s.cohort_size, 2) AS retention_rate,
    c.revenue,
    c.avg_revenue_per_customer,
    -- 累積収益
    SUM(c.revenue) OVER (
      PARTITION BY c.cohort_month 
      ORDER BY c.months_since_first
    ) AS cumulative_revenue,
    -- 前月比の変化率
    LAG(c.customers) OVER (
      PARTITION BY c.cohort_month 
      ORDER BY c.months_since_first
    ) AS prev_month_customers,
    CASE
      WHEN LAG(c.customers) OVER (
        PARTITION BY c.cohort_month 
        ORDER BY c.months_since_first
      ) > 0 THEN
        ROUND((c.customers - LAG(c.customers) OVER (
          PARTITION BY c.cohort_month 
          ORDER BY c.months_since_first
        )) * 100.0 / LAG(c.customers) OVER (
          PARTITION BY c.cohort_month 
          ORDER BY c.months_since_first
        ), 2)
      ELSE NULL
    END AS month_over_month_change
  FROM
    cohort_data c
  INNER JOIN
    cohort_sizes s ON c.cohort_month = s.cohort_month
),

-- ピボットテーブル形式での出力（見やすい形式）
pivot_retention AS (
  SELECT
    FORMAT_DATE('%Y-%m', cohort_month) AS cohort,
    MAX(CASE WHEN months_since_first = 0 THEN cohort_size END) AS month_0_users,
    MAX(CASE WHEN months_since_first = 0 THEN 100 END) AS month_0,
    MAX(CASE WHEN months_since_first = 1 THEN retention_rate END) AS month_1,
    MAX(CASE WHEN months_since_first = 2 THEN retention_rate END) AS month_2,
    MAX(CASE WHEN months_since_first = 3 THEN retention_rate END) AS month_3,
    MAX(CASE WHEN months_since_first = 4 THEN retention_rate END) AS month_4,
    MAX(CASE WHEN months_since_first = 5 THEN retention_rate END) AS month_5,
    MAX(CASE WHEN months_since_first = 6 THEN retention_rate END) AS month_6,
    MAX(CASE WHEN months_since_first = 7 THEN retention_rate END) AS month_7,
    MAX(CASE WHEN months_since_first = 8 THEN retention_rate END) AS month_8,
    MAX(CASE WHEN months_since_first = 9 THEN retention_rate END) AS month_9,
    MAX(CASE WHEN months_since_first = 10 THEN retention_rate END) AS month_10,
    MAX(CASE WHEN months_since_first = 11 THEN retention_rate END) AS month_11,
    MAX(CASE WHEN months_since_first = 12 THEN retention_rate END) AS month_12
  FROM
    retention_matrix
  GROUP BY
    cohort_month
  ORDER BY
    cohort_month DESC
)

-- 最終出力: ピボット形式のリテンション率テーブル
SELECT
  cohort,
  month_0_users AS initial_customers,
  -- 各月のリテンション率（%表示）
  CONCAT(CAST(month_0 AS STRING), '%') AS M0,
  CONCAT(CAST(month_1 AS STRING), '%') AS M1,
  CONCAT(CAST(month_2 AS STRING), '%') AS M2,
  CONCAT(CAST(month_3 AS STRING), '%') AS M3,
  CONCAT(CAST(month_4 AS STRING), '%') AS M4,
  CONCAT(CAST(month_5 AS STRING), '%') AS M5,
  CONCAT(CAST(month_6 AS STRING), '%') AS M6,
  CONCAT(CAST(month_7 AS STRING), '%') AS M7,
  CONCAT(CAST(month_8 AS STRING), '%') AS M8,
  CONCAT(CAST(month_9 AS STRING), '%') AS M9,
  CONCAT(CAST(month_10 AS STRING), '%') AS M10,
  CONCAT(CAST(month_11 AS STRING), '%') AS M11,
  CONCAT(CAST(month_12 AS STRING), '%') AS M12,
  -- 3ヶ月後のリテンション率でコホートの質を評価
  CASE
    WHEN month_3 >= 40 THEN '優良コホート'
    WHEN month_3 >= 30 THEN '標準コホート'
    WHEN month_3 >= 20 THEN '要改善コホート'
    ELSE '問題コホート'
  END AS cohort_quality
FROM
  pivot_retention
ORDER BY
  cohort DESC;

-- =====================================================
-- 追加分析: コホート別のLTV予測
-- =====================================================
/*
WITH cohort_ltv AS (
  SELECT
    cohort_month,
    months_since_first,
    customers,
    revenue,
    cumulative_revenue,
    ROUND(cumulative_revenue / cohort_size, 2) AS ltv_per_customer
  FROM
    retention_matrix
  WHERE
    months_since_first <= 12
)
SELECT
  FORMAT_DATE('%Y-%m', cohort_month) AS cohort,
  MAX(CASE WHEN months_since_first = 3 THEN ltv_per_customer END) AS ltv_3m,
  MAX(CASE WHEN months_since_first = 6 THEN ltv_per_customer END) AS ltv_6m,
  MAX(CASE WHEN months_since_first = 12 THEN ltv_per_customer END) AS ltv_12m,
  -- 12ヶ月LTVの予測（6ヶ月データから）
  ROUND(MAX(CASE WHEN months_since_first = 6 THEN ltv_per_customer END) * 1.8, 2) AS predicted_ltv_12m
FROM
  cohort_ltv
GROUP BY
  cohort_month
ORDER BY
  cohort_month DESC;
*/