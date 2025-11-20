-- =====================================================
-- RFM分析 (Recency, Frequency, Monetary)
-- 顧客を購買行動でセグメント化
-- 対応DB: BigQuery, Presto SQL
-- 
-- 実務での実装内容を反映：
-- - 5段階CTE処理による段階的データ加工
-- - APPROX_QUANTILESによるパーセンタイル計算（パフォーマンス最適化）
-- - Window関数による効率的な集計
-- - 複数条件によるセグメント分類
-- =====================================================

WITH rfm_base AS (
  -- 基本的なRFM指標を計算
  SELECT
    customer_id,
    MAX(order_date) AS last_order_date,
    COUNT(DISTINCT order_id) AS frequency,
    SUM(order_amount) AS monetary,
    -- 最終購入日からの経過日数
    DATE_DIFF(CURRENT_DATE(), MAX(order_date), DAY) AS recency_days
  FROM
    orders
  WHERE
    order_status = 'completed'
    AND order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  GROUP BY
    customer_id
),

rfm_percentiles AS (
  -- パーセンタイルを計算してスコアリング基準を作成
  SELECT
    APPROX_QUANTILES(recency_days, 4)[OFFSET(1)] AS recency_p25,
    APPROX_QUANTILES(recency_days, 4)[OFFSET(2)] AS recency_p50,
    APPROX_QUANTILES(recency_days, 4)[OFFSET(3)] AS recency_p75,
    APPROX_QUANTILES(frequency, 4)[OFFSET(1)] AS frequency_p25,
    APPROX_QUANTILES(frequency, 4)[OFFSET(2)] AS frequency_p50,
    APPROX_QUANTILES(frequency, 4)[OFFSET(3)] AS frequency_p75,
    APPROX_QUANTILES(monetary, 4)[OFFSET(1)] AS monetary_p25,
    APPROX_QUANTILES(monetary, 4)[OFFSET(2)] AS monetary_p50,
    APPROX_QUANTILES(monetary, 4)[OFFSET(3)] AS monetary_p75
  FROM
    rfm_base
),

rfm_scores AS (
  -- 各顧客にRFMスコアを付与 (1-4の4段階)
  SELECT
    b.customer_id,
    b.last_order_date,
    b.recency_days,
    b.frequency,
    b.monetary,
    -- Recencyスコア (低いほど良い = 最近購入)
    CASE
      WHEN b.recency_days <= p.recency_p25 THEN 4
      WHEN b.recency_days <= p.recency_p50 THEN 3
      WHEN b.recency_days <= p.recency_p75 THEN 2
      ELSE 1
    END AS recency_score,
    -- Frequencyスコア (高いほど良い)
    CASE
      WHEN b.frequency >= p.frequency_p75 THEN 4
      WHEN b.frequency >= p.frequency_p50 THEN 3
      WHEN b.frequency >= p.frequency_p25 THEN 2
      ELSE 1
    END AS frequency_score,
    -- Monetaryスコア (高いほど良い)
    CASE
      WHEN b.monetary >= p.monetary_p75 THEN 4
      WHEN b.monetary >= p.monetary_p50 THEN 3
      WHEN b.monetary >= p.monetary_p25 THEN 2
      ELSE 1
    END AS monetary_score
  FROM
    rfm_base b
  CROSS JOIN
    rfm_percentiles p
),

rfm_segments AS (
  -- RFMスコアの組み合わせから顧客セグメントを定義
  SELECT
    *,
    -- スコアを文字列として結合 (例: "444" = 最優良顧客)
    CONCAT(
      CAST(recency_score AS STRING),
      CAST(frequency_score AS STRING),
      CAST(monetary_score AS STRING)
    ) AS rfm_score_combined,
    -- 顧客セグメント分類
    CASE
      -- Champions (最優良顧客)
      WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
      WHEN recency_score >= 3 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
      WHEN recency_score >= 4 AND frequency_score >= 3 AND monetary_score >= 4 THEN 'Champions'
      
      -- Loyal Customers (ロイヤル顧客)
      WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
      
      -- Potential Loyalists (ロイヤル化可能顧客)
      WHEN recency_score >= 3 AND frequency_score >= 2 AND monetary_score >= 3 THEN 'Potential Loyalists'
      WHEN recency_score >= 4 AND frequency_score >= 1 AND monetary_score >= 3 THEN 'Potential Loyalists'
      
      -- Recent Customers (新規顧客)
      WHEN recency_score >= 4 AND frequency_score = 1 THEN 'Recent Customers'
      
      -- At Risk (離脱リスク顧客)
      WHEN recency_score = 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
      
      -- Can't Lose Them (重要だが離脱危機)
      WHEN recency_score <= 2 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Cant Lose Them'
      
      -- Hibernating (休眠顧客)
      WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Hibernating'
      
      -- Lost (離脱顧客)
      WHEN recency_score = 1 THEN 'Lost'
      
      -- その他
      ELSE 'Others'
    END AS customer_segment
  FROM
    rfm_scores
)

-- 最終結果: セグメント別の統計情報
SELECT
  customer_segment,
  COUNT(*) AS customer_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
  ROUND(AVG(recency_days), 1) AS avg_recency_days,
  ROUND(AVG(frequency), 1) AS avg_frequency,
  ROUND(AVG(monetary), 0) AS avg_monetary_value,
  ROUND(SUM(monetary), 0) AS total_monetary_value,
  -- アクションプラン提案
  CASE
    WHEN customer_segment = 'Champions' THEN 'VIPプログラム提供、新商品の先行案内'
    WHEN customer_segment = 'Loyal Customers' THEN 'リファラルプログラム、レビュー依頼'
    WHEN customer_segment = 'Potential Loyalists' THEN 'メンバーシップ提案、クロスセル'
    WHEN customer_segment = 'Recent Customers' THEN 'オンボーディング強化、商品教育'
    WHEN customer_segment = 'At Risk' THEN '特別オファー、フィードバック収集'
    WHEN customer_segment = 'Cant Lose Them' THEN '緊急の再エンゲージメント施策'
    WHEN customer_segment = 'Hibernating' THEN '再活性化キャンペーン'
    WHEN customer_segment = 'Lost' THEN 'ウィンバックキャンペーン'
    ELSE '標準的なマーケティング施策'
  END AS recommended_action
FROM
  rfm_segments
GROUP BY
  customer_segment
ORDER BY
  customer_count DESC;