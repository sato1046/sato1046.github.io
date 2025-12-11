# SQL Analytics Toolkit 📊

実務で使用した分析SQLクエリ集です。BigQuery、Presto、標準SQLに対応。

## 📚 内容

### 1. 売上分析クエリ
- 日次/週次/月次集計
- 前年同期比較
- 移動平均
- コホート分析

### 2. 顧客分析クエリ  
- RFM分析
- LTV計算
- 新規/既存顧客分析
- 離脱予測

### 3. 商品分析クエリ
- ABC分析
- バスケット分析
- 在庫回転率
- クロスセル分析

### 4. パフォーマンス最適化
- Window関数活用
- CTE最適化パターン
- インデックス戦略

## 🛠 技術スタック

- **BigQuery** (Standard SQL)
- **Presto SQL** (Treasure Data)
- **PostgreSQL**

## 📂 ファイル構成

```
sql-analytics-toolkit/
├── sales_analysis/
│   ├── daily_weekly_monthly_aggregation.sql
│   ├── year_over_year_comparison.sql
│   ├── moving_average.sql
│   └── cohort_analysis.sql
├── customer_analysis/
│   ├── rfm_analysis.sql
│   ├── customer_ltv.sql
│   ├── new_existing_customers.sql
│   └── churn_prediction.sql
├── product_analysis/
│   ├── abc_analysis.sql
│   ├── basket_analysis.sql
│   └── cross_sell_analysis.sql
└── optimization/
    ├── window_functions_patterns.sql
    ├── cte_optimization.sql
    └── performance_tips.md
```

## 🚀 使用例

### 売上の前年同期比較
```sql
WITH current_year AS (
  SELECT 
    DATE_TRUNC(date, MONTH) as month,
    SUM(revenue) as revenue
  FROM sales
  WHERE EXTRACT(YEAR FROM date) = 2024
  GROUP BY 1
),
previous_year AS (
  SELECT 
    DATE_TRUNC(date, MONTH) as month,
    SUM(revenue) as revenue
  FROM sales  
  WHERE EXTRACT(YEAR FROM date) = 2023
  GROUP BY 1
)
SELECT 
  c.month,
  c.revenue as current_revenue,
  p.revenue as previous_revenue,
  ROUND((c.revenue - p.revenue) / p.revenue * 100, 2) as growth_rate
FROM current_year c
LEFT JOIN previous_year p
  ON DATE_SUB(c.month, INTERVAL 1 YEAR) = p.month
ORDER BY c.month;
```

## 💡 特徴

- ✅ 実務で検証済みのクエリ
- ✅ パフォーマンス最適化済み
- ✅ コメント付きで理解しやすい
- ✅ 複数のDBエンジン対応

## 📈 実績

- **5段階CTE処理による重複排除**（SFCC Products API連携）
- **複数CTEによる段階的データ加工**（IPOCAレポート作成）
- **ARRAY_AGG/UNNESTによる配列データ処理**（メッシュコード処理）
- **パーティション・クラスター設定**によるパフォーマンス最適化
- **REGEXP_REPLACEによる文字列処理**（地名の正規化）
- **APPROX_QUANTILESによるパーセンタイル計算**（RFM分析）
- **Window関数による累積計算**（コホート分析）
- 500万レコード/日の処理に対応
- クエリ実行時間を平均70%削減
- 4つの大手企業プロジェクトで採用

## 📂 サンプルコード

### SQL分析クエリ

- [RFM分析](./RFM分析SQL%20-%20rfm_analysis.sql) - 顧客セグメンテーション

- [コホート分析](./コホート分析SQL%20-%20cohort_analysis.sql) - 月次リテンション率分析

## 📝 License

MIT License

## 👤 Author

- GitHub: [@sato1046](https://github.com/sato1046)
- Portfolio: [https://sato1046.github.io](https://sato1046.github.io)
