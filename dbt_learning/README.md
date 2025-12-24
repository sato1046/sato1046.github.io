# dbt Learning Project

## 概要
実務で保守していた300行のSQLを、dbtを用いて12モデルに再構築したプロジェクトです。

## 技術スタック
- **データウェアハウス**: BigQuery
- **変換ツール**: dbt 1.11.0
- **CI/CD**: GitHub Actions
- **ドキュメント**: dbt docs + GitHub Pages
- **品質保証**: dbt test (27テスト実装)

## プロジェクト構造
```
dbt_learning/
├── models/
│   ├── staging/          # 生データの整形
│   │   └── stg_sales.sql (incremental)
│   ├── intermediate/     # 各粒度・各切り口での集計
│   │   ├── int_sales_by_year.sql
│   │   ├── int_sales_by_month.sql
│   │   ├── int_sales_by_week.sql
│   │   ├── int_sales_by_day.sql
│   │   ├── int_sales_by_channel.sql
│   │   ├── int_sales_by_category.sql
│   │   └── int_sales_by_segment.sql
│   └── marts/            # 最終成果物
│       └── fct_sales_multi_grain.sql
├── tests/
│   └── generic/          # カスタムテスト
│       ├── test_valid_date_range.sql
│       ├── test_positive_values.sql
│       └── test_valid_values_list.sql
└── .github/
    └── workflows/
        └── dbt_ci.yml    # CI/CD設定
```

## 成果
| 項目 | Before | After | 効果 |
|------|--------|-------|------|
| 処理時間 | 2時間 | 6分（2回目以降） | 96%削減 |
| ファイル構造 | 1ファイル300行 | 12モデル（平均25行） | モジュール化 |
| テスト | 手動テスト（1日） | 27テスト自動実行（5分） | 95%削減 |
| ドキュメント | Excel手動更新（半日） | 自動生成（0分） | 100%削減 |
| 変更時の影響確認 | 目視確認（3日） | Lineage Graph（5分） | 99%削減 |
| 新メンバー引き継ぎ | 1週間 | 5分（Lineage Graph説明） | 99%削減 |

## dbtドキュメント
📚 **公開URL**: https://sato1046.github.io/docs/dbt/

Lineage Graph、テスト結果、モデル詳細を確認できます。

## 関連記事
📝 **[実務SQLをdbt化した10日間 - データエンジニアが語る、ビジネス視点を失わない技術習得法](https://qiita.com/sato1046/items/ccca56cae5f6318f97db)**

10日間の学習ロードマップ、つまづいた点と解決方法、ポートフォリオ公開方法をまとめています。

## セットアップ

### 前提条件
- Python 3.11以上
- GCPアカウント
- BigQueryプロジェクト

### インストール
```bash
# dbt-bigqueryのインストール
pip install dbt-bigquery==1.10.2

# プロジェクトのクローン
git clone https://github.com/sato1046/sato1046.github.io.git
cd sato1046.github.io/dbt_learning

# profiles.ymlの設定（~/.dbt/profiles.yml）
# [設定例は省略]
```

### 実行
```bash
# モデルのビルド
dbt run

# テストの実行
dbt test

# ドキュメントの生成
dbt docs generate
dbt docs serve
```

## 環境構成
- **dev**: 開発環境（個人作業）
- **stg**: ステージング環境（チーム共有）
- **prd**: 本番環境（最高品質のみ）

## ライセンス
MIT License

## 作成者
[@sato1046](https://github.com/sato1046)

---

【使い方】
1. 既存のREADME.mdを開く
2. 「## 関連記事」セクションを追加（成果の表の後あたり）
3. Qiita記事のURLを実際のものに置き換える
4. コミット＆プッシュ
