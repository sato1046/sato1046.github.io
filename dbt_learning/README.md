\# dbt Learning Project



dbt（data build tool）の学習プロジェクト。BigQueryを使用したデータ変換パイプライン。



\## プロジェクト構成

```

dbt\_learning/

├── models/

│   ├── staging/       # 元データの基本整形

│   ├── intermediate/  # 中間集計

│   └── marts/         # 最終的なデータマート

├── tests/

│   └── generic/       # カスタムテスト

├── .github/

│   └── workflows/     # CI/CD設定

└── dbt\_project.yml

```



\## 環境



\- \*\*dev\*\*: 開発環境（dbt\_dev）

\- \*\*stg\*\*: ステージング環境（dbt\_stg）

\- \*\*prd\*\*: 本番環境（dbt\_prd）



\## セットアップ

```bash

\# 依存パッケージのインストール

dbt deps



\# モデルの実行

dbt run --target dev



\# テストの実行

dbt test

```



\## データ品質テスト



\- 標準テスト: unique, not\_null

\- カスタムテスト: valid\_date\_range, positive\_values

\- dbt\_expectations: 範囲チェック、データ型検証



\## CI/CD



Pull Request時に自動テストを実行。stg環境でのテスト後、mainブランチマージで本番デプロイ。

