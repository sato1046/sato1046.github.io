# Python Data Pipeline Examples 🐍

実務で使用したPythonによるデータパイプライン実装例

## 📚 内容

### 1. ETLパイプライン（実務ベース版）
- **API → BigQueryへのデータ取込**
  - バイナリサーチによる適応的期間分割アルゴリズム（1,500件超で自動分割）
  - Response Entity Too Largeエラー対応（期間再分割、最大5回リトライ）
  - OAuth 2.0認証とトークン自動更新
  - 10万件単位のバッチ処理とメモリクリア
  - カラムマッピング（キャメルケース→スネークケース、150項目以上）
- CSVファイルの自動処理
- データクレンジング・変換

### 2. エラーハンドリング（実務ベース版）
- **複数エラーパターンへの対応**
  - タイムアウトエラー（リトライ処理）
  - 接続エラー（リトライ処理）
  - 5xxエラー（リトライ処理、exponential backoff）
  - 401認証エラー（OAuthトークン自動更新）
  - Response Entity Too Largeエラー（期間再分割）
- 自動バリデーション
- 異常値検出
- データプロファイリング

### 3. 自動化スクリプト
- 定期実行バッチ
- エラー通知
- ログ管理

## 🛠 技術スタック

- **Python 3.9+**
- **pandas** - データ処理
- **Google Cloud SDK** - BigQuery連携
- **requests** - API連携
- **schedule** - スケジューリング

## 📂 ファイル構成

```
python-data-pipeline/
├── etl/
│   ├── api_to_bigquery.py
│   ├── csv_processor.py
│   └── data_transformer.py
├── quality_check/
│   ├── data_validator.py
│   ├── anomaly_detector.py
│   └── data_profiler.py
├── automation/
│   ├── scheduler.py
│   ├── error_handler.py
│   └── logger_config.py
├── config/
│   ├── settings.yaml
│   └── schema.json
└── tests/
    └── test_pipeline.py
```

## 💡 特徴

- ✅ エラーハンドリング完備
- ✅ 再実行可能な設計
- ✅ ログ出力とモニタリング
- ✅ 設定ファイルによる柔軟な構成

## 📈 実績

- **1日あたり最大2万件の更新対応**（SFCC Products API連携）
- **10万件単位のバッチ処理**によるメモリ効率化
- **バイナリサーチによる適応的期間分割**でAPI制限を回避
- **Response Entity Too Largeエラー対応**により大規模データ取得を実現
- **150項目以上のカラムマッピング**（キャメルケース→スネークケース）
- **5段階CTE処理による重複排除**でデータ品質を確保
- 日次100万レコードの安定処理
- 99.9%の稼働率を実現
- 処理時間を70%削減

## 📂 サンプルコード

### Pythonデータパイプライン

- [API to BigQueryパイプライン](API%20to%20BigQuery%20Pipeline%20-%20api_to_bigquery.py) - ETL処理実装例

## 👤 Author

- GitHub: [@sato1046](https://github.com/sato1046)
- Portfolio: [https://sato1046.github.io](https://sato1046.github.io)