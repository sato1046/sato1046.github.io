# 👋 データエンジニア ポートフォリオ

## 🚀 About Me

データ基盤構築・ETL開発・BI分析環境の専門家として、大手企業のデータ活用を支援しています。  
複雑なデータパイプラインの設計から実装まで、エンドツーエンドのソリューションを提供します。

### 💼 専門領域
- **データ基盤構築**: BigQuery, Treasure Data, GCPを活用した大規模データ基盤の設計・構築
- **ETL/ELT開発**: 複雑なデータ変換処理とパイプライン自動化
- **BI環境構築**: Tableau連携、多次元データマート設計
- **マーケティング分析**: GA4/GTM実装、タグ管理基盤移行

## 🛠️ Technical Skills

### データ基盤 & クラウド
![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![Treasure Data](https://img.shields.io/badge/Treasure_Data-FF6B6B?style=for-the-badge&logoColor=white)
![GCP](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)

### プログラミング言語
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)
![JavaScript](https://img.shields.io/badge/JavaScript-F7DF1E?style=for-the-badge&logo=javascript&logoColor=black)

### ワークフロー & オーケストレーション
![Digdag](https://img.shields.io/badge/Digdag-2088FF?style=for-the-badge&logoColor=white)
![Cloud Functions](https://img.shields.io/badge/Cloud_Functions-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)

### 分析 & 可視化
![Tableau](https://img.shields.io/badge/Tableau-E97627?style=for-the-badge&logo=tableau&logoColor=white)
![GA4](https://img.shields.io/badge/Google_Analytics_4-E37400?style=for-the-badge&logo=google-analytics&logoColor=white)
![GTM](https://img.shields.io/badge/Google_Tag_Manager-246FDB?style=for-the-badge&logo=google-tag-manager&logoColor=white)

## 📊 実績データ

<table>
<tr>
<td align="center">
<h3>6+</h3>
<p>大規模プロジェクト</p>
</td>
<td align="center">
<h3>96%</h3>
<p>処理時間削減</p>
</td>
<td align="center">
<h3>99.9%</h3>
<p>システム稼働率</p>
</td>
<td align="center">
<h3>500万+</h3>
<p>日次処理レコード</p>
</td>
</tr>
</table>

## 🏆 主要プロジェクト

### 1. 🎮 大手玩具メーカー データ基盤構築
**期間**: 2024年4月〜8月 | **役割**: データエンジニア

- Salesforce Marketing CloudからTreasure Dataへの自動データ連携パイプライン構築
- **成果**: 月40時間の作業削減、データ品質問題を3時間以内に解決
- **技術**: Treasure Data, Digdag, SFTP, Presto SQL

### 2. 👟 スポーツアパレル企業 BIデータマート構築
**期間**: 2024年6月〜7月 | **役割**: リードデータエンジニア

- 多次元集計データマート（年/月/週/日×チャネル×会員属性×商品カテゴリ）の設計・構築
- **成果**: 分析リードタイムを2日→30分に短縮（96%削減）
- **技術**: Treasure Data, Presto SQL, Tableau, CTEs, Window関数

### 3. 🏠 不動産投資会社 BigQuery分析基盤
**期間**: 2024年2月 | **役割**: データエンジニア

- Excel管理の物件情報100件以上をBigQuery基盤に移行
- **成果**: 月40時間の評価作業を即時実行可能に、税務計算精度100%達成
- **技術**: BigQuery, StandardSQL, Google Sheets API

### 4. 👗 アパレル4ブランド タグ管理基盤移行
**期間**: 2024年3月〜11月 | **役割**: シニアデータエンジニア

- 4ブランド×50種類以上のマーケティングタグをTealiumからGTMへ移行
- **成果**: タグ発火率95%→99.8%、エラー率10%→0.2%
- **技術**: GTM, GA4, JavaScript, dataLayer

### 5. 📚 学習管理システム ETLパイプライン
**期間**: 2024年 | **役割**: データエンジニア

- 学習管理システムとBigQueryを連携するETLパイプラインをGCP上に構築
- **成果**: 日次バッチ稼働率99.9%、完全自動化を実現
- **技術**: BigQuery, Cloud Functions, Python, AES-256暗号化

### 6. ✈️ 大手不動産企業 空港事業部 GPS位置情報分析基盤構築
**期間**: 2024年 | **役割**: データエンジニア（実装・顧客対応担当）  
**チーム規模**: 2名

#### プロジェクト概要
国内旅行レンタルデバイスから収集されるGPS位置情報の異常値を自動検出・補正する大規模データ処理パイプラインを構築。
時速1000km超の非現実的な移動データやV字型の異常パターンを自動修正し、正確な旅行経路分析を実現。

#### 主な業務内容
**1. GPS位置情報異常値検出・補正システムの開発**
- **課題**: 大量の異常値（時速1000km超の移動、V字パターン、座標の急激な変動）により正確な分析が困難
- **解決策**: 
  - JavaScript UDFによるカスタムロジック実装
  - Haversine公式による球面距離計算と時速1000km超の移動を自動検出・補正
  - V字パターン検出アルゴリズム（開始点・終了点が近接し、途中で大きく移動したパターンを判定）
  - 適応的傾向分析による座標補正（データ点数に応じた最適アルゴリズム選択）
  - 時系列データ処理による日付間の連続性担保

**2. 大規模SQLクエリの設計・最適化**
- 450行超のSQLクエリを10段階以上のCTE処理に分解
- ROW_NUMBERの事前計算により集計関数エラーを回避
- ARRAY_AGG/UNNESTを用いた配列データの集約・展開処理を実装
- パーティション処理による効率的なデータ分割

**3. 段階的な実装と顧客対応**
- 3日間の検証・調整フェーズでクライアント要望に迅速対応
- 日付単位とオーダーID単位の処理スコープの違いを明確化
- 詳細なコメントによる技術文書化とトレーサビリティ確保

#### 主な成果
- GPS位置情報の異常値を自動検出・補正し、正確な旅行経路データの生成を実現
- 450行超の大規模クエリによる複雑なデータ変換パイプラインを構築
- クライアント要望への即日対応により、継続的なロジック改善を実現
- 詳細な技術文書により、保守性・拡張性を確保

#### 技術スタック
- **データ基盤**: Google BigQuery
- **クエリ言語**: Standard SQL、JavaScript UDF
- **データ処理**: CTE、Window関数（LAG/LEAD/FIRST_VALUE/LAST_VALUE）、配列処理、パーティション処理
- **地理空間計算**: Haversine距離計算アルゴリズム

#### 工夫した点
- 顧客とのコミュニケーションをSlackで密に行い、曖昧な要件を技術仕様に精緻化
- 複数の異常値検出ロジックを独立したUDFとして実装し、テストと保守を容易化
- データ点数に応じた適応的なアルゴリズム設計により、様々なデータパターンに対応
- 速度補正済みデータを後続処理に正しく引き継ぐ設計により、補正の一貫性を担保

## 💡 技術的な強み

### 1. 大規模データ処理
- 500万レコード/日の高速処理実装
- 複雑なSQL（300行超、12パターンのUNION ALL）の最適化
- メモリ効率を考慮したパフォーマンスチューニング

### 2. データ品質管理
- データ型不整合問題の迅速な解決（L1/L2層での段階的変換）
- 文字化け・フォーマット不統一への対応
- 包括的なテスト戦略（単体・結合・回帰テスト）

### 3. チーム支援・教育
- ジュニアエンジニアへの技術指導
- ドキュメント整備による属人化防止
- コードレビューによる品質向上

## 📈 ビジネスインパクト

- **業務効率化**: 手動作業を月40〜80時間削減
- **意思決定の高速化**: 分析時間を数日から数分に短縮
- **品質向上**: エラー率を10%から0.2%以下に改善
- **コスト削減**: 自動化による人的リソースの最適化

## 🎯 得意とする課題解決

✅ レガシーシステムのモダナイゼーション  
✅ リアルタイムデータパイプライン構築  
✅ 複雑なビジネスロジックのSQL実装  
✅ マルチソースデータの統合と正規化  
✅ スケーラブルなデータアーキテクチャ設計  

## 📫 Contact

- 🌐 [ポートフォリオサイト](https://sato1046.github.io)
- 💼 副業・業務委託のご相談はお気軽にご連絡ください

## 🔄 継続的な学習

常に最新技術をキャッチアップし、以下の分野でスキルを拡張しています：
- クラウドネイティブアーキテクチャ
- リアルタイムストリーミング処理
- MLOps/データサイエンス基盤
- データガバナンス・セキュリティ

---

### 🌟 お仕事のご依頼について

データ基盤構築、ETL開発、BI環境整備などのプロジェクトでお困りの際は、ぜひご相談ください。  
短期集中型のプロジェクトから長期的な支援まで、柔軟に対応いたします。

**対応可能な業務:**
- データ基盤の設計・構築
- ETL/ELTパイプライン開発
- データマート・DWH構築
- データ品質改善
- 技術支援・コンサルティング

---

*"データを価値に変える" - 複雑なデータ課題を、実用的なソリューションで解決します。*
