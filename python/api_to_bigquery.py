"""
API to BigQuery データパイプライン（実務ベース版）
外部APIからデータを取得し、BigQueryに自動格納するETLパイプライン

実務での実装内容を反映：
- バイナリサーチによる適応的期間分割アルゴリズム
- Response Entity Too Largeエラー対応（期間再分割）
- OAuth 2.0認証とトークン自動更新
- 10万件単位のバッチ処理とメモリクリア
- カラムマッピング（キャメルケース→スネークケース）
- 複数エラーパターンへの対応（リトライ処理）
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd
import requests
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ロガー設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class APIToBigQueryPipeline:
    """
    外部APIからBigQueryへのデータパイプライン（実務ベース版）
    
    Features:
        - バイナリサーチによる適応的期間分割（1,500件超で自動分割）
        - Response Entity Too Largeエラー対応（期間再分割、最大5回リトライ）
        - OAuth 2.0認証とトークン自動更新
        - 10万件単位のバッチ処理とメモリクリア
        - カラムマッピング（キャメルケース→スネークケース）
        - 複数エラーパターンへの対応（リトライ処理、exponential backoff）
        - ページネーション対応
        - 増分取得対応
        - データバリデーション
    """
    
    def __init__(
        self,
        api_config: Dict[str, Any],
        bq_config: Dict[str, Any]
    ):
        """
        パイプラインの初期化
        
        Args:
            api_config: API接続設定
            bq_config: BigQuery接続設定
        """
        self.api_base_url = api_config['base_url']
        self.api_key = api_config.get('api_key')
        self.headers = api_config.get('headers', {})
        self.oauth_config = api_config.get('oauth', {})  # OAuth設定
        
        self.project_id = bq_config['project_id']
        self.dataset_id = bq_config['dataset_id']
        self.table_id = bq_config['table_id']
        
        # BigQueryクライアント初期化
        self.bq_client = bigquery.Client(project=self.project_id)
        self.table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        
        # HTTPセッション設定（リトライ機能付き）
        self.session = self._create_session_with_retry()
        
        # OAuthトークン管理
        self.oauth_token = None
        self.token_expires_at = None
        
        # バッチ処理設定
        self.batch_size = 100000  # 10万件単位
        self.max_records_per_period = 1500  # 期間あたりの最大件数
        
        # カラムマッピング辞書（キャメルケース→スネークケース）
        self.column_mapping = api_config.get('column_mapping', {})
        
    def _create_session_with_retry(self) -> requests.Session:
        """リトライ機能付きHTTPセッション作成（実務ベース版）"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,  # exponential backoff
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "POST", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # API認証設定
        if self.api_key:
            self.headers['Authorization'] = f'Bearer {self.api_key}'
        session.headers.update(self.headers)
        
        return session
    
    def get_oauth_token(self) -> str:
        """
        OAuth 2.0トークンを取得（Client Credentials Flow）
        
        Returns:
            OAuthトークン
        """
        if self.oauth_token and self.token_expires_at and datetime.now() < self.token_expires_at:
            return self.oauth_token
        
        logger.info("OAuthトークンを取得中...")
        
        # OAuth設定から認証情報を取得
        client_id = self.oauth_config.get('client_id')
        client_secret = self.oauth_config.get('client_secret')
        token_url = self.oauth_config.get('token_url')
        scope = self.oauth_config.get('scope', '')
        
        # Basic認証ヘッダー作成
        from base64 import b64encode
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = b64encode(credentials.encode('utf-8')).decode('utf-8')
        
        headers = {
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        data = {
            'grant_type': 'client_credentials',
            'scope': scope
        }
        
        try:
            response = requests.post(token_url, headers=headers, data=data, timeout=30)
            response.raise_for_status()
            
            token_data = response.json()
            self.oauth_token = token_data['access_token']
            
            # トークンの有効期限を設定（デフォルト: 1時間）
            expires_in = token_data.get('expires_in', 3600)
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)  # 1分前のマージン
            
            logger.info("OAuthトークン取得完了")
            return self.oauth_token
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"OAuthトークン取得エラー: {e}")
            raise Exception(f"OAuth token acquisition failed: {e}")
    
    def get_estimated_count(
        self,
        endpoint: str,
        from_date: datetime,
        to_date: datetime,
        params: Optional[Dict] = None
    ) -> Optional[int]:
        """
        指定期間のレコード数を推定（実際のデータは取得しない）
        
        Args:
            endpoint: APIエンドポイント
            from_date: 開始日時
            to_date: 終了日時
            params: 追加パラメータ
            
        Returns:
            推定件数（エラー時はNone）
        """
        # OAuthトークンが必要な場合は取得
        if self.oauth_config:
            token = self.get_oauth_token()
            headers = {**self.headers, 'Authorization': f'Bearer {token}'}
        else:
            headers = self.headers
        
        # 件数確認用のクエリ（1件だけ取得してtotalを確認）
        search_params = {
            **(params or {}),
            'offset': 0,
            'limit': 1,
            'from': from_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'to': to_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        }
        
        url = f"{self.api_base_url}/{endpoint}"
        
        try:
            response = self.session.get(url, headers=headers, params=search_params, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            estimated_count = result.get('total', len(result.get('data', [])))
            
            logger.info(
                f"期間 {from_date.strftime('%Y-%m-%d')} - {to_date.strftime('%Y-%m-%d')}: "
                f"推定 {estimated_count} 件"
            )
            
            return estimated_count
            
        except requests.exceptions.HTTPError as e:
            # 401エラーは認証問題のため即座に処理停止
            if response.status_code == 401:
                logger.error(f"認証エラーが発生しました。トークンを確認してください: {e}")
                raise Exception(f"認証エラー: {e}")
            
            logger.warning(f"件数推定エラー: {e}")
            return None
        except Exception as e:
            logger.warning(f"件数推定エラー: {str(e)}")
            return None
    
    def find_optimal_period_end(
        self,
        endpoint: str,
        start_date: datetime,
        max_end_date: datetime,
        target_max_records: int = 1500,
        params: Optional[Dict] = None
    ) -> datetime:
        """
        バイナリサーチで最適な終了日を見つける（適応的期間分割アルゴリズム）
        
        Args:
            endpoint: APIエンドポイント
            start_date: 開始日時
            max_end_date: 最大終了日時
            target_max_records: 目標最大件数（デフォルト: 1,500件）
            params: 追加パラメータ
            
        Returns:
            最適な終了日時
        """
        logger.info(
            f"最適期間を検索中: {start_date.strftime('%Y-%m-%d')} "
            f"から最大 {max_end_date.strftime('%Y-%m-%d')} まで"
        )
        
        # まず1日単位で件数をチェック
        one_day_end = start_date + timedelta(days=1)
        if one_day_end > max_end_date:
            one_day_end = max_end_date
        
        one_day_count = self.get_estimated_count(endpoint, start_date, one_day_end, params)
        
        # 1日で1,500件を超える場合は時間単位で処理
        if one_day_count and one_day_count > target_max_records:
            logger.info(f"1日で{one_day_count}件検出。時間単位で分割処理します")
            return self.find_optimal_period_by_hour(
                endpoint, start_date, one_day_end, target_max_records, params
            )
        
        # 1日で1,500件以下の場合は日単位で処理
        logger.info("1日で1,500件以下。日単位で処理します")
        
        left_days = 1
        right_days = (max_end_date - start_date).days + 1
        optimal_end = start_date + timedelta(days=1)
        
        while left_days <= right_days:
            mid_days = (left_days + right_days) // 2
            test_end = start_date + timedelta(days=mid_days)
            
            if test_end > max_end_date:
                test_end = max_end_date
            
            # 推定件数を取得
            estimated_count = self.get_estimated_count(
                endpoint, start_date, test_end, params
            )
            
            if estimated_count is None:
                # エラーの場合は保守的に短い期間を選択
                right_days = mid_days - 1
                continue
            
            if estimated_count <= target_max_records:
                # まだ余裕があるので、より長い期間を試す
                optimal_end = test_end
                left_days = mid_days + 1
            else:
                # 多すぎるので、より短い期間を試す
                right_days = mid_days - 1
            
            # API呼び出し間隔を制限
            time.sleep(0.5)
        
        logger.info(
            f"最適期間決定: {start_date.strftime('%Y-%m-%d')} - "
            f"{optimal_end.strftime('%Y-%m-%d')}"
        )
        return optimal_end
    
    def find_optimal_period_by_hour(
        self,
        endpoint: str,
        start_date: datetime,
        max_end_date: datetime,
        target_max_records: int = 1500,
        params: Optional[Dict] = None
    ) -> datetime:
        """
        時間単位でバイナリサーチを行い、最適な終了日時を見つける
        
        Args:
            endpoint: APIエンドポイント
            start_date: 開始日時
            max_end_date: 最大終了日時
            target_max_records: 目標最大件数
            params: 追加パラメータ
            
        Returns:
            最適な終了日時
        """
        logger.info(
            f"時間単位での最適期間を検索中: {start_date.strftime('%Y-%m-%d %H:%M')} "
            f"から {max_end_date.strftime('%Y-%m-%d %H:%M')} まで"
        )
        
        total_hours = int((max_end_date - start_date).total_seconds() / 3600)
        left_hours = 1
        right_hours = total_hours
        optimal_end = start_date + timedelta(hours=1)
        
        while left_hours <= right_hours:
            mid_hours = (left_hours + right_hours) // 2
            test_end = start_date + timedelta(hours=mid_hours)
            
            if test_end > max_end_date:
                test_end = max_end_date
            
            # 推定件数を取得
            estimated_count = self.get_estimated_count(
                endpoint, start_date, test_end, params
            )
            
            if estimated_count is None:
                # エラーの場合は保守的に短い期間を選択
                right_hours = mid_hours - 1
                continue
            
            if estimated_count <= target_max_records:
                # まだ余裕があるので、より長い期間を試す
                optimal_end = test_end
                left_hours = mid_hours + 1
            else:
                # 多すぎるので、より短い期間を試す
                right_hours = mid_hours - 1
            
            # API呼び出し間隔を制限
            time.sleep(0.5)
        
        logger.info(
            f"時間単位での最適期間決定: {start_date.strftime('%Y-%m-%d %H:%M')} - "
            f"{optimal_end.strftime('%Y-%m-%d %H:%M')}"
        )
        return optimal_end
    
    def request_with_retry(
        self,
        url: str,
        headers: Dict[str, str],
        json_data: Optional[Dict] = None,
        method: str = 'POST',
        max_retries: int = 3,
        initial_wait: int = 2
    ) -> requests.Response:
        """
        APIリクエストをretry処理付きで実行（実務ベース版）
        
        Args:
            url: リクエストURL
            headers: リクエストヘッダー
            json_data: POSTリクエストのJSONデータ
            method: HTTPメソッド（デフォルト: POST）
            max_retries: 最大retry回数（デフォルト: 3）
            initial_wait: 初期待機時間（秒、デフォルト: 2）
        
        Returns:
            requests.Response: レスポンスオブジェクト
        
        Raises:
            Exception: retry後も失敗した場合、またはretry不可なエラーが発生した場合
        """
        retry_count = 0
        wait_time = initial_wait
        
        while retry_count <= max_retries:
            try:
                # 通常のリクエスト処理
                if method == 'POST':
                    response = requests.post(url, headers=headers, json=json_data, timeout=60)
                else:
                    response = requests.get(url, headers=headers, timeout=60)
                
                # 成功した場合はそのまま返す
                if response.status_code < 400:
                    return response
                
                # 4xxエラー（クライアントエラー）はretry不可
                if 400 <= response.status_code < 500:
                    if response.status_code == 401:
                        # 401エラーは認証問題のため即座に処理停止
                        logger.error(f"認証エラー (401): {response.text}")
                        raise Exception(f"認証エラーが発生しました。処理を停止します: {response.status_code}")
                    elif response.status_code == 403:
                        logger.error(f"認可エラー (403): {response.text}")
                        raise Exception(f"アクセス権限エラーが発生しました: {response.status_code}")
                    elif response.status_code == 400:
                        logger.error(f"Bad Request (400): {response.text}")
                        raise Exception(f"リクエストパラメータエラー: {response.status_code}")
                    else:
                        # その他の4xxエラーもretry不可
                        logger.error(f"クライアントエラー ({response.status_code}): {response.text}")
                        raise Exception(f"API request failed: {response.status_code}")
                
                # 5xxエラー（サーバーエラー）はretry可能
                if 500 <= response.status_code < 600:
                    # Response Entity Too Largeエラーの特別処理
                    if response.status_code == 500 and "Response Entity Too Large" in response.text:
                        logger.warning("Response Entity Too Largeエラーが発生しました")
                        raise ResponseEntityTooLargeError("Response Entity Too Large")
                    
                    if retry_count < max_retries:
                        logger.warning(
                            f"サーバーエラー ({response.status_code}): {response.text}\n"
                            f"Retry {retry_count + 1}/{max_retries} 回目: {wait_time}秒待機してから再試行します..."
                        )
                        time.sleep(wait_time)
                        wait_time *= 2  # exponential backoff
                        retry_count += 1
                        continue
                    else:
                        logger.error(
                            f"サーバーエラー ({response.status_code}): "
                            f"最大retry回数 ({max_retries}) に達しました"
                        )
                        raise Exception(f"API request failed after {max_retries} retries: {response.status_code}")
                
                # その他のステータスコード
                response.raise_for_status()
                
            except requests.exceptions.Timeout as e:
                if retry_count < max_retries:
                    logger.warning(f"タイムアウトエラー: {wait_time}秒待機してから再試行します...")
                    time.sleep(wait_time)
                    wait_time *= 2
                    retry_count += 1
                    continue
                else:
                    logger.error(f"タイムアウトエラー: 最大retry回数 ({max_retries}) に達しました")
                    raise Exception(f"API request timeout after {max_retries} retries: {str(e)}")
            
            except requests.exceptions.ConnectionError as e:
                if retry_count < max_retries:
                    logger.warning(f"接続エラー: {wait_time}秒待機してから再試行します...")
                    time.sleep(wait_time)
                    wait_time *= 2
                    retry_count += 1
                    continue
                else:
                    logger.error(f"接続エラー: 最大retry回数 ({max_retries}) に達しました")
                    raise Exception(f"API connection error after {max_retries} retries: {str(e)}")
            
            except ResponseEntityTooLargeError:
                # Response Entity Too Largeエラーは呼び出し元で処理
                raise
        
        # ここには到達しないはずだが、念のため
        raise Exception(f"API request failed after {max_retries} retries")
    
    def handle_response_too_large_error(
        self,
        endpoint: str,
        from_date: datetime,
        to_date: datetime,
        params: Optional[Dict] = None,
        retry_count: int = 0
    ) -> List[Dict]:
        """
        Response Entity Too Largeエラーが発生した際に期間を細分化して再帰的に処理
        
        Args:
            endpoint: APIエンドポイント
            from_date: 開始日時
            to_date: 終了日時
            params: 追加パラメータ
            retry_count: 現在のリトライ回数
            
        Returns:
            取得したデータのリスト
        """
        # 最大リトライ回数を制限（無限ループを防ぐ）
        max_retries = 5
        if retry_count >= max_retries:
            logger.warning(
                f"最大リトライ回数 ({max_retries}) に到達しました。処理をスキップします。"
            )
            return []
        
        logger.info(
            f"Response Entity Too Largeエラー処理開始 "
            f"(リトライ {retry_count + 1}/{max_retries})"
        )
        logger.info(
            f"期間: {from_date.strftime('%Y-%m-%d %H:%M')} - "
            f"{to_date.strftime('%Y-%m-%d %H:%M')}"
        )
        
        # 期間を半分に分割
        time_diff = to_date - from_date
        half_time = time_diff / 2
        mid_date = from_date + half_time
        
        logger.info(
            f"期間を分割: {from_date.strftime('%Y-%m-%d %H:%M')} - "
            f"{mid_date.strftime('%Y-%m-%d %H:%M')} と "
            f"{mid_date.strftime('%Y-%m-%d %H:%M')} - "
            f"{to_date.strftime('%Y-%m-%d %H:%M')}"
        )
        
        all_data = []
        
        try:
            # 前半期間を処理
            logger.info(
                f"前半期間処理開始: {from_date.strftime('%Y-%m-%d %H:%M')} - "
                f"{mid_date.strftime('%Y-%m-%d %H:%M')}"
            )
            first_half_data = self.fetch_data_from_api(
                endpoint, from_date, mid_date, params
            )
            all_data.extend(first_half_data)
            logger.info(f"前半期間処理完了: {len(first_half_data)} 件")
            
            # API制限を考慮してウェイト
            time.sleep(2)
            
            # 後半期間を処理
            logger.info(
                f"後半期間処理開始: {mid_date.strftime('%Y-%m-%d %H:%M')} - "
                f"{to_date.strftime('%Y-%m-%d %H:%M')}"
            )
            second_half_data = self.fetch_data_from_api(
                endpoint, mid_date, to_date, params
            )
            all_data.extend(second_half_data)
            logger.info(f"後半期間処理完了: {len(second_half_data)} 件")
            
            logger.info(
                f"Response Entity Too Largeエラー処理完了: 合計 {len(all_data)} 件"
            )
            
        except Exception as e:
            logger.error(f"期間分割処理中にエラーが発生: {str(e)}")
            # エラーが発生した場合は、さらに細かく分割を試行
            if retry_count < max_retries - 1:
                logger.info("さらに細かく期間を分割して再試行します")
                return self.handle_response_too_large_error(
                    endpoint, from_date, to_date, params, retry_count + 1
                )
            else:
                logger.warning("最大リトライ回数に到達したため、この期間をスキップします")
                return []
        
        return all_data
    
    def fetch_data_from_api(
        self,
        endpoint: str,
        from_date: datetime,
        to_date: datetime,
        params: Optional[Dict] = None,
        use_adaptive_period: bool = True
    ) -> List[Dict]:
        """
        APIからデータ取得（適応的期間分割対応、ページネーション対応）
        
        Args:
            endpoint: APIエンドポイント
            from_date: 開始日時
            to_date: 終了日時
            params: 追加パラメータ
            use_adaptive_period: 適応的期間分割を使用するか
            
        Returns:
            取得したデータのリスト
        """
        all_data = []
        
        # 適応的期間分割を使用する場合
        if use_adaptive_period:
            # 最適な期間を探索
            optimal_end = self.find_optimal_period_end(
                endpoint, from_date, to_date, self.max_records_per_period, params
            )
            
            # 期間が分割される場合は再帰的に処理
            if optimal_end < to_date:
                # 前半期間を処理
                first_half = self.fetch_data_from_api(
                    endpoint, from_date, optimal_end, params, use_adaptive_period
                )
                all_data.extend(first_half)
                
                # 後半期間を処理
                second_half = self.fetch_data_from_api(
                    endpoint, optimal_end, to_date, params, use_adaptive_period
                )
                all_data.extend(second_half)
                
                return all_data
            
            # 最適期間が決定されたので、その期間でデータ取得
            to_date = optimal_end
        
        # OAuthトークンが必要な場合は取得
        if self.oauth_config:
            token = self.get_oauth_token()
            headers = {**self.headers, 'Authorization': f'Bearer {token}'}
        else:
            headers = self.headers
        
        url = f"{self.api_base_url}/{endpoint}"
        page_size = 20  # API制限に合わせて設定
        offset = 0
        page = 0
        max_pages = 100  # 無限ループ防止
        
        logger.info(f"APIからデータ取得開始: {url}")
        logger.info(
            f"期間: {from_date.strftime('%Y-%m-%d %H:%M')} - "
            f"{to_date.strftime('%Y-%m-%d %H:%M')}"
        )
        
        while page < max_pages:
            page += 1
            
            # リクエストボディ作成
            search_body = {
                "offset": offset,
                "limit": page_size,
                "sorts": [
                    {
                        "field": "lastModified",
                        "sortOrder": "asc"
                    }
                ],
                "query": {
                    "filtered_query": {
                        "query": {
                            "match_all_query": {}
                        },
                        "filter": {
                            "range_filter": {
                                "field": "last_modified",
                                "from": from_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                                "to": to_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                            }
                        }
                    }
                }
            }
            
            # 追加パラメータをマージ
            if params:
                search_body.update(params)
            
            try:
                # API呼び出し（リトライ処理付き）
                response = self.request_with_retry(
                    url, headers, json_data=search_body, method='POST'
                )
                
                result = response.json()
                
                # データがなくなったら終了
                if 'hits' not in result or not result['hits']:
                    logger.info(f"ページ {page}: データなし。処理完了")
                    break
                
                # 取得したデータを追加
                for hit in result['hits']:
                    if 'data' in hit:
                        all_data.append(hit.get('data', {}))
                    else:
                        all_data.append(hit)
                
                logger.info(
                    f"ページ {page}: {len(result['hits'])} 件取得 "
                    f"（合計: {len(all_data)} 件）"
                )
                
                # 取得した件数が1ページ分未満なら終了
                if len(result['hits']) < page_size:
                    logger.info("最終ページに到達")
                    break
                
                # 次のページのオフセットを設定
                offset += len(result['hits'])
                
                # API呼び出し間隔を制限
                time.sleep(0.5)
                
            except ResponseEntityTooLargeError:
                # Response Entity Too Largeエラー時は期間を細分化
                logger.warning("Response Entity Too Largeエラー。期間を細分化して再試行します")
                return self.handle_response_too_large_error(
                    endpoint, from_date, to_date, params
                )
            
            except Exception as e:
                logger.error(f"API呼び出しエラー（ページ{page}）: {e}")
                raise
        
        return all_data
    
    def process_column_mapping(self, data: List[Dict]) -> List[Dict]:
        """
        カラムマッピング（キャメルケース→スネークケース）
        
        Args:
            data: 生データ
            
        Returns:
            マッピング済みデータ
        """
        if not self.column_mapping:
            return data
        
        processed_data = []
        for record in data:
            mapped_record = {}
            for key, value in record.items():
                # マッピング辞書に存在する場合は変換
                if key in self.column_mapping:
                    mapped_key = self.column_mapping[key]
                else:
                    # マッピング辞書にない場合は自動変換（キャメルケース→スネークケース）
                    import re
                    mapped_key = re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()
                
                mapped_record[mapped_key] = value
            
            processed_data.append(mapped_record)
        
        return processed_data
    
    def upload_batch_to_bigquery(
        self,
        batch_data: List[Dict],
        batch_number: int
    ) -> int:
        """
        バッチデータをBigQueryにアップロード（10万件単位）
        
        Args:
            batch_data: バッチデータ
            batch_number: バッチ番号
            
        Returns:
            アップロード件数
        """
        try:
            logger.info(f"バッチ {batch_number}: {len(batch_data)} 件のデータを変換中...")
            
            # カラムマッピング
            processed_data = self.process_column_mapping(batch_data)
            
            # DataFrame作成
            df = pd.DataFrame(processed_data)
            
            # データ型変換
            for col in df.columns:
                # 日付型カラムの処理
                if 'date' in col.lower() or 'time' in col.lower():
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # 数値型カラムの処理
                elif df[col].dtype == 'object':
                    try:
                        df[col] = pd.to_numeric(df[col], errors='ignore')
                    except:
                        pass
            
            # NULL値の処理
            df = df.replace({pd.NaT: None, pd.NA: None, '': None})
            
            logger.info(f"バッチ {batch_number}: BigQueryアップロード中...")
            
            # BigQueryテーブル存在チェック
            try:
                table = self.bq_client.get_table(self.table_ref)
                logger.info(f"既存テーブル確認: {self.table_ref}")
            except:
                logger.info(f"新規テーブル作成: {self.table_ref}")
            
            # ジョブ設定
            job_config = bigquery.LoadJobConfig(
                write_disposition='WRITE_APPEND',
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ],
                autodetect=True,
                create_disposition='CREATE_IF_NEEDED'
            )
            
            # データロード実行
            job = self.bq_client.load_table_from_dataframe(
                df, self.table_ref, job_config=job_config
            )
            
            # ジョブ完了待機
            job.result()
            
            logger.info(f"バッチ {batch_number}: BigQueryアップロード完了 ({len(df)} 件)")
            
            # メモリクリア
            del processed_data
            del df
            
            return len(batch_data)
            
        except GoogleCloudError as e:
            logger.error(f"バッチ {batch_number} BigQueryアップロードエラー: {e}")
            raise
    
    def validate_data(self, data: List[Dict]) -> pd.DataFrame:
        """
        データバリデーションと前処理
        
        Args:
            data: 生データ
            
        Returns:
            検証済みDataFrame
        """
        if not data:
            raise ValueError("データが空です")
        
        # カラムマッピング
        processed_data = self.process_column_mapping(data)
        
        df = pd.DataFrame(processed_data)
        logger.info(f"データ件数: {len(df)}件, カラム: {list(df.columns)}")
        
        # データ型変換とクレンジング
        for col in df.columns:
            # 日付型カラムの処理
            if 'date' in col.lower() or 'time' in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # 数値型カラムの処理
            elif df[col].dtype == 'object':
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except:
                    pass
        
        # NULL値の処理
        df = df.replace({pd.NaT: None, pd.NA: None, '': None})
        
        # 重複削除（必要に応じて）
        initial_count = len(df)
        df = df.drop_duplicates()
        if len(df) < initial_count:
            logger.warning(f"重複データ{initial_count - len(df)}件を削除")
        
        # 必須カラムのチェック
        required_columns = ['id', 'created_at']  # 必須カラムを定義
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            logger.warning(f"必須カラムが不足: {missing_columns}")
        
        # メタデータ追加
        df['_loaded_at'] = datetime.now()
        df['_pipeline_version'] = '1.0.0'
        
        return df
    
    def load_to_bigquery(
        self,
        df: pd.DataFrame,
        write_disposition: str = 'WRITE_APPEND'
    ) -> None:
        """
        BigQueryへのデータロード
        
        Args:
            df: ロードするDataFrame
            write_disposition: 書き込みモード
        """
        try:
            # BigQueryテーブル存在チェック
            try:
                table = self.bq_client.get_table(self.table_ref)
                logger.info(f"既存テーブル確認: {self.table_ref}")
            except:
                logger.info(f"新規テーブル作成: {self.table_ref}")
            
            # ジョブ設定
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ],
                autodetect=True,
                create_disposition='CREATE_IF_NEEDED'
            )
            
            # データロード実行
            job = self.bq_client.load_table_from_dataframe(
                df,
                self.table_ref,
                job_config=job_config
            )
            
            # ジョブ完了待機
            job.result()
            
            logger.info(
                f"BigQueryロード完了: {len(df)}件 → {self.table_ref}"
            )
            
        except GoogleCloudError as e:
            logger.error(f"BigQueryロードエラー: {e}")
            raise
    
    def get_last_sync_timestamp(self) -> Optional[datetime]:
        """最終同期時刻を取得（増分取得用）"""
        query = f"""
        SELECT MAX(_loaded_at) as last_sync
        FROM `{self.table_ref}`
        """
        
        try:
            result = self.bq_client.query(query).result()
            for row in result:
                if row.last_sync:
                    return row.last_sync
        except:
            logger.info("最終同期時刻の取得に失敗（初回実行の可能性）")
        
        # デフォルトは30日前
        return datetime.now() - timedelta(days=30)
    
    def run_pipeline(
        self,
        endpoint: str,
        incremental: bool = True,
        full_refresh: bool = False,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        パイプライン実行（実務ベース版）
        
        Args:
            endpoint: APIエンドポイント
            incremental: 増分取得モード
            full_refresh: フルリフレッシュモード
            from_date: 開始日時（指定時はこの日時から取得）
            to_date: 終了日時（指定時はこの日時まで取得）
            
        Returns:
            実行結果サマリ
        """
        start_time = datetime.now()
        logger.info(f"パイプライン開始: {endpoint}")
        
        try:
            # 期間設定
            if from_date and to_date:
                # 指定された期間を使用
                fetch_from = from_date
                fetch_to = to_date
            elif incremental and not full_refresh:
                # 増分取得の場合、最終同期時刻以降のデータを取得
                last_sync = self.get_last_sync_timestamp()
                fetch_from = last_sync
                fetch_to = datetime.now()
                logger.info(f"増分取得モード: {last_sync}以降のデータ")
            else:
                # フルリフレッシュの場合、デフォルト期間を使用
                fetch_from = datetime.now() - timedelta(days=30)
                fetch_to = datetime.now()
                logger.info(f"フルリフレッシュモード: {fetch_from} - {fetch_to}")
            
            # 1. APIからデータ取得（適応的期間分割対応）
            raw_data = self.fetch_data_from_api(
                endpoint, fetch_from, fetch_to, use_adaptive_period=True
            )
            
            if not raw_data:
                logger.info("新規データなし")
                return {
                    'status': 'success',
                    'records_processed': 0,
                    'duration': str(datetime.now() - start_time)
                }
            
            # 2. バッチ処理（10万件単位）
            total_processed = 0
            batch_count = 0
            batch_data = []
            
            for record in raw_data:
                batch_data.append(record)
                
                # バッチサイズに達したらBigQueryにアップロード
                if len(batch_data) >= self.batch_size:
                    batch_count += 1
                    batch_processed = self.upload_batch_to_bigquery(
                        batch_data, batch_count
                    )
                    total_processed += batch_processed
                    
                    # メモリクリア
                    batch_data.clear()
                    logger.info(
                        f"バッチ {batch_count} 完了: {batch_processed} 件処理。"
                        f"総処理数: {total_processed} 件"
                    )
            
            # 残りのバッチを処理
            if batch_data:
                batch_count += 1
                batch_processed = self.upload_batch_to_bigquery(
                    batch_data, batch_count
                )
                total_processed += batch_processed
                logger.info(f"最終バッチ {batch_count} 完了: {batch_processed} 件処理")
            
            # 実行結果
            result = {
                'status': 'success',
                'records_processed': total_processed,
                'duration': str(datetime.now() - start_time),
                'batches': batch_count,
                'sample_data': raw_data[:3] if raw_data else []
            }
            
            logger.info(f"パイプライン完了: {result['records_processed']}件処理")
            return result
            
        except Exception as e:
            logger.error(f"パイプラインエラー: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'duration': str(datetime.now() - start_time)
            }


class ResponseEntityTooLargeError(Exception):
    """Response Entity Too Largeエラー用のカスタム例外"""
    pass


def main():
    """メイン実行関数"""
    
    # 設定
    api_config = {
        'base_url': 'https://api.example.com/v1',
        'api_key': 'your-api-key',  # APIキー認証の場合
        'headers': {
            'Content-Type': 'application/json'
        },
        # OAuth 2.0認証の場合
        'oauth': {
            'client_id': 'your-client-id',
            'client_secret': 'your-client-secret',
            'token_url': 'https://api.example.com/oauth/token',
            'scope': 'read:products'
        },
        # カラムマッピング辞書（キャメルケース→スネークケース）
        'column_mapping': {
            'lastModified': 'last_modified',
            'creationDate': 'creation_date',
            'primaryCategoryId': 'primary_category_id',
            # ... 150項目以上のマッピング
        }
    }
    
    bq_config = {
        'project_id': 'your-project-id',
        'dataset_id': 'your_dataset',
        'table_id': 'api_data'
    }
    
    # パイプライン実行
    pipeline = APIToBigQueryPipeline(api_config, bq_config)
    
    # 増分取得実行
    result = pipeline.run_pipeline(
        endpoint='products',
        incremental=True
    )
    
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
