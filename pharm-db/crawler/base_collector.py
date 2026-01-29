"""
Base collector class for all data collectors.
Provides common functionality for API requests, pagination, and data storage.
"""

import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Generator, Optional
from urllib.parse import urlencode

import requests

from .config import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class BaseCollector(ABC):
    """
    Abstract base class for all data collectors.

    Provides common functionality for:
    - API request handling with retry logic
    - Pagination support
    - Data transformation
    - Elasticsearch indexing
    """

    def __init__(self, name: str = None, index_name: str = None):
        """
        BaseCollector 초기화

        Args:
            name: 수집기 이름 (로깅용). None이면 클래스명 사용
            index_name: Elasticsearch 인덱스명. None이면 es_index 프로퍼티 사용
        """
        self.name = name or self.__class__.__name__
        self._index_name = index_name
        self.logger = logging.getLogger(self.name)
        self.session = requests.Session()
        self.service_key = config.DATA_GO_KR_API_KEY

    @property
    def base_url(self) -> str:
        """Base URL for the API. Override in subclass."""
        return ""

    @property
    def endpoint(self) -> str:
        """API endpoint path. Override in subclass."""
        return ""

    @property
    def es_index(self) -> str:
        """Elasticsearch index name for storing collected data. Override in subclass."""
        return self._index_name or ""

    def build_url(self, params: dict) -> str:
        """Build the full API URL with query parameters."""
        params["serviceKey"] = self.service_key
        query_string = urlencode(params, safe="%")
        return f"{self.base_url}{self.endpoint}?{query_string}"

    def make_request(
        self,
        params: dict,
        timeout: int = 30,
    ) -> Optional[dict]:
        """
        Make an API request with retry logic.

        Args:
            params: Query parameters for the request.
            timeout: Request timeout in seconds.

        Returns:
            Response data as dictionary or None if failed.
        """
        url = self.build_url(params)

        for attempt in range(config.MAX_RETRIES):
            try:
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()

                content_type = response.headers.get("Content-Type", "")
                if "json" in content_type:
                    return response.json()
                else:
                    return self._parse_xml_response(response.text)

            except requests.RequestException as e:
                self.logger.warning(
                    f"Request failed (attempt {attempt + 1}/{config.MAX_RETRIES}): {e}"
                )
                if attempt < config.MAX_RETRIES - 1:
                    time.sleep(config.RETRY_DELAY * (attempt + 1))

        self.logger.error(f"Failed to fetch data after {config.MAX_RETRIES} attempts")
        return None

    def _parse_xml_response(self, xml_text: str) -> Optional[dict]:
        """
        Parse XML response to dictionary.

        Args:
            xml_text: Raw XML response text.

        Returns:
            Parsed data as dictionary.
        """
        try:
            import xmltodict

            return xmltodict.parse(xml_text)
        except ImportError:
            self.logger.error("xmltodict package is required for XML parsing")
            return None
        except Exception as e:
            self.logger.error(f"Failed to parse XML response: {e}")
            return None

    def paginate(
        self,
        base_params: dict,
        page_size: int = None,
    ) -> Generator[list, None, None]:
        """
        Iterate through paginated API responses.

        Args:
            base_params: Base query parameters.
            page_size: Number of results per page.

        Yields:
            List of items from each page.
        """
        page_size = page_size or config.DEFAULT_PAGE_SIZE
        page_no = 1

        while True:
            params = {
                **base_params,
                "pageNo": page_no,
                "numOfRows": page_size,
            }

            response = self.make_request(params)
            if not response:
                break

            items = self.extract_items(response)
            if not items:
                break

            yield items

            total_count = self.extract_total_count(response)
            if total_count and page_no * page_size >= total_count:
                break

            page_no += 1

    def extract_items(self, response: dict) -> list:
        """
        Extract items from API response.
        Override in subclass if response structure differs.

        Args:
            response: Raw API response dictionary.

        Returns:
            List of item dictionaries.
        """
        try:
            body = response.get("response", {}).get("body", {})
            items = body.get("items", {})

            if items is None:
                return []

            item_list = items.get("item", [])

            if isinstance(item_list, dict):
                return [item_list]

            return item_list if item_list else []
        except Exception as e:
            self.logger.error(f"Failed to extract items: {e}")
            return []

    def extract_total_count(self, response: dict) -> Optional[int]:
        """
        Extract total count from API response.

        Args:
            response: Raw API response dictionary.

        Returns:
            Total count of items or None.
        """
        try:
            body = response.get("response", {}).get("body", {})
            total_count = body.get("totalCount")
            return int(total_count) if total_count else None
        except Exception:
            return None

    def transform(self, item: dict) -> dict:
        """
        Transform raw API item to standardized format.
        Override in subclass for custom transformation.

        Args:
            item: Raw item from API response.

        Returns:
            Transformed item dictionary.
        """
        return item

    def collect(
        self,
        params: dict = None,
        page_size: int = None,
    ) -> list:
        """
        Collect all data from the API.

        Args:
            params: Additional query parameters.
            page_size: Number of results per page.

        Returns:
            List of all collected and transformed items.
        """
        params = params or {}
        all_items = []

        self.logger.info(f"Starting collection from {self.base_url}{self.endpoint}")

        for page_items in self.paginate(params, page_size):
            transformed = [self.transform(item) for item in page_items]
            all_items.extend(transformed)
            self.logger.info(f"Collected {len(all_items)} items so far...")

        self.logger.info(f"Collection complete. Total items: {len(all_items)}")
        return all_items

    def collect_by_region(
        self,
        sido_codes: list[str] = None,
        sggu_codes: list[str] = None,
        page_size: int = None,
    ) -> list:
        """
        Collect data filtered by region codes.

        Args:
            sido_codes: List of sido (province) codes.
            sggu_codes: List of sggu (city/county) codes.
            page_size: Number of results per page.

        Returns:
            List of all collected and transformed items.
        """
        all_items = []

        if sido_codes:
            for sido_cd in sido_codes:
                params = {"sidoCd": sido_cd}
                items = self.collect(params, page_size)
                all_items.extend(items)
        elif sggu_codes:
            for sggu_cd in sggu_codes:
                params = {"sgguCd": sggu_cd}
                items = self.collect(params, page_size)
                all_items.extend(items)
        else:
            all_items = self.collect({}, page_size)

        return all_items

    def save_to_es(
        self,
        data: list,
        index_name: str = None,
        id_field: str = None,
    ) -> dict:
        """
        Elasticsearch에 데이터 저장 (Bulk API 사용).

        Args:
            data: 저장할 데이터 리스트.
            index_name: Elasticsearch 인덱스 이름 (기본값: self.es_index).
            id_field: 문서 ID로 사용할 필드명 (선택).

        Returns:
            저장 결과 통계 (success, failed 카운트).
        """
        if not data:
            self.logger.warning("저장할 데이터가 없습니다.")
            return {"success": 0, "failed": 0}

        from .utils.es_client import ElasticsearchClient

        index_name = index_name or self.es_index

        self.logger.info(f"Elasticsearch 저장 시작: {len(data)}건 -> {index_name}")

        with ElasticsearchClient() as es:
            es.create_index_if_not_exists(index_name)
            result = es.bulk_index(
                index_name=index_name,
                documents=data,
                id_field=id_field,
                raise_on_error=False,
            )

        self.logger.info(
            f"Elasticsearch 저장 완료: 성공={result['success']}, 실패={result['failed']}"
        )

        return {"success": result["success"], "failed": result["failed"]}

    def _make_request(
        self,
        url: str,
        params: dict = None,
        method: str = "GET",
        headers: dict = None,
        timeout: int = None,
    ) -> Optional[requests.Response]:
        """
        HTTP 요청 유틸리티 (재시도 로직 포함).

        Args:
            url: 요청 URL.
            params: 쿼리 파라미터 또는 POST 데이터.
            method: HTTP 메서드 (GET/POST).
            headers: 추가 헤더.
            timeout: 타임아웃 (초).

        Returns:
            Response 객체 또는 None (실패 시).
        """
        timeout = timeout or config.DEFAULT_TIMEOUT
        params = params or {}

        request_headers = {"Content-Type": "application/json"}
        if headers:
            request_headers.update(headers)

        for attempt in range(config.MAX_RETRIES):
            try:
                self.logger.debug(
                    f"HTTP 요청: {method} {url} (시도 {attempt + 1}/{config.MAX_RETRIES})"
                )

                if method.upper() == "GET":
                    response = self.session.get(
                        url,
                        params=params,
                        headers=request_headers,
                        timeout=timeout,
                    )
                elif method.upper() == "POST":
                    response = self.session.post(
                        url,
                        json=params,
                        headers=request_headers,
                        timeout=timeout,
                    )
                else:
                    self.logger.error(f"지원하지 않는 HTTP 메서드: {method}")
                    return None

                if response.status_code == 200:
                    return response

                if response.status_code in [429, 500, 502, 503, 504]:
                    self.logger.warning(
                        f"HTTP 요청 실패 (status={response.status_code}), "
                        f"재시도 중... ({attempt + 1}/{config.MAX_RETRIES})"
                    )
                    if attempt < config.MAX_RETRIES - 1:
                        time.sleep(config.RETRY_DELAY * (attempt + 1))
                    continue

                self.logger.error(
                    f"HTTP 요청 실패: status={response.status_code}, "
                    f"url={url}, response={response.text[:200]}"
                )
                return None

            except requests.Timeout:
                self.logger.warning(
                    f"HTTP 요청 타임아웃, 재시도 중... ({attempt + 1}/{config.MAX_RETRIES})"
                )
                if attempt < config.MAX_RETRIES - 1:
                    time.sleep(config.RETRY_DELAY * (attempt + 1))

            except requests.RequestException as e:
                self.logger.error(f"HTTP 요청 중 오류 발생: {e}")
                if attempt < config.MAX_RETRIES - 1:
                    time.sleep(config.RETRY_DELAY * (attempt + 1))

        self.logger.error(f"HTTP 요청 최종 실패: {url}")
        return None

    def _parse_json_response(self, response: requests.Response) -> Optional[dict]:
        """
        Response 객체에서 JSON 파싱.

        Args:
            response: requests.Response 객체.

        Returns:
            파싱된 JSON 딕셔너리 또는 None.
        """
        if response is None:
            return None
        try:
            return response.json()
        except Exception as e:
            self.logger.error(f"JSON 파싱 실패: {e}")
            return None

    def api_call(
        self,
        url: str,
        params: dict = None,
        headers: dict = None,
        max_retries: int = None,
    ) -> Optional[dict]:
        """
        API 호출 및 JSON 응답 반환.

        Args:
            url: API URL.
            params: 쿼리 파라미터.
            headers: 요청 헤더.
            max_retries: 최대 재시도 횟수 (기본값: config.MAX_RETRIES).

        Returns:
            JSON 응답 또는 None.
        """
        response = self._make_request(url, params=params, headers=headers)
        return self._parse_json_response(response)
