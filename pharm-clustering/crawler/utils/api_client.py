"""
공공데이터 API 클라이언트

공공데이터포털(data.go.kr) API 호출을 위한 클라이언트 클래스입니다.
재시도 로직, 에러 핸들링, 페이지네이션 처리를 제공합니다.
"""

import time
import logging
from typing import Any, Optional
from urllib.parse import urlencode

import requests
from requests.exceptions import RequestException, Timeout, HTTPError

from crawler.config import config, CONFIG

logger = logging.getLogger(__name__)


class PublicDataAPIError(Exception):
    """공공데이터 API 에러"""

    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class PublicDataAPIClient:
    """
    공공데이터포털 API 클라이언트

    특징:
    - 재시도 로직 (최대 3회)
    - 타임아웃 설정 (30초)
    - 페이지네이션 자동 처리
    - 에러 핸들링
    """

    DEFAULT_TIMEOUT = 30  # 초
    MAX_RETRIES = 3
    RETRY_DELAY = 1.0  # 초

    def __init__(self, service_key: Optional[str] = None):
        """
        클라이언트 초기화

        Args:
            service_key: 공공데이터 API 서비스 키.
                        None인 경우 config에서 가져옴.
        """
        self.service_key = service_key or CONFIG["data_go_kr"]["service_key"]

        if not self.service_key:
            logger.warning("공공데이터 API 서비스 키가 설정되지 않았습니다.")

        self.session = requests.Session()

    def get(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None,
    ) -> dict:
        """
        GET 요청 수행

        Args:
            endpoint: API 엔드포인트 URL
            params: 요청 파라미터 (serviceKey는 자동 추가됨)
            timeout: 요청 타임아웃 (초). 기본값: 30초
            max_retries: 최대 재시도 횟수. 기본값: 3회

        Returns:
            API 응답 데이터 (dict)

        Raises:
            PublicDataAPIError: API 요청 실패 시
        """
        timeout = timeout or self.DEFAULT_TIMEOUT
        max_retries = max_retries or self.MAX_RETRIES

        # 파라미터 준비
        request_params = params.copy() if params else {}
        request_params["serviceKey"] = self.service_key

        last_exception: Optional[Exception] = None

        for attempt in range(1, max_retries + 1):
            try:
                logger.debug(f"API 요청 시도 {attempt}/{max_retries}: {endpoint}")

                response = self.session.get(
                    endpoint,
                    params=request_params,
                    timeout=timeout,
                )

                response.raise_for_status()

                # JSON 응답 파싱
                data = response.json()

                # 공공데이터 API 에러 코드 확인
                self._check_api_error(data)

                logger.debug(f"API 요청 성공: {endpoint}")
                return data

            except Timeout as e:
                last_exception = e
                logger.warning(f"API 요청 타임아웃 (시도 {attempt}/{max_retries}): {endpoint}")

            except HTTPError as e:
                last_exception = e
                status_code = e.response.status_code if e.response else None
                logger.warning(
                    f"HTTP 에러 {status_code} (시도 {attempt}/{max_retries}): {endpoint}"
                )

                # 4xx 에러는 재시도하지 않음 (400, 401, 403, 404 등)
                if status_code and 400 <= status_code < 500:
                    raise PublicDataAPIError(
                        f"클라이언트 에러: {e}",
                        status_code=status_code,
                    ) from e

            except RequestException as e:
                last_exception = e
                logger.warning(f"요청 에러 (시도 {attempt}/{max_retries}): {e}")

            except ValueError as e:
                # JSON 파싱 에러
                last_exception = e
                logger.warning(f"JSON 파싱 에러 (시도 {attempt}/{max_retries}): {e}")

            # 재시도 전 대기 (마지막 시도가 아닌 경우)
            if attempt < max_retries:
                delay = self.RETRY_DELAY * attempt  # 지수 백오프
                logger.debug(f"{delay}초 후 재시도...")
                time.sleep(delay)

        # 모든 재시도 실패
        raise PublicDataAPIError(
            f"API 요청 실패 (최대 재시도 횟수 초과): {endpoint}",
        ) from last_exception

    def get_all_pages(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        page_key: str = "pageNo",
        rows_key: str = "numOfRows",
        total_count_path: Optional[list[str]] = None,
        items_path: Optional[list[str]] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
    ) -> list[dict]:
        """
        페이지네이션을 처리하여 전체 데이터 수집

        Args:
            endpoint: API 엔드포인트 URL
            params: 요청 파라미터
            page_key: 페이지 번호 파라미터명. 기본값: "pageNo"
            rows_key: 페이지당 행 수 파라미터명. 기본값: "numOfRows"
            total_count_path: 전체 건수 경로 (예: ["response", "body", "totalCount"])
                             None인 경우 자동 탐지 시도
            items_path: 데이터 항목 경로 (예: ["response", "body", "items", "item"])
                       None인 경우 자동 탐지 시도
            page_size: 페이지당 행 수. 기본값: 100
            max_pages: 최대 페이지 수 제한. None인 경우 전체 수집

        Returns:
            전체 데이터 리스트

        Raises:
            PublicDataAPIError: API 요청 실패 시
        """
        all_items: list[dict] = []
        current_page = 1

        # 기본 파라미터 설정
        request_params = params.copy() if params else {}
        request_params[rows_key] = page_size

        while True:
            request_params[page_key] = current_page

            logger.info(f"페이지 {current_page} 수집 중: {endpoint}")

            data = self.get(endpoint, request_params)

            # 전체 건수 추출
            total_count = self._extract_value(data, total_count_path or ["response", "body", "totalCount"])
            if total_count is None:
                # 대체 경로 시도
                total_count = self._extract_value(data, ["response", "body", "total_count"])
                if total_count is None:
                    total_count = self._extract_value(data, ["totalCount"])

            # 데이터 항목 추출
            items = self._extract_value(data, items_path or ["response", "body", "items", "item"])
            if items is None:
                # 대체 경로 시도
                items = self._extract_value(data, ["response", "body", "items"])
                if items is None:
                    items = self._extract_value(data, ["items"])

            # 항목이 없으면 종료
            if not items:
                logger.info(f"페이지 {current_page}: 데이터 없음, 수집 종료")
                break

            # 단일 항목인 경우 리스트로 변환
            if isinstance(items, dict):
                items = [items]

            all_items.extend(items)
            logger.info(f"페이지 {current_page}: {len(items)}건 수집 (누적: {len(all_items)}건)")

            # 전체 수집 완료 확인
            if total_count is not None:
                try:
                    total_count = int(total_count)
                    if len(all_items) >= total_count:
                        logger.info(f"전체 {total_count}건 수집 완료")
                        break
                except (ValueError, TypeError):
                    pass

            # 페이지 크기보다 적게 반환된 경우 마지막 페이지
            if len(items) < page_size:
                logger.info(f"마지막 페이지 도달 (수신: {len(items)} < 요청: {page_size})")
                break

            # 최대 페이지 제한 확인
            if max_pages and current_page >= max_pages:
                logger.info(f"최대 페이지 수 {max_pages} 도달, 수집 중단")
                break

            current_page += 1

        logger.info(f"페이지네이션 완료: 총 {len(all_items)}건 수집")
        return all_items

    def _check_api_error(self, data: dict) -> None:
        """
        공공데이터 API 에러 코드 확인

        Args:
            data: API 응답 데이터

        Raises:
            PublicDataAPIError: API 에러 코드가 있는 경우
        """
        # 일반적인 공공데이터 API 응답 구조 확인
        header = data.get("response", {}).get("header", {})
        result_code = header.get("resultCode")
        result_msg = header.get("resultMsg", "")

        # 정상 코드 (00, 0, "00" 등)
        if result_code in ("00", "0", 0, None):
            return

        # 에러 코드별 메시지
        error_messages = {
            "01": "어플리케이션 에러",
            "02": "DB 에러",
            "03": "데이터 없음",
            "04": "HTTP 에러",
            "05": "서비스 연결 실패",
            "10": "잘못된 요청 파라미터",
            "11": "필수 파라미터 누락",
            "12": "해당 오픈 API 서비스가 없거나 폐기됨",
            "20": "서비스 접근 거부",
            "21": "일시적으로 사용할 수 없는 서비스 키",
            "22": "서비스 요청 제한 횟수 초과",
            "30": "등록되지 않은 서비스 키",
            "31": "기한 만료된 서비스 키",
            "32": "등록되지 않은 IP",
            "33": "서명되지 않은 호출",
            "99": "기타 에러",
        }

        error_msg = error_messages.get(str(result_code), result_msg)
        raise PublicDataAPIError(
            f"API 에러 [{result_code}]: {error_msg}",
            response=data,
        )

    def _extract_value(self, data: dict, path: list[str]) -> Any:
        """
        중첩된 딕셔너리에서 경로에 해당하는 값 추출

        Args:
            data: 데이터 딕셔너리
            path: 키 경로 리스트

        Returns:
            추출된 값 또는 None
        """
        current = data
        for key in path:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current

    def close(self) -> None:
        """세션 종료"""
        self.session.close()

    def __enter__(self) -> "PublicDataAPIClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
