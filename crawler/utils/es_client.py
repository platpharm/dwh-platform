"""
Elasticsearch Client Module

Elasticsearch 연결 및 데이터 인덱싱을 위한 클라이언트 클래스
"""

import logging
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from crawler.config import CONFIG

logger = logging.getLogger(__name__)


class ElasticsearchClient:
    """
    Elasticsearch 클라이언트 래퍼 클래스

    Elasticsearch 연결, 인덱스 관리, 벌크 인덱싱 기능을 제공합니다.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
    ):
        """
        ElasticsearchClient 초기화

        Args:
            host: Elasticsearch 호스트. None이면 config에서 가져옴
            port: Elasticsearch 포트. None이면 config에서 가져옴
        """
        es_config = CONFIG.get("elasticsearch", {})
        self._host = host or es_config.get("host", "localhost")
        self._port = port or es_config.get("port", 54321)
        self._client: Optional[Elasticsearch] = None

    def _connect(self) -> Elasticsearch:
        """Elasticsearch 연결 생성"""
        return Elasticsearch(
            hosts=[{"host": self._host, "port": self._port, "scheme": "http"}],
            request_timeout=30,
            max_retries=3,
            retry_on_timeout=True,
        )

    def get_client(self) -> Elasticsearch:
        """
        Elasticsearch 클라이언트 반환

        연결이 없거나 끊어진 경우 새로 연결합니다.

        Returns:
            Elasticsearch 클라이언트 인스턴스
        """
        if self._client is None:
            self._client = self._connect()
            logger.info(f"Elasticsearch 연결 완료: {self._host}:{self._port}")
        return self._client

    def create_index_if_not_exists(
        self,
        index_name: str,
        mapping: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        인덱스가 존재하지 않으면 생성

        Args:
            index_name: 생성할 인덱스 이름
            mapping: 인덱스 매핑 설정. None이면 기본 매핑 사용

        Returns:
            인덱스가 새로 생성되었으면 True, 이미 존재하면 False
        """
        client = self.get_client()

        if client.indices.exists(index=index_name):
            logger.info(f"인덱스 '{index_name}'가 이미 존재합니다.")
            return False

        # 기본 인덱스 설정
        body: Dict[str, Any] = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "korean": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase"],
                        }
                    }
                },
            }
        }

        if mapping:
            body["mappings"] = mapping

        client.indices.create(index=index_name, body=body)
        logger.info(f"인덱스 '{index_name}' 생성 완료")
        return True

    def bulk_index(
        self,
        index_name: str,
        documents: List[Dict[str, Any]],
        id_field: Optional[str] = None,
        chunk_size: int = 500,
        raise_on_error: bool = True,
    ) -> Dict[str, int]:
        """
        문서들을 벌크로 인덱싱

        Args:
            index_name: 대상 인덱스 이름
            documents: 인덱싱할 문서 리스트
            id_field: 문서 ID로 사용할 필드명. None이면 자동 생성
            chunk_size: 한 번에 처리할 문서 수
            raise_on_error: 에러 발생시 예외를 발생시킬지 여부

        Returns:
            성공/실패 카운트를 담은 딕셔너리
            {
                "success": 성공한 문서 수,
                "failed": 실패한 문서 수,
                "total": 전체 문서 수
            }
        """
        if not documents:
            logger.warning("인덱싱할 문서가 없습니다.")
            return {"success": 0, "failed": 0, "total": 0}

        client = self.get_client()

        def generate_actions():
            for doc in documents:
                action = {
                    "_index": index_name,
                    "_source": doc,
                }
                if id_field and id_field in doc:
                    action["_id"] = doc[id_field]
                yield action

        success_count = 0
        failed_count = 0

        try:
            success, failed = bulk(
                client,
                generate_actions(),
                chunk_size=chunk_size,
                raise_on_error=raise_on_error,
                raise_on_exception=raise_on_error,
            )
            success_count = success
            if isinstance(failed, list):
                failed_count = len(failed)
            else:
                failed_count = failed

        except Exception as e:
            logger.error(f"벌크 인덱싱 중 오류 발생: {e}")
            if raise_on_error:
                raise
            failed_count = len(documents)

        result = {
            "success": success_count,
            "failed": failed_count,
            "total": len(documents),
        }

        logger.info(
            f"벌크 인덱싱 완료 - 인덱스: {index_name}, "
            f"성공: {success_count}, 실패: {failed_count}, 전체: {len(documents)}"
        )

        return result

    def close(self) -> None:
        """Elasticsearch 연결 종료"""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("Elasticsearch 연결 종료")

    def __enter__(self) -> "ElasticsearchClient":
        """Context manager 진입"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager 종료"""
        self.close()
