"""Elasticsearch 클라이언트"""
from typing import Any, Dict, List, Optional, Generator
from elasticsearch import Elasticsearch, helpers
from ..config import es_config, ESIndex


class ESClient:
    """Elasticsearch 클라이언트 래퍼"""

    def __init__(self):
        self.client = Elasticsearch(
            hosts=es_config.hosts,
            basic_auth=(es_config.username, es_config.password)
            if es_config.username and es_config.password else None,
            request_timeout=30,
            verify_certs=False,
        )

    def health_check(self) -> bool:
        """ES 연결 상태 확인"""
        try:
            return self.client.ping()
        except Exception:
            return False

    def index_document(
        self,
        index: str,
        document: Dict[str, Any],
        doc_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """단일 문서 인덱싱"""
        return self.client.index(index=index, id=doc_id, document=document)

    def bulk_index(
        self,
        index: str,
        documents: List[Dict[str, Any]],
        id_field: Optional[str] = None
    ) -> tuple:
        """대량 문서 인덱싱"""
        def generate_actions() -> Generator:
            for doc in documents:
                action = {
                    "_index": index,
                    "_source": doc,
                }
                if id_field and id_field in doc:
                    action["_id"] = doc[id_field]
                yield action

        return helpers.bulk(self.client, generate_actions())

    def search(
        self,
        index: str,
        query: Dict[str, Any],
        size: int = 100,
        source: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """문서 검색"""
        body = {"query": query, "size": size}
        if source:
            body["_source"] = source

        response = self.client.search(index=index, body=body)
        return [hit["_source"] for hit in response["hits"]["hits"]]

    def scroll_search(
        self,
        index: str,
        query: Dict[str, Any],
        scroll: str = "5m",
        size: int = 1000
    ) -> Generator[Dict[str, Any], None, None]:
        """스크롤 검색 (대용량 데이터)"""
        response = self.client.search(
            index=index,
            body={"query": query},
            scroll=scroll,
            size=size
        )

        scroll_id = response["_scroll_id"]
        hits = response["hits"]["hits"]

        while hits:
            for hit in hits:
                yield hit["_source"]

            response = self.client.scroll(scroll_id=scroll_id, scroll=scroll)
            scroll_id = response["_scroll_id"]
            hits = response["hits"]["hits"]

        self.client.clear_scroll(scroll_id=scroll_id)

    def get_document(self, index: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """단일 문서 조회"""
        try:
            response = self.client.get(index=index, id=doc_id)
            return response["_source"]
        except Exception:
            return None

    def delete_by_query(self, index: str, query: Dict[str, Any]) -> Dict[str, Any]:
        """쿼리로 문서 삭제"""
        return self.client.delete_by_query(index=index, body={"query": query})

    def create_index(
        self,
        index: str,
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """인덱스 생성"""
        body = {}
        if mappings:
            body["mappings"] = mappings
        if settings:
            body["settings"] = settings

        return self.client.indices.create(index=index, body=body, ignore=400)

    def index_exists(self, index: str) -> bool:
        """인덱스 존재 여부 확인"""
        return self.client.indices.exists(index=index)


# 싱글톤 인스턴스
es_client = ESClient()
