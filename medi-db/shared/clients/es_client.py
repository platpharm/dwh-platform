import logging
import time
import urllib3
from typing import Any, Dict, List, Optional, Generator
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError as ESConnectionError
from ..config import es_config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

class ESClient:

    def __init__(self, max_retries: int = 3, retry_delay: float = 2.0):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.client = self._create_client()

    def _create_client(self) -> Elasticsearch:
        primary_url = f"{es_config.scheme}://{es_config.host}:{es_config.port}"

        fallback_url = f"http://{es_config.host}:{es_config.port}"

        auth = None
        if es_config.username and es_config.password:
            auth = (es_config.username, es_config.password)

        client = self._try_connect(primary_url, auth)
        if client:
            logger.info(f"ES 연결 성공: {primary_url}")
            return client

        if es_config.scheme == "https" and fallback_url != primary_url:
            logger.warning(f"HTTPS 연결 실패, HTTP로 fallback 시도: {fallback_url}")
            client = self._try_connect(fallback_url, auth)
            if client:
                logger.info(f"ES HTTP 연결 성공: {fallback_url}")
                return client

        logger.warning(f"ES 초기 연결 실패, 클라이언트 생성만 수행: {primary_url}")
        return Elasticsearch(
            hosts=[primary_url],
            basic_auth=auth,
            request_timeout=30,
            verify_certs=False,
            ssl_show_warn=False,
            retry_on_timeout=True,
            max_retries=self.max_retries,
        )

    def _try_connect(self, url: str, auth: Optional[tuple]) -> Optional[Elasticsearch]:
        for attempt in range(self.max_retries):
            client = None
            try:
                client = Elasticsearch(
                    hosts=[url],
                    basic_auth=auth,
                    request_timeout=30,
                    verify_certs=False,
                    ssl_show_warn=False,
                    retry_on_timeout=True,
                    max_retries=self.max_retries,
                )
                if client.ping():
                    return client
                else:
                    client.close()
            except ESConnectionError as e:
                if client:
                    client.close()
                logger.warning(f"ES 연결 시도 {attempt + 1}/{self.max_retries} 실패: {url} - {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
            except Exception as e:
                if client:
                    client.close()
                logger.warning(f"ES 연결 오류: {url} - {e}")
                break
        return None

    def health_check(self) -> bool:
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
        return self.client.index(index=index, id=doc_id, document=document)

    def bulk_index(
        self,
        index: str,
        documents: List[Dict[str, Any]],
        id_field: Optional[str] = None
    ) -> tuple:
        def generate_actions() -> Generator:
            for doc in documents:
                action = {
                    "_index": index,
                    "_source": doc,
                }
                if id_field and id_field in doc:
                    action["_id"] = doc[id_field]
                yield action

        try:
            success, errors = helpers.bulk(
                self.client,
                generate_actions(),
                raise_on_error=False,
                raise_on_exception=False,
            )
            if errors:
                logger.error(f"bulk_index 부분 실패: {len(errors)}건 실패 (총 {success + len(errors)}건 중)")
                for err in errors[:5]:
                    logger.error(f"  실패 상세: {err}")
            return success, errors
        except Exception as e:
            logger.error(f"bulk_index 전체 실패: {e}")
            raise

    def search(
        self,
        index: str,
        query: Dict[str, Any],
        size: int = 100,
        source: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
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
        scroll_id = None
        try:
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
        finally:
            if scroll_id is not None:
                try:
                    self.client.clear_scroll(scroll_id=scroll_id)
                except Exception as e:
                    logger.warning(f"스크롤 컨텍스트 정리 실패: {e}")

    def get_document(self, index: str, doc_id: str) -> Optional[Dict[str, Any]]:
        try:
            response = self.client.get(index=index, id=doc_id)
            return response["_source"]
        except Exception:
            return None

    def aggregate(
        self,
        index: str,
        query: Dict[str, Any],
        aggs: Dict[str, Any],
        size: int = 0
    ) -> Dict[str, Any]:
        body = {"query": query, "size": size, "aggs": aggs}
        response = self.client.search(index=index, body=body)
        return response.get("aggregations", {})

    def delete_by_query(self, index: str, query: Dict[str, Any]) -> Dict[str, Any]:
        result = self.client.delete_by_query(
            index=index,
            body={"query": query},
            conflicts="proceed",
        )
        if result.get("failures"):
            logger.warning(
                f"delete_by_query 부분 실패: {len(result['failures'])}건, "
                f"삭제 완료: {result.get('deleted', 0)}건"
            )
        return result

    def create_index(
        self,
        index: str,
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        body = {}
        if mappings:
            body["mappings"] = mappings
        if settings:
            body["settings"] = settings

        return self.client.indices.create(index=index, body=body, ignore=400)

    def index_exists(self, index: str) -> bool:
        return self.client.indices.exists(index=index)

es_client = ESClient()
