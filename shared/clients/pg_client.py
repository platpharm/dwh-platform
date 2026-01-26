"""PostgreSQL 클라이언트"""
from typing import Any, Dict, List, Optional, Generator
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
from ..config import pg_config


class PGClient:
    """PostgreSQL 클라이언트 래퍼"""

    def __init__(self):
        self.config = pg_config

    @contextmanager
    def get_connection(self):
        """컨텍스트 매니저로 연결 관리"""
        conn = psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.user,
            password=self.config.password,
        )
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def get_cursor(self, dict_cursor: bool = True):
        """컨텍스트 매니저로 커서 관리"""
        with self.get_connection() as conn:
            cursor_factory = RealDictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()

    def health_check(self) -> bool:
        """DB 연결 상태 확인"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except Exception:
            return False

    def execute(self, query: str, params: Optional[tuple] = None) -> None:
        """쿼리 실행 (결과 없음)"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)

    def fetch_one(
        self,
        query: str,
        params: Optional[tuple] = None
    ) -> Optional[Dict[str, Any]]:
        """단일 행 조회"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchone()

    def fetch_all(
        self,
        query: str,
        params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """전체 행 조회"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def fetch_many(
        self,
        query: str,
        params: Optional[tuple] = None,
        size: int = 1000
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """배치 단위로 조회 (대용량 데이터)"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            while True:
                rows = cursor.fetchmany(size)
                if not rows:
                    break
                yield rows

    def fetch_with_server_cursor(
        self,
        query: str,
        params: Optional[tuple] = None,
        batch_size: int = 1000,
        cursor_name: str = "server_cursor"
    ) -> Generator[Dict[str, Any], None, None]:
        """서버 사이드 커서로 조회 (메모리 효율적)"""
        with self.get_connection() as conn:
            with conn.cursor(name=cursor_name, cursor_factory=RealDictCursor) as cursor:
                cursor.itersize = batch_size
                cursor.execute(query, params)
                for row in cursor:
                    yield row

    # 도메인별 조회 메서드
    def get_orders_detail(
        self,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """주문 상세 데이터 조회"""
        query = """
            SELECT od.*, o.account_id, o.ordered_at
            FROM orders_detail od
            JOIN orders o ON od.order_id = o.id
            WHERE od.deleted_at IS NULL
            ORDER BY od.id
        """
        if limit:
            query += f" LIMIT {limit} OFFSET {offset}"
        return self.fetch_all(query)

    def get_products(self) -> List[Dict[str, Any]]:
        """상품 데이터 조회"""
        query = """
            SELECT id, name, efficacy, ingredient, category2, std,
                   thumb_img1, vendor_id
            FROM product
            WHERE deleted_at IS NULL
            ORDER BY id
        """
        return self.fetch_all(query)

    def get_accounts(self) -> List[Dict[str, Any]]:
        """약국 데이터 조회"""
        query = """
            SELECT id, name, address, lat, lng, phone
            FROM account
            WHERE deleted_at IS NULL
            ORDER BY id
        """
        return self.fetch_all(query)

    def get_vendors(self) -> List[Dict[str, Any]]:
        """도매사 데이터 조회"""
        query = """
            SELECT id, name
            FROM vendor
            WHERE deleted_at IS NULL
            ORDER BY id
        """
        return self.fetch_all(query)


# 싱글톤 인스턴스
pg_client = PGClient()
