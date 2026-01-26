"""
플랫팜 발주 데이터 수집기 (Order Platpharm Collector)

내부 RDBMS(PostgreSQL)에서 의약품 발주 데이터를 수집합니다.

데이터 소스:
    - DB: PostgreSQL (localhost:12345, 세션 매니저 통해 접속)
    - 테이블: orders_detail
    - 인증: DB_USER, DB_PASSWORD, DB_NAME 환경변수

ES 인덱스: order_platpharm
"""

import os
import sys
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

# 패키지 경로 임포트
from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


class OrderPlatpharmCollector(BaseCollector):
    """
    플랫팜 의약품 발주 데이터 수집기

    PostgreSQL DB에서 orders_detail 테이블의 발주 데이터를 수집하여
    Elasticsearch에 저장합니다.

    Attributes:
        ES_INDEX (str): Elasticsearch 인덱스명
        TABLE_NAME (str): 조회 대상 테이블명
        BATCH_SIZE (int): 한 번에 조회할 레코드 수
    """

    ES_INDEX = "order_platpharm"
    TABLE_NAME = "orders_detail"
    BATCH_SIZE = 1000

    def __init__(self):
        """OrderPlatpharmCollector 초기화"""
        super().__init__(index_name=self.ES_INDEX)

        # DB 접속 정보 설정 (환경변수 우선, CONFIG fallback)
        rdbms_config = CONFIG.get("rdbms", {})
        self.db_host = os.getenv("DB_HOST", rdbms_config.get("host", "localhost"))
        self.db_port = int(os.getenv("DB_PORT", rdbms_config.get("port", 12345)))
        self.db_name = os.getenv("DB_NAME", rdbms_config.get("database", "platpharm"))
        self.db_user = os.getenv("DB_USER", rdbms_config.get("user", ""))
        self.db_password = os.getenv("DB_PASSWORD", rdbms_config.get("password", ""))

        self._connection: Optional[psycopg2.extensions.connection] = None

        # 접속 정보 검증
        if not self.db_user or not self.db_password:
            self.logger.warning(
                "DB_USER 또는 DB_PASSWORD가 설정되지 않았습니다. "
                ".env 파일을 확인하세요."
            )

    def _get_connection(self) -> psycopg2.extensions.connection:
        """
        PostgreSQL DB 연결 획득

        기존 연결이 유효하면 재사용하고, 없으면 새로 생성합니다.

        Returns:
            psycopg2 connection 객체

        Raises:
            psycopg2.Error: DB 연결 실패 시
        """
        if self._connection is None or self._connection.closed:
            self.logger.info(
                f"PostgreSQL 연결 생성: {self.db_host}:{self.db_port}/{self.db_name}"
            )
            self._connection = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                connect_timeout=30,
            )
        return self._connection

    def _close_connection(self) -> None:
        """DB 연결 종료"""
        if self._connection is not None and not self._connection.closed:
            self._connection.close()
            self.logger.info("PostgreSQL 연결 종료")
        self._connection = None

    def _get_total_count(self) -> int:
        """
        테이블 전체 레코드 수 조회

        Returns:
            전체 레코드 수
        """
        conn = self._get_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(self.TABLE_NAME)
                )
            )
            result = cursor.fetchone()
            return result[0] if result else 0

    def _fetch_batch(
        self,
        offset: int,
        limit: int
    ) -> List[Dict[str, Any]]:
        """
        배치 단위로 데이터 조회

        Args:
            offset: 시작 위치
            limit: 조회할 레코드 수

        Returns:
            조회된 레코드 리스트 (딕셔너리 형태)
        """
        conn = self._get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # 기본 SELECT * 쿼리 사용
            # 실제 테이블 스키마 확인 후 필요 시 컬럼 지정 가능
            query = sql.SQL(
                "SELECT * FROM {} ORDER BY id LIMIT %s OFFSET %s"
            ).format(sql.Identifier(self.TABLE_NAME))

            try:
                cursor.execute(query, (limit, offset))
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
            except psycopg2.Error as e:
                # id 컬럼이 없을 경우 ORDER BY 없이 재시도
                self.logger.warning(
                    f"id 컬럼으로 정렬 실패, ORDER BY 없이 재시도: {e}"
                )
                conn.rollback()

                query_no_order = sql.SQL(
                    "SELECT * FROM {} LIMIT %s OFFSET %s"
                ).format(sql.Identifier(self.TABLE_NAME))
                cursor.execute(query_no_order, (limit, offset))
                rows = cursor.fetchall()
                return [dict(row) for row in rows]

    def _serialize_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        DB 레코드를 ES 저장 가능한 형태로 변환

        Args:
            row: DB에서 조회한 레코드

        Returns:
            직렬화된 레코드
        """
        serialized = {}
        for key, value in row.items():
            if value is None:
                serialized[key] = None
            elif isinstance(value, datetime):
                # datetime을 ISO 형식 문자열로 변환
                serialized[key] = value.isoformat()
            elif isinstance(value, (int, float, str, bool)):
                serialized[key] = value
            else:
                # 기타 타입은 문자열로 변환
                serialized[key] = str(value)
        return serialized

    def collect(self) -> Generator[Dict[str, Any], None, None]:
        """
        발주 데이터 수집

        orders_detail 테이블에서 배치 단위로 데이터를 조회하여
        Generator로 반환합니다.

        Yields:
            수집된 발주 데이터 레코드
        """
        self.logger.info("플랫팜 발주 데이터 수집 시작")

        try:
            # 전체 건수 확인
            total_count = self._get_total_count()
            self.logger.info(f"전체 발주 레코드 수: {total_count}건")

            if total_count == 0:
                self.logger.warning("조회된 발주 데이터가 없습니다.")
                return

            # 배치 단위로 데이터 조회
            offset = 0
            collected_count = 0

            while offset < total_count:
                self.logger.info(
                    f"배치 조회 중: offset={offset}, "
                    f"진행률={offset * 100 // total_count}%"
                )

                rows = self._fetch_batch(offset, self.BATCH_SIZE)

                if not rows:
                    self.logger.warning(f"offset={offset}에서 데이터 없음, 종료")
                    break

                for row in rows:
                    # 데이터 직렬화 후 yield
                    serialized = self._serialize_row(row)
                    collected_count += 1
                    yield serialized

                offset += len(rows)

            self.logger.info(f"발주 데이터 수집 완료: 총 {collected_count}건")

        except psycopg2.Error as e:
            self.logger.error(f"DB 오류 발생: {e}")
            raise

        finally:
            self._close_connection()

    def get_table_schema(self) -> List[Dict[str, str]]:
        """
        테이블 스키마 정보 조회 (디버깅/확인용)

        Returns:
            컬럼 정보 리스트 (column_name, data_type, is_nullable)
        """
        conn = self._get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (self.TABLE_NAME,))
            return [dict(row) for row in cursor.fetchall()]

    def run(self) -> Dict[str, int]:
        """
        수집 및 Elasticsearch 저장 실행

        Returns:
            저장 결과 통계 (success, failed 카운트)
        """
        self.logger.info("OrderPlatpharmCollector 실행 시작")

        try:
            # 테이블 스키마 확인 (로깅)
            try:
                schema = self.get_table_schema()
                if schema:
                    self.logger.info(f"테이블 스키마 ({self.TABLE_NAME}):")
                    for col in schema:
                        self.logger.info(
                            f"  - {col['column_name']}: "
                            f"{col['data_type']} "
                            f"(nullable: {col['is_nullable']})"
                        )
                else:
                    self.logger.warning(
                        f"테이블 스키마를 찾을 수 없음: {self.TABLE_NAME}"
                    )
            except Exception as e:
                self.logger.warning(f"스키마 조회 실패 (계속 진행): {e}")

            # 데이터 수집
            all_orders = list(self.collect())

            if not all_orders:
                self.logger.warning("수집된 발주 데이터가 없습니다.")
                return {"success": 0, "failed": 0}

            # Elasticsearch 저장
            success_count = self.save_to_es(all_orders)

            result = {
                "success": success_count,
                "failed": len(all_orders) - success_count,
            }

            self.logger.info(
                f"OrderPlatpharmCollector 실행 완료: "
                f"성공={result['success']}, 실패={result['failed']}"
            )

            return result

        except Exception as e:
            self.logger.error(f"수집 실행 중 오류: {e}")
            raise

        finally:
            self._close_connection()


# CLI 실행 지원
if __name__ == "__main__":
    collector = OrderPlatpharmCollector()
    collector.run()
