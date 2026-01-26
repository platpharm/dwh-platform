"""논문 제목 크롤러 (PubMed, arXiv 등)"""

import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from xml.etree import ElementTree

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from shared.clients.es_client import es_client
from shared.models.schemas import TrendData
from shared.config import ESIndex


class PaperCrawler:
    """논문 검색 크롤러 (PubMed, arXiv 지원)"""

    PUBMED_SEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    PUBMED_FETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    ARXIV_API_URL = "http://export.arxiv.org/api/query"

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    async def search_pubmed(
        self,
        keywords: List[str],
        max_results: int = 100,
    ) -> List[Dict[str, Any]]:
        """PubMed에서 논문 검색"""
        results = []

        for keyword in keywords:
            # 1. 검색하여 ID 목록 가져오기
            search_params = {
                "db": "pubmed",
                "term": f"{keyword}[Title/Abstract]",
                "retmax": max_results,
                "retmode": "json",
                "sort": "relevance",
            }

            response = await self.client.get(
                self.PUBMED_SEARCH_URL,
                params=search_params,
            )
            response.raise_for_status()
            search_data = response.json()

            id_list = search_data.get("esearchresult", {}).get("idlist", [])

            if not id_list:
                continue

            # 2. ID로 상세 정보 가져오기
            fetch_params = {
                "db": "pubmed",
                "id": ",".join(id_list),
                "retmode": "xml",
            }

            response = await self.client.get(
                self.PUBMED_FETCH_URL,
                params=fetch_params,
            )
            response.raise_for_status()

            # XML 파싱
            root = ElementTree.fromstring(response.content)

            for article in root.findall(".//PubmedArticle"):
                title_elem = article.find(".//ArticleTitle")
                abstract_elem = article.find(".//AbstractText")
                pubdate_elem = article.find(".//PubDate")
                pmid_elem = article.find(".//PMID")

                title = title_elem.text if title_elem is not None else ""
                abstract = abstract_elem.text if abstract_elem is not None else ""
                pmid = pmid_elem.text if pmid_elem is not None else ""

                # 출판일 파싱
                pub_date = datetime.now()
                if pubdate_elem is not None:
                    year = pubdate_elem.find("Year")
                    month = pubdate_elem.find("Month")
                    if year is not None:
                        try:
                            month_num = int(month.text) if month is not None else 1
                            pub_date = datetime(int(year.text), month_num, 1)
                        except (ValueError, TypeError):
                            pass

                results.append({
                    "keyword": keyword,
                    "source": "pubmed",
                    "title": title,
                    "abstract": abstract[:500] if abstract else "",
                    "paper_id": pmid,
                    "pub_date": pub_date,
                    "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                })

            # Rate limiting
            time.sleep(0.5)

        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    async def search_arxiv(
        self,
        keywords: List[str],
        max_results: int = 100,
    ) -> List[Dict[str, Any]]:
        """arXiv에서 논문 검색"""
        results = []

        for keyword in keywords:
            params = {
                "search_query": f"all:{keyword}",
                "start": 0,
                "max_results": max_results,
                "sortBy": "relevance",
                "sortOrder": "descending",
            }

            response = await self.client.get(
                self.ARXIV_API_URL,
                params=params,
            )
            response.raise_for_status()

            # XML 파싱 (Atom 형식)
            root = ElementTree.fromstring(response.content)
            ns = {"atom": "http://www.w3.org/2005/Atom"}

            for entry in root.findall("atom:entry", ns):
                title_elem = entry.find("atom:title", ns)
                summary_elem = entry.find("atom:summary", ns)
                id_elem = entry.find("atom:id", ns)
                published_elem = entry.find("atom:published", ns)

                title = title_elem.text.strip() if title_elem is not None else ""
                summary = summary_elem.text.strip() if summary_elem is not None else ""
                arxiv_id = id_elem.text if id_elem is not None else ""

                # 출판일 파싱
                pub_date = datetime.now()
                if published_elem is not None:
                    try:
                        pub_date = datetime.fromisoformat(
                            published_elem.text.replace("Z", "+00:00")
                        )
                    except ValueError:
                        pass

                # arXiv ID 추출
                paper_id = arxiv_id.split("/")[-1] if arxiv_id else ""

                results.append({
                    "keyword": keyword,
                    "source": "arxiv",
                    "title": title,
                    "abstract": summary[:500] if summary else "",
                    "paper_id": paper_id,
                    "pub_date": pub_date,
                    "url": arxiv_id,
                })

            # Rate limiting
            time.sleep(0.5)

        return results

    def _calculate_relevance_score(
        self,
        paper: Dict[str, Any],
        keyword: str,
    ) -> float:
        """논문 관련도 점수 계산 (0-100)"""
        score = 0.0
        title = paper.get("title", "").lower()
        abstract = paper.get("abstract", "").lower()
        keyword_lower = keyword.lower()

        # 제목에 키워드 포함 시 가중치
        if keyword_lower in title:
            score += 50.0

        # 초록에 키워드 포함 시 가중치
        if keyword_lower in abstract:
            keyword_count = abstract.count(keyword_lower)
            score += min(30.0, keyword_count * 10.0)

        # 최근 논문 가중치
        pub_date = paper.get("pub_date", datetime.now())
        if isinstance(pub_date, datetime):
            days_old = (datetime.now() - pub_date).days
            recency_score = max(0, 20 - (days_old / 30))  # 최대 20점
            score += recency_score

        return min(100.0, score)

    def _parse_to_trend_data(
        self,
        papers: List[Dict[str, Any]],
    ) -> List[TrendData]:
        """논문 데이터를 TrendData 스키마로 변환"""
        trend_data_list = []

        for paper in papers:
            keyword = paper.get("keyword", "")
            score = self._calculate_relevance_score(paper, keyword)

            trend_data = TrendData(
                keyword=keyword,
                source="paper",
                value=score,
                date=paper.get("pub_date", datetime.now()),
                metadata={
                    "title": paper.get("title", ""),
                    "abstract": paper.get("abstract", ""),
                    "paper_id": paper.get("paper_id", ""),
                    "paper_source": paper.get("source", ""),
                    "url": paper.get("url", ""),
                },
            )
            trend_data_list.append(trend_data)

        return trend_data_list

    async def crawl_and_save(
        self,
        keywords: List[str],
        sources: Optional[List[str]] = None,
        max_results: int = 50,
    ) -> int:
        """크롤링 후 ES에 저장"""
        if sources is None:
            sources = ["pubmed", "arxiv"]

        all_papers = []

        if "pubmed" in sources:
            pubmed_papers = await self.search_pubmed(keywords, max_results)
            all_papers.extend(pubmed_papers)

        if "arxiv" in sources:
            arxiv_papers = await self.search_arxiv(keywords, max_results)
            all_papers.extend(arxiv_papers)

        if not all_papers:
            return 0

        trend_data_list = self._parse_to_trend_data(all_papers)

        # TrendData를 dict로 변환
        documents = [
            {
                **data.model_dump(),
                "date": data.date.isoformat(),
            }
            for data in trend_data_list
        ]

        # ES에 bulk 저장
        success, _ = es_client.bulk_index(
            index=ESIndex.TREND_DATA,
            documents=documents,
        )

        return success


# 팩토리 함수 (async context manager 사용을 위해)
def create_paper_crawler() -> PaperCrawler:
    """PaperCrawler 인스턴스 생성"""
    return PaperCrawler()
