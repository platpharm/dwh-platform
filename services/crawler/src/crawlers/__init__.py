"""크롤러 모듈"""

from .naver_datalab import NaverDatalabCrawler
from .google_trends import GoogleTrendsCrawler
from .paper_crawler import PaperCrawler

__all__ = ["NaverDatalabCrawler", "GoogleTrendsCrawler", "PaperCrawler"]
