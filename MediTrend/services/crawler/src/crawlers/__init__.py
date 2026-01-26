"""크롤러 모듈"""

from .google_trends import GoogleTrendsCrawler
from .paper_crawler import PaperCrawler

__all__ = ["GoogleTrendsCrawler", "PaperCrawler"]
