from src.crawler.spiders.keyword_spider import KeywordSpider
from src.config.settings import CrawlerSettings
from src.database.connection import DatabaseConnection

def main():
    # Basic configuration
    settings = CrawlerSettings()
    settings.CONCURRENT_REQUESTS = 16
    settings.DOWNLOAD_DELAY = 1.0
    settings.USER_AGENT = 'Mozilla/5.0 (compatible; URLCrawler/1.0)'

    # Initialize spider with basic settings
    spider = KeywordSpider(
        start_urls=['https://example.com'],
        allowed_domains=['example.com'],
        keywords=['technology', 'programming']
    )

    # Run crawler
    try:
        spider.start_crawling()
    except Exception as e:
        print(f"Crawling error: {e}")

if __name__ == "__main__":
    main()
