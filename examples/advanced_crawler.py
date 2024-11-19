from src.crawler.spiders.keyword_spider import KeywordSpider
from src.crawler.middlewares.user_agent import RandomUserAgentMiddleware
from src.crawler.middlewares.proxy import ProxyMiddleware
from src.crawler.utils.deduplication import URLDeduplicator
from src.config.settings import CrawlerSettings
from src.database.connection import DatabaseConnection
from src.database.models import URLModel

def main():
    # Advanced configuration
    settings = CrawlerSettings()
    settings.CONCURRENT_REQUESTS = 32
    settings.DOWNLOAD_DELAY = 0.5
    settings.RETRY_TIMES = 3
    settings.COOKIES_ENABLED = False
    
    # Initialize database connection
    db = DatabaseConnection()
    
    # Initialize URL deduplicator
    deduplicator = URLDeduplicator()
    
    # Configure middlewares
    middlewares = {
        'user_agent': RandomUserAgentMiddleware(),
        'proxy': ProxyMiddleware(
            proxy_list=['http://proxy1.example.com', 'http://proxy2.example.com']
        )
    }

    # Initialize spider with advanced settings
    spider = KeywordSpider(
        start_urls=[
            'https://example.com',
            'https://example.org'
        ],
        allowed_domains=[
            'example.com',
            'example.org'
        ],
        keywords=['python', 'web crawling', 'data mining'],
        max_depth=3,
        follow_links=True,
        middlewares=middlewares,
        deduplicator=deduplicator
    )

    # Custom callback for processing found URLs
    def process_url(url, metadata):
        if url and not deduplicator.is_duplicate(url):
            url_model = URLModel(
                url=url,
                domain=metadata.get('domain'),
                depth=metadata.get('depth'),
                found_date=metadata.get('timestamp')
            )
            db.save(url_model)

    spider.set_url_callback(process_url)

    # Run crawler with error handling
    try:
        spider.start_crawling()
    except Exception as e:
        print(f"Crawling error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    main()
