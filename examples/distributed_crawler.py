from src.crawler.spiders.keyword_spider import KeywordSpider
from src.config.settings import CrawlerSettings
from src.database.connection import DatabaseConnection
from src.crawler.utils.validation import URLValidator
from concurrent.futures import ThreadPoolExecutor
import queue
import threading

class DistributedCrawler:
    def __init__(self, num_workers=4):
        self.num_workers = num_workers
        self.url_queue = queue.Queue()
        self.results = queue.Queue()
        self.settings = CrawlerSettings()
        self.db = DatabaseConnection()
        self.validator = URLValidator()
        
    def worker(self, worker_id):
        spider = KeywordSpider(
            start_urls=[],  # Will be fed through queue
            allowed_domains=['*'],  # Allow any domain
            keywords=['python', 'programming'],
            max_depth=2
        )
        
        while True:
            try:
                url = self.url_queue.get(timeout=5)
                if self.validator.is_valid(url):
                    spider.crawl_url(url)
                self.url_queue.task_done()
            except queue.Empty:
                break
            except Exception as e:
                print(f"Worker {worker_id} error: {e}")

    def run(self, start_urls):
        # Load initial URLs
        for url in start_urls:
            self.url_queue.put(url)

        # Start worker threads
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            workers = [
                executor.submit(self.worker, i) 
                for i in range(self.num_workers)
            ]

        # Wait for completion
        self.url_queue.join()

def main():
    # Example usage of distributed crawler
    start_urls = [
        'https://example1.com',
        'https://example2.com',
        'https://example3.com',
        'https://example4.com'
    ]
    
    crawler = DistributedCrawler(num_workers=4)
    
    try:
        crawler.run(start_urls)
    except KeyboardInterrupt:
        print("Crawling interrupted by user")
    except Exception as e:
        print(f"Crawling error: {e}")

if __name__ == "__main__":
    main()
