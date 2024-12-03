import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.exceptions import CloseSpider
from typing import Set, List, Dict, Any, Optional
from urllib.parse import urlparse
import logging
from datetime import datetime
import hashlib
from src.crawler.utils.validation import URLValidator
from src.crawler.utils.normalize import URLNormalizer
from src.database.models import URLDocument

class KeywordURLSpider(CrawlSpider):
    name = 'keyword_spider'
    
    def __init__(
        self,
        start_urls: List[str],
        allowed_domains: Optional[List[str]] = None,
        keywords: Optional[List[str]] = None,
        max_depth: int = 3,
        max_urls: int = 250000,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        
        # Initialize configurations
        self.start_urls = start_urls
        self.allowed_domains = allowed_domains or [
            urlparse(url).netloc for url in start_urls
        ]
        self.keywords = keywords or []
        self.max_depth = max_depth
        self.max_urls = max_urls
        
        # Initialize counters and sets
        self.urls_processed = 0
        self.seen_urls: Set[str] = set()
        
        # Initialize utilities
        self.url_validator = URLValidator()
        self.url_normalizer = URLNormalizer()
        self.logger = logging.getLogger(__name__)
        
        # Define crawl rules
        self.rules = (
            Rule(
                LinkExtractor(
                    allow_domains=self.allowed_domains,
                    deny=r'.*\.(pdf|zip|rar|tar|gz|doc|docx|xls|xlsx|jpg|jpeg|png|gif|mp4|mp3)$',
                    unique=True
                ),
                callback='parse_item',
                follow=True,
                process_links='process_links',
                errback='handle_error'
            ),
        )
        
        # Initialize stats
        self.crawler.stats.set_value('start_time', datetime.utcnow())
        self.crawler.stats.set_value('urls_processed', 0)
        self.crawler.stats.set_value('urls_failed', 0)

    def start_requests(self):
        """Initialize requests with start URLs"""
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse_item,
                errback=self.handle_error,
                meta={'depth': 0}
            )

    def process_links(self, links: List[scrapy.Link]) -> List[scrapy.Link]:
        """Process and filter extracted links"""
        processed_links = []
        for link in links:
            # Skip if max URLs reached
            if self.urls_processed >= self.max_urls:
                raise CloseSpider('Max URLs limit reached')
            
            # Validate and normalize URL
            validation_result = self.url_validator.is_valid_url(link.url)
            if not validation_result.is_valid:
                self.logger.debug(f"Skipping invalid URL {link.url}: {validation_result.reason}")
                continue
            
            normalized_url = self.url_normalizer.normalize(link.url)
            if not normalized_url:
                continue
                
            # Check if URL was already seen
            url_hash = hashlib.sha256(normalized_url.encode()).hexdigest()
            if url_hash in self.seen_urls:
                continue
                
            self.seen_urls.add(url_hash)
            processed_links.append(link)
            
        return processed_links

    def parse_item(self, response):
        """Parse individual pages"""
        try:
            # Extract page information
            url = response.url
            normalized_url = self.url_normalizer.normalize(url)
            url_hash = hashlib.sha256(normalized_url.encode()).hexdigest()
            depth = response.meta.get('depth', 0)
            
            # Extract page metadata
            title = response.css('title::text').get()
            description = response.css('meta[name="description"]::attr(content)').get()
            
            # Check for keywords if specified
            found_keywords = []
            if self.keywords:
                page_text = ' '.join(response.css('body ::text').getall()).lower()
                found_keywords = [
                    keyword for keyword in self.keywords
                    if keyword.lower() in page_text
                ]
            
            # Create item
            item = URLDocument(
                url=url,
                url_hash=url_hash,
                normalized_url=normalized_url,
                domain=urlparse(url).netloc,
                source_url=response.request.headers.get('Referer', b'').decode(),
                depth=depth,
                keywords=found_keywords,
                title=title,
                description=description,
                status_code=response.status,
                content_type=response.headers.get('Content-Type', b'').decode(),
                timestamp=datetime.utcnow(),
                metadata={
                    'headers': dict(response.headers),
                    'size': len(response.body),
                }
            )
            
            # Update stats
            self.urls_processed += 1
            self.crawler.stats.inc_value('urls_processed')
            
            yield item.dict()
            
            # Check limits
            if self.urls_processed >= self.max_urls:
                raise CloseSpider('Max URLs limit reached')
            if depth >= self.max_depth:
                self.logger.debug(f"Max depth reached for {url}")
                return

        except Exception as e:
            self.logger.error(f"Error parsing {response.url}: {str(e)}")
            self.crawler.stats.inc_value('urls_failed')

    def handle_error(self, failure):
        """Handle request failures"""
        self.logger.error(f"Request failed: {failure.request.url}")
        self.crawler.stats.inc_value('urls_failed')
        
        # Create error item
        url = failure.request.url
        normalized_url = self.url_normalizer.normalize(url)
        url_hash = hashlib.sha256(normalized_url.encode()).hexdigest()
        
        item = URLDocument(
            url=url,
            url_hash=url_hash,
            normalized_url=normalized_url,
            domain=urlparse(url).netloc,
            source_url=failure.request.headers.get('Referer', b'').decode(),
            depth=failure.request.meta.get('depth', 0),
            status_code=failure.value.response.status if hasattr(failure.value, 'response') else None,
            error_count=1,
            is_active=False,
            metadata={
                'error': str(failure.value),
                'error_type': failure.type.__name__
            }
        )
        
        yield item.dict()

    def closed(self, reason):
        """Handle spider closure"""
        end_time = datetime.utcnow()
        self.crawler.stats.set_value('end_time', end_time)
        self.logger.info(f"Spider closed: {reason}")
        self.logger.info(f"Processed {self.urls_processed} URLs")
