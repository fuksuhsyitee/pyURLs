from typing import Dict, List, Optional
from scrapy import signals
from scrapy.http import Request
from scrapy.spiders import Spider
import random

class ScrapyUserAgentMiddleware:
    """Scrapy middleware for handling User-Agent headers."""
    
    COMMON_BROWSERS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0"
    ]

    @classmethod
    def from_crawler(cls, crawler):
        """Scrapy's method to create middleware from crawler."""
        middleware = cls(crawler.settings.get('USER_AGENT_CONFIG'))
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware

    def __init__(self, config: Dict):
        """
        Initialize the User-Agent middleware.
        
        Args:
            config: Dictionary containing user agent configuration
        """
        self.mode = config.get('mode', 'single')
        self.custom_agent = config.get('custom_agent')
        self.rotate_list = config.get('rotate_list', [])
        self.use_common_browsers = config.get('use_common_browsers', True)
        
        # Build the final UA list based on configuration
        self.user_agents = self._build_user_agent_list()
        self.current_index = 0

    def _build_user_agent_list(self) -> List[str]:
        """Build the list of user agents based on configuration."""
        if self.mode == 'single':
            return [self.custom_agent]
            
        ua_list = []
        if self.rotate_list:
            ua_list.extend(self.rotate_list)
        
        if self.use_common_browsers:
            ua_list.extend(self.COMMON_BROWSERS)
            
        return ua_list if ua_list else [self.custom_agent]

    def process_request(self, request: Request, spider: Spider) -> None:
        """Process the request and set the User-Agent header."""
        if self.mode == 'random':
            user_agent = random.choice(self.user_agents)
        elif self.mode == 'rotate':
            user_agent = self.user_agents[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.user_agents)
        else:  # single mode
            user_agent = self.user_agents[0]

        request.headers['User-Agent'] = user_agent

    def spider_opened(self, spider: Spider) -> None:
        """Called when spider is opened."""
        spider.logger.info(f'UserAgent Middleware: Using mode "{self.mode}" with {len(self.user_agents)} user agents')
