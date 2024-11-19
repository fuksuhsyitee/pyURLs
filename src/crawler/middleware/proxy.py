import random
from typing import List, Optional
import logging
import requests
from scrapy import signals
from scrapy.exceptions import NotConfigured

class RotateProxyMiddleware:
    """Middleware to rotate proxies for each request"""

    def __init__(self, proxy_list: List[str]):
        self.proxies = proxy_list
        self.current_proxy: Optional[str] = None
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.get('PROXY_LIST'):
            raise NotConfigured('PROXY_LIST setting is required')
            
        proxy_list = crawler.settings.get('PROXY_LIST')
        
        middleware = cls(proxy_list)
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware

    def process_request(self, request, spider):
        """Set proxy for request"""
        if self.proxies:
            self.current_proxy = random.choice(self.proxies)
            request.meta['proxy'] = self.current_proxy
            self.logger.debug(f'Using proxy: {self.current_proxy}')

    def process_response(self, request, response, spider):
        """Handle proxy response"""
        if response.status in [403, 429]:  # Forbidden or Too Many Requests
            self.logger.warning(f'Proxy {self.current_proxy} received {response.status}')
            if self.current_proxy in self.proxies:
                self.proxies.remove(self.current_proxy)
            return request
        return response

    def spider_opened(self, spider):
        self.logger.info('Proxy middleware enabled')
