from typing import Dict

class CrawlerSettings:
    @staticmethod
    def get_settings(config: Dict) -> Dict:
        """
        Generate Scrapy settings from configuration.
        
        Args:
            config: Dictionary containing crawler configuration
        
        Returns:
            Dictionary of Scrapy settings
        """
        settings = {
            # Basic Settings
            'BOT_NAME': 'URLCrawler',
            'ROBOTSTXT_OBEY': True,
            'LOG_LEVEL': 'INFO',
            
            # Concurrency Settings
            'CONCURRENT_REQUESTS': config['crawler']['concurrent_requests'],
            'CONCURRENT_REQUESTS_PER_DOMAIN': 16,
            'CONCURRENT_REQUESTS_PER_IP': 16,
            
            # Delay and Timeout Settings
            'DOWNLOAD_DELAY': config['crawler']['download_delay'],
            'DOWNLOAD_TIMEOUT': 30,
            'RANDOMIZE_DOWNLOAD_DELAY': True,
            
            # Retry Settings
            'RETRY_ENABLED': True,
            'RETRY_TIMES': config['crawler']['retry_times'],
            'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
            
            # Cache Settings
            'HTTPCACHE_ENABLED': True,
            'HTTPCACHE_EXPIRATION_SECS': 86400,  # 24 hours
            'HTTPCACHE_DIR': 'httpcache',
            'HTTPCACHE_IGNORE_HTTP_CODES': [401, 403, 404, 429, 500, 502, 503, 504],
            
            # Cookie and Header Settings
            'COOKIES_ENABLED': config['crawler']['cookies_enabled'],
            'DEFAULT_REQUEST_HEADERS': {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en',
            },
            
            # MongoDB Settings
            'MONGODB_URI': config['mongodb']['uri'],
            'MONGODB_DATABASE': config['mongodb']['database'],
            'MONGODB_COLLECTION': config['mongodb']['collection'],
            'MONGODB_UNIQUE_KEY': config['mongodb'].get('unique_key', 'url'),  # Default to 'url' if not specified
            'MONGODB_BUFFER_SIZE': config['mongodb'].get('buffer_size', 100),  # Number of items to buffer before writing
            
            # Middleware Settings
            'DOWNLOADER_MIDDLEWARES': {
                # User Agent Middleware
                'src.crawler.middlewares.user_agent.ScrapyUserAgentMiddleware': 400,
                'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
                
                # Proxy Middleware (if enabled)
                'src.crawler.middlewares.proxy.ProxyMiddleware': 350,
                
                # Retry Middleware
                'scrapy.downloadermiddlewares.retry.RetryMiddleware': 500,
                
                # Other built-in middlewares
                'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
            },
            
            # Spider Middleware Settings
            'SPIDER_MIDDLEWARES': {
                'scrapy.spidermiddlewares.depth.DepthMiddleware': 100,
                'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 200,
                'scrapy.spidermiddlewares.offsite.OffsiteMiddleware': 300,
                'scrapy.spidermiddlewares.referer.RefererMiddleware': 400,
                'scrapy.spidermiddlewares.urllength.UrlLengthMiddleware': 500,
            },
            
            # Pipeline Settings
            'ITEM_PIPELINES': {
                'src.crawler.pipelines.url_filter.URLFilterPipeline': 100,
                'src.crawler.pipelines.data_cleaner.DataCleanerPipeline': 200,
                'src.crawler.pipelines.mongodb.MongoDBPipeline': 300,
            },
            
            # Extension Settings
            'EXTENSIONS': {
                'scrapy.extensions.telnet.TelnetConsole': None,  # Disable telnet console
                'scrapy.extensions.corestats.CoreStats': 100,
                'scrapy.extensions.memusage.MemoryUsage': 200,
                'scrapy.extensions.logstats.LogStats': 300,
            },
            
            # User Agent Configuration
            'USER_AGENT_CONFIG': config['user_agent'],
            
            # Proxy Configuration (if enabled)
            'PROXY_CONFIG': config.get('proxies', {'enabled': False}),
        }
        
        return settings

    @staticmethod
    def get_test_settings() -> Dict:
        """
        Generate settings for testing purposes.
        
        Returns:
            Dictionary of test-specific Scrapy settings
        """
        return {
            'BOT_NAME': 'TestCrawler',
            'ROBOTSTXT_OBEY': False,
            'LOG_LEVEL': 'DEBUG',
            'CONCURRENT_REQUESTS': 1,
            'DOWNLOAD_DELAY': 0,
            'COOKIES_ENABLED': False,
            'HTTPCACHE_ENABLED': False,
            # Test MongoDB settings
            'MONGODB_URI': 'mongodb://localhost:27017',
            'MONGODB_DATABASE': 'test_db',
            'MONGODB_COLLECTION': 'test_urls',
            'MONGODB_UNIQUE_KEY': 'url',
            'MONGODB_BUFFER_SIZE': 10,  # Smaller buffer size for testing
        }
