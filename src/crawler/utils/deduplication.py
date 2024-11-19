from typing import Set, Dict, List, Optional
import hashlib
from urllib.parse import urlparse, parse_qs, urlencode
import logging
from dataclasses import dataclass
from datetime import datetime
import mmh3  # MurmurHash3 for efficient hashing
from src.crawler.utils.normalize import URLNormalizer

@dataclass
class DeduplicationResult:
    is_duplicate: bool
    hash_value: str
    normalized_url: str
    reason: Optional[str] = None

class URLDeduplicator:
    """Efficient URL deduplication system with multiple deduplication strategies"""
    
    def __init__(
        self,
        max_cache_size: int = 1_000_000,
        use_bloom_filter: bool = True,
        bloom_filter_size: int = 10_000_000,
        bloom_filter_fp_rate: float = 0.01
    ):
        self.logger = logging.getLogger(__name__)
        self.url_normalizer = URLNormalizer()
        self.seen_urls: Set[str] = set()
        self.url_frequency: Dict[str, int] = {}
        self.max_cache_size = max_cache_size
        self.stats = {
            'total_urls': 0,
            'duplicates': 0,
            'unique_urls': 0,
            'cache_evictions': 0
        }
        
        # Bloom filter initialization
        self.use_bloom_filter = use_bloom_filter
        if use_bloom_filter:
            self._initialize_bloom_filter(bloom_filter_size, bloom_filter_fp_rate)
    
    def _initialize_bloom_filter(self, size: int, fp_rate: float) -> None:
        """Initialize bloom filter with given size and false positive rate"""
        try:
            from pybloom_live import ScalableBloomFilter
            self.bloom_filter = ScalableBloomFilter(
                initial_capacity=size,
                error_rate=fp_rate,
                mode=ScalableBloomFilter.SMALL_SET_GROWTH
            )
        except ImportError:
            self.logger.warning("pybloom_live not installed. Falling back to regular set.")
            self.use_bloom_filter = False
    
    def _compute_url_hash(self, url: str) -> str:
        """Compute efficient hash of URL using MurmurHash3"""
        return hex(mmh3.hash128(url))[2:]
    
    def _should_evict_cache(self) -> bool:
        """Check if cache eviction is needed"""
        return len(self.seen_urls) >= self.max_cache_size
    
    def _evict_cache(self) -> None:
        """Evict least frequently used URLs from cache"""
        if not self._should_evict_cache():
            return
            
        # Sort URLs by frequency and keep top 75%
        sorted_urls = sorted(
            self.url_frequency.items(),
            key=lambda x: x[1],
            reverse=True
        )
        urls_to_keep = set(url for url, _ in sorted_urls[:int(self.max_cache_size * 0.75)])
        
        # Update cache
        self.seen_urls = urls_to_keep
        self.url_frequency = {
            url: freq for url, freq in self.url_frequency.items()
            if url in urls_to_keep
        }
        
        self.stats['cache_evictions'] += 1
        self.logger.info(f"Cache evicted. New size: {len(self.seen_urls)}")
    
    def _normalize_query_parameters(self, url: str) -> str:
        """Normalize query parameters by sorting and removing unnecessary ones"""
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query, keep_blank_values=True)
        
        # Remove common tracking parameters
        tracking_params = {'utm_source', 'utm_medium', 'utm_campaign', 'fbclid', 'ref'}
        for param in tracking_params:
            query_params.pop(param, None)
        
        # Sort and encode parameters
        if query_params:
            normalized_query = urlencode(sorted(query_params.items()), doseq=True)
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{normalized_query}"
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    
    def is_duplicate(self, url: str) -> DeduplicationResult:
        """
        Check if URL is duplicate using multiple deduplication strategies
        Returns DeduplicationResult with status and details
        """
        try:
            self.stats['total_urls'] += 1
            
            # Normalize URL
            normalized_url = self.url_normalizer.normalize(url)
            if not normalized_url:
                return DeduplicationResult(
                    is_duplicate=True,
                    hash_value="",
                    normalized_url=url,
                    reason="Invalid URL format"
                )
            
            # Normalize query parameters
            normalized_url = self._normalize_query_parameters(normalized_url)
            
            # Compute hash
            url_hash = self._compute_url_hash(normalized_url)
            
            # Check bloom filter first (fast negative checks)
            if self.use_bloom_filter:
                if normalized_url not in self.bloom_filter:
                    self.bloom_filter.add(normalized_url)
                    self.seen_urls.add(url_hash)
                    self.url_frequency[url_hash] = 1
                    self.stats['unique_urls'] += 1
                    return DeduplicationResult(
                        is_duplicate=False,
                        hash_value=url_hash,
                        normalized_url=normalized_url
                    )
            
            # Check exact duplicates
            if url_hash in self.seen_urls:
                self.url_frequency[url_hash] = self.url_frequency.get(url_hash, 0) + 1
                self.stats['duplicates'] += 1
                return DeduplicationResult(
                    is_duplicate=True,
                    hash_value=url_hash,
                    normalized_url=normalized_url,
                    reason="Exact duplicate"
                )
            
            # New unique URL
            self.seen_urls.add(url_hash)
            self.url_frequency[url_hash] = 1
            self.stats['unique_urls'] += 1
            
            # Cache eviction if needed
            self._evict_cache()
            
            return DeduplicationResult(
                is_duplicate=False,
                hash_value=url_hash,
                normalized_url=normalized_url
            )
            
        except Exception as e:
            self.logger.error(f"Error in deduplication: {str(e)}")
            return DeduplicationResult(
                is_duplicate=True,
                hash_value="",
                normalized_url=url,
                reason=f"Error: {str(e)}"
            )
    
    def get_stats(self) -> Dict[str, any]:
        """Get current deduplication statistics"""
        return {
            **self.stats,
            'cache_size': len(self.seen_urls),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def clear_cache(self) -> None:
        """Clear all cached URLs"""
        self.seen_urls.clear()
        self.url_frequency.clear()
        if self.use_bloom_filter:
            self._initialize_bloom_filter(
                size=10_000_000,
                fp_rate=0.01
            )
        self.stats['cache_evictions'] += 1
        self.logger.info("Cache cleared")

    def add_batch(self, urls: List[str]) -> List[DeduplicationResult]:
        """Process a batch of URLs efficiently"""
        return [self.is_duplicate(url) for url in urls]
