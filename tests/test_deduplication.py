import pytest
from src.crawler.utils.deduplication import URLDeduplicator

def test_url_normalization():
    deduplicator = URLDeduplicator()
    
    # Test cases
    assert deduplicator.normalize_url('https://www.example.com/') == 'https://example.com'
    assert deduplicator.normalize_url('https://EXAMPLE.com/path/') == 'https://example.com/path'
    assert deduplicator.normalize_url('https://example.com/path?utm_source=test') == 'https://example.com/path'

def test_duplicate_detection():
    deduplicator = URLDeduplicator()
    
    url1 = 'https://example.com/page'
    url2 = 'https://www.example.com/page/'
    
    assert not deduplicator.is_duplicate(url1)
    assert deduplicator.is_duplicate(url2)  # Should be considered duplicate
