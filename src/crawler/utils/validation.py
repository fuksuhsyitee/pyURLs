from typing import List
from urllib.parse import urlparse
import re
import tldextract
import logging
from dataclasses import dataclass

@dataclass
class URLValidationResult:
    is_valid: bool
    reason: str = ""

class URLValidator:
    """URL validation utility class"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Common file extensions to skip
        self.skip_extensions = {
            '.pdf', '.doc', '.docx', '.xls', '.xlsx',
            '.zip', '.rar', '.tar', '.gz', '.jpg', 
            '.jpeg', '.png', '.gif', '.mp4', '.mp3'
        }
        
        # Blocked domains (example)
        self.blocked_domains = {
            'facebook.com', 'twitter.com', 'instagram.com',
            'youtube.com', 'linkedin.com'
        }

    def is_valid_url(self, url: str) -> URLValidationResult:
        """
        Validate URL based on multiple criteria
        Returns URLValidationResult with validation status and reason
        """
        try:
            # Basic URL parsing
            parsed = urlparse(url)
            if not all([parsed.scheme, parsed.netloc]):
                return URLValidationResult(False, "Invalid URL format")

            # Check scheme
            if parsed.scheme not in ['http', 'https']:
                return URLValidationResult(False, "Invalid scheme")

            # Extract domain
            ext = tldextract.extract(url)
            domain = f"{ext.domain}.{ext.suffix}"

            # Check blocked domains
            if domain in self.blocked_domains:
                return URLValidationResult(False, "Blocked domain")

            # Check file extensions
            if any(url.lower().endswith(ext) for ext in self.skip_extensions):
                return URLValidationResult(False, "Blocked file extension")

            # Check URL length
            if len(url) > 2000:
                return URLValidationResult(False, "URL too long")

            # Additional checks can be added here

            return URLValidationResult(True)

        except Exception as e:
            self.logger.error(f"URL validation error for {url}: {str(e)}")
            return URLValidationResult(False, f"Validation error: {str(e)}")

    def normalize_url(self, url: str) -> str:
        """Normalize URL for consistent comparison"""
        try:
            parsed = urlparse(url)
            # Remove default ports
            netloc = re.sub(r':80$', '', parsed.netloc)
            netloc = re.sub(r':443$', '', netloc)
            # Reconstruct URL
            return f"{parsed.scheme}://{netloc}{parsed.path}"
        except Exception as e:
            self.logger.error(f"URL normalization error: {str(e)}")
            return url
