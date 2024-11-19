from typing import Dict, Set, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import re
import logging

class URLNormalizer:
    """URL normalization utility class"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Parameters to remove from URLs
        self.remove_params: Set[str] = {
            'utm_source', 'utm_medium', 'utm_campaign',
            'utm_term', 'utm_content', 'fbclid',
            'gclid', 'ref', 'source'
        }
        
        # Domain variations mapping
        self.domain_variations: Dict[str, str] = {
            'www.': '',  # Remove www
            '.m.': '.',  # Mobile domains
            '-mobile.': '.'  # Alternative mobile domains
        }

    def normalize(self, url: str) -> Optional[str]:
        """
        Normalize URL by applying various transformations
        Returns normalized URL or None if invalid
        """
        try:
            # Parse URL
            parsed = urlparse(url)
            
            # Convert to lowercase
            netloc = parsed.netloc.lower()
            path = parsed.path.lower()
            
            # Remove default ports
            netloc = re.sub(r':80$', '', netloc)
            netloc = re.sub(r':443$', '', netloc)
            
            # Apply domain variations
            for variation, replacement in self.domain_variations.items():
                netloc = netloc.replace(variation, replacement)
            
            # Remove trailing slash from path
            path = path.rstrip('/')
            if not path:
                path = '/'
            
            # Handle query parameters
            query_params = parse_qs(parsed.query)
            filtered_params = {
                k: v for k, v in query_params.items()
                if k not in self.remove_params
            }
            
            # Sort query parameters
            sorted_params = sorted(filtered_params.items())
            new_query = urlencode(sorted_params, doseq=True)
            
            # Reconstruct URL
            normalized = urlunparse((
                parsed.scheme,
                netloc,
                path,
                '',  # params
                new_query,
                ''   # fragment
            ))
            
            return normalized

        except Exception as e:
            self.logger.error(f"URL normalization error for {url}: {str(e)}")
            return None

    def get_domain(self, url: str) -> Optional[str]:
        """Extract and normalize domain from URL"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Remove port if present
            domain = domain.split(':')[0]
            
            # Apply domain variations
            for variation, replacement in self.domain_variations.items():
                domain = domain.replace(variation, replacement)
                
            return domain
            
        except Exception as e:
            self.logger.error(f"Domain extraction error for {url}: {str(e)}")
            return None
