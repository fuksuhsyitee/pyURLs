from typing import Dict, Any
import logging
from datetime import datetime
from pymongo.errors import DuplicateKeyError
from scrapy.exceptions import DropItem
from src.database.connection import DatabaseConnection
from src.database.models import URLDocument
from src.config.settings import CrawlerSettings

class MongoDBPipeline:
    """Pipeline for storing URLs in MongoDB"""

    def __init__(self, settings: CrawlerSettings):
        self.settings = settings
        self.logger = logging.getLogger(__name__)
        self.db_connection = None
        self.collection = None
        self.stats = {
            'processed': 0,
            'dropped': 0,
            'errors': 0
        }

    @classmethod
    def from_crawler(cls, crawler):
        settings = CrawlerSettings()
        return cls(settings)

    def open_spider(self, spider):
        """Initialize database connection when spider opens"""
        try:
            self.db_connection = DatabaseConnection(self.settings)
            self.db_connection.connect()
            self.collection = self.db_connection.get_collection(
                self.settings.MONGODB_COLLECTION
            )
            # Ensure indexes
            self.db_connection.create_indexes(self.settings.MONGODB_COLLECTION)
            self.logger.info("MongoDB pipeline initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize MongoDB pipeline: {e}")
            raise

    def close_spider(self, spider):
        """Clean up database connection when spider closes"""
        if self.db_connection:
            self.logger.info(
                f"Pipeline stats - Processed: {self.stats['processed']}, "
                f"Dropped: {self.stats['dropped']}, "
                f"Errors: {self.stats['errors']}"
            )
            self.db_connection.close()

    def process_item(self, item: Dict[str, Any], spider) -> Dict[str, Any]:
        """Process and store URL data"""
        try:
            # Convert item to URLDocument
            url_doc = URLDocument(
                url=item['url'],
                url_hash=item['url_hash'],
                normalized_url=item['normalized_url'],
                domain=item['domain'],
                source_url=item.get('source_url'),
                depth=item.get('depth', 0),
                keywords=item.get('keywords', []),
                title=item.get('title'),
                description=item.get('description'),
                status_code=item.get('status_code'),
                content_type=item.get('content_type'),
                timestamp=datetime.utcnow(),
                metadata=item.get('metadata', {})
            )

            # Try to insert document
            try:
                self.collection.insert_one(url_doc.dict())
                self.stats['processed'] += 1
                self.logger.debug(f"Stored URL: {item['url']}")
            except DuplicateKeyError:
                self.stats['dropped'] += 1
                raise DropItem(f"Duplicate URL found: {item['url']}")

            return item

        except Exception as e:
            self.stats['errors'] += 1
            self.logger.error(f"Error processing item: {e}")
            raise DropItem(f"Error processing item: {str(e)}")
