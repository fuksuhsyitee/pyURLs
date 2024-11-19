from typing import Optional
import pymongo
from pymongo.database import Database
from pymongo.collection import Collection
import logging
from contextlib import contextmanager
from src.config.settings import CrawlerSettings

class DatabaseConnection:
    """MongoDB connection manager"""

    def __init__(self, settings: CrawlerSettings):
        self.settings = settings
        self.client: Optional[pymongo.MongoClient] = None
        self.db: Optional[Database] = None
        self.logger = logging.getLogger(__name__)

    def connect(self) -> None:
        """Establish connection to MongoDB"""
        try:
            self.client = pymongo.MongoClient(
                self.settings.MONGODB_URI,
                serverSelectionTimeoutMS=5000
            )
            # Test connection
            self.client.server_info()
            self.db = self.client[self.settings.MONGODB_DB]
            self.logger.info("Successfully connected to MongoDB")
        except Exception as e:
            self.logger.error(f"MongoDB connection error: {str(e)}")
            raise

    def get_collection(self, collection_name: str) -> Collection:
        """Get MongoDB collection"""
        if not self.db:
            self.connect()
        return self.db[collection_name]

    def close(self) -> None:
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            self.logger.info("MongoDB connection closed")

    @contextmanager
    def session(self):
        """Context manager for database sessions"""
        if not self.client:
            self.connect()
        session = self.client.start_session()
        try:
            yield session
        finally:
            session.end_session()

    def create_indexes(self, collection_name: str) -> None:
        """Create necessary indexes"""
        collection = self.get_collection(collection_name)
        
        indexes = [
            ("url_hash", pymongo.ASCENDING),
            ("domain", pymongo.ASCENDING),
            ("timestamp", pymongo.ASCENDING),
            ("keywords", pymongo.ASCENDING)
        ]
        
        for index in indexes:
            collection.create_index([index], background=True)
            self.logger.info(f"Created index on {index[0]}")
