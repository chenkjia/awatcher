"""
MongoDB客户端模块，提供数据库连接和操作功能
"""
import time
from pymongo import MongoClient as PyMongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from utils.logger import logger
from config import config

class MongoClient:
    """MongoDB客户端类，提供数据库连接和CRUD操作"""
    _instance = None
    _client = None
    _db = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoClient, cls).__new__(cls)
            cls._connect()
        return cls._instance
    
    @classmethod
    def _connect(cls):
        """连接MongoDB数据库"""
        mongo_config = config.get_mongodb_config()
        host = mongo_config.get('host', 'localhost')
        port = mongo_config.get('port', 27017)
        db_name = mongo_config.get('db_name', 'alib')
        username = mongo_config.get('username', '')
        password = mongo_config.get('password', '')
        max_pool_size = mongo_config.get('connection_pool_size', 10)
        max_retry = mongo_config.get('max_retry_attempts', 3)
        retry_delay = mongo_config.get('retry_delay_seconds', 5)
        
        retry_count = 0
        while retry_count < max_retry:
            try:
                # 构建连接URI
                if username and password:
                    uri = f"mongodb://{username}:{password}@{host}:{port}/{db_name}"
                else:
                    uri = f"mongodb://{host}:{port}/{db_name}"
                
                # 创建客户端连接
                cls._client = PyMongoClient(
                    uri,
                    maxPoolSize=max_pool_size,
                    serverSelectionTimeoutMS=5000
                )
                
                # 测试连接
                cls._client.admin.command('ping')
                cls._db = cls._client[db_name]
                logger.info(f"MongoDB连接成功: {host}:{port}/{db_name}")
                return
            
            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                retry_count += 1
                logger.error(f"MongoDB连接失败 (尝试 {retry_count}/{max_retry}): {e}")
                if retry_count < max_retry:
                    logger.info(f"等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
                else:
                    logger.error("MongoDB连接重试次数已达上限，无法连接数据库")
                    raise ConnectionError(f"无法连接到MongoDB: {e}")
    
    @classmethod
    def get_collection(cls, collection_name):
        """获取指定的集合"""
        if cls._db is None:
            cls._connect()
        return cls._db[collection_name]
    
    @classmethod
    def insert_one(cls, collection_name, document):
        """插入单个文档"""
        collection = cls.get_collection(collection_name)
        result = collection.insert_one(document)
        return result.inserted_id
    
    @classmethod
    def insert_many(cls, collection_name, documents):
        """插入多个文档"""
        collection = cls.get_collection(collection_name)
        result = collection.insert_many(documents)
        return result.inserted_ids
    
    @classmethod
    def find_one(cls, collection_name, query=None):
        """查找单个文档"""
        collection = cls.get_collection(collection_name)
        return collection.find_one(query or {})
    
    @classmethod
    def find(cls, collection_name, query=None, projection=None, sort=None, limit=0, skip=0):
        """查找多个文档"""
        collection = cls.get_collection(collection_name)
        cursor = collection.find(query or {}, projection)
        
        if sort:
            cursor = cursor.sort(sort)
        if skip:
            cursor = cursor.skip(skip)
        if limit:
            cursor = cursor.limit(limit)
            
        return list(cursor)
    
    @classmethod
    def update_one(cls, collection_name, query, update, upsert=False):
        """更新单个文档"""
        collection = cls.get_collection(collection_name)
        result = collection.update_one(query, update, upsert=upsert)
        return result.modified_count
    
    @classmethod
    def update_many(cls, collection_name, query, update, upsert=False):
        """更新多个文档"""
        collection = cls.get_collection(collection_name)
        result = collection.update_many(query, update, upsert=upsert)
        return result.modified_count
    
    @classmethod
    def delete_one(cls, collection_name, query):
        """删除单个文档"""
        collection = cls.get_collection(collection_name)
        result = collection.delete_one(query)
        return result.deleted_count
    
    @classmethod
    def delete_many(cls, collection_name, query):
        """删除多个文档"""
        collection = cls.get_collection(collection_name)
        result = collection.delete_many(query)
        return result.deleted_count
    
    @classmethod
    def count_documents(cls, collection_name, query=None):
        """计算文档数量"""
        collection = cls.get_collection(collection_name)
        return collection.count_documents(query or {})
    
    @classmethod
    def create_index(cls, collection_name, keys, **kwargs):
        """创建索引"""
        collection = cls.get_collection(collection_name)
        return collection.create_index(keys, **kwargs)
    
    @classmethod
    def close(cls):
        """关闭数据库连接"""
        if cls._client:
            cls._client.close()
            cls._client = None
            cls._db = None
            logger.info("MongoDB连接已关闭")