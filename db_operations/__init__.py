"""
数据库操作模块，负责与MongoDB数据库交互
"""
from .mongo_client import MongoClient

# 导出MongoClient类
__all__ = ['MongoClient']