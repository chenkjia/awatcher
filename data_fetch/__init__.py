"""
数据获取模块，负责与BaoStock API交互获取数据
"""
from .baostock_client import BaostockClient

# 导出BaostockClient类
__all__ = ['BaostockClient']