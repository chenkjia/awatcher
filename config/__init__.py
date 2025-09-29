"""
配置模块，负责加载和管理项目配置
"""
import json
import os
from pathlib import Path

class Config:
    """配置管理类，负责加载和提供配置信息"""
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._load_config()
        return cls._instance
    
    @classmethod
    def _load_config(cls):
        """加载配置文件"""
        config_path = Path(os.path.dirname(os.path.abspath(__file__))) / "config.json"
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                cls._config = json.load(f)
        except Exception as e:
            raise Exception(f"加载配置文件失败: {e}")
    
    @classmethod
    def get_mongodb_config(cls):
        """获取MongoDB配置"""
        if not cls._config:
            cls._load_config()
        return cls._config.get('mongodb', {})
    
    @classmethod
    def get_baostock_config(cls):
        """获取BaoStock配置"""
        if not cls._config:
            cls._load_config()
        return cls._config.get('baostock', {})
    
    @classmethod
    def get_data_update_config(cls):
        """获取数据更新配置"""
        if not cls._config:
            cls._load_config()
        return cls._config.get('data_update', {})
    
    @classmethod
    def get_logging_config(cls):
        """获取日志配置"""
        if not cls._config:
            cls._load_config()
        return cls._config.get('logging', {})

# 导出配置实例
config = Config()