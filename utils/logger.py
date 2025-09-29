"""
日志工具模块，提供统一的日志记录功能
"""
import os
import sys
from pathlib import Path
from loguru import logger

from config import config

def setup_logger():
    """
    配置日志记录器
    """
    log_config = config.get_logging_config()
    log_level = log_config.get('level', 'INFO')
    log_file = log_config.get('file_path', 'logs/awatcher.log')
    rotation = log_config.get('rotation', '10 MB')
    
    # 确保日志目录存在
    log_dir = os.path.dirname(log_file)
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # 清除默认处理器
    logger.remove()
    
    # 添加控制台处理器
    logger.add(sys.stderr, level=log_level)
    
    # 添加文件处理器
    logger.add(
        log_file,
        rotation=rotation,
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message} | {file}:{line}"
    )
    
    return logger

# 导出配置好的日志记录器
logger = setup_logger()