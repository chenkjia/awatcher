"""
股票数据模型，定义MongoDB集合结构和操作方法
"""
from datetime import datetime
from .mongo_client import MongoClient
from utils.logger import logger

class StockModel:
    """股票数据模型类，提供股票数据的存储和查询功能"""
    
    COLLECTION_NAME = 'stocks'
    
    @classmethod
    def setup_indexes(cls):
        """设置集合索引"""
        mongo_client = MongoClient()
        
        # 创建索引
        indexes = [
            ('code', 1),
            ('name', 1),
            ('market', 1),
            ('isFocused', 1),
            ('isHourFocused', 1),
            ('focusedDays', 1),
            ('hourFocusedDays', 1),
            ('isStar', 1),
            ('dayLine.time', 1),
            ('hourLine.time', 1)
        ]
        
        for field, direction in indexes:
            mongo_client.create_index(cls.COLLECTION_NAME, [(field, direction)])
            logger.info(f"为 {cls.COLLECTION_NAME} 集合创建索引: {field}")
    
    @classmethod
    def save_stock(cls, stock_data):
        """保存或更新股票基本信息"""
        mongo_client = MongoClient()
        
        # 检查股票是否已存在
        existing_stock = mongo_client.find_one(cls.COLLECTION_NAME, {'code': stock_data['code']})
        
        if existing_stock:
            # 更新现有股票信息
            update_data = {'$set': {}}
            
            # 只更新基本字段，不更新数组字段
            for key, value in stock_data.items():
                if key not in ['dayLine', 'hourLine', 'adjustFactor']:
                    update_data['$set'][key] = value
            
            mongo_client.update_one(cls.COLLECTION_NAME, {'code': stock_data['code']}, update_data)
            logger.debug(f"更新股票信息: {stock_data['code']} - {stock_data.get('name', '')}")
            return existing_stock['_id']
        else:
            # 创建新股票记录
            if 'dayLine' not in stock_data:
                stock_data['dayLine'] = []
            if 'hourLine' not in stock_data:
                stock_data['hourLine'] = []
            if 'adjustFactor' not in stock_data:
                stock_data['adjustFactor'] = []
            
            inserted_id = mongo_client.insert_one(cls.COLLECTION_NAME, stock_data)
            logger.info(f"新增股票: {stock_data['code']} - {stock_data.get('name', '')}")
            return inserted_id
    
    @classmethod
    def update_day_line(cls, code, day_line_data):
        """更新股票日线数据"""
        mongo_client = MongoClient()
        
        # 检查日线数据是否已存在
        existing_stock = mongo_client.find_one(
            cls.COLLECTION_NAME, 
            {
                'code': code,
                'dayLine.time': day_line_data['time']
            }
        )
        
        if existing_stock:
            # 更新现有日线数据
            mongo_client.update_one(
                cls.COLLECTION_NAME,
                {
                    'code': code,
                    'dayLine.time': day_line_data['time']
                },
                {'$set': {'dayLine.$': day_line_data}}
            )
            logger.debug(f"更新股票日线数据: {code}, 日期: {day_line_data['time']}")
        else:
            # 添加新的日线数据
            mongo_client.update_one(
                cls.COLLECTION_NAME,
                {'code': code},
                {'$push': {'dayLine': day_line_data}}
            )
            logger.debug(f"添加股票日线数据: {code}, 日期: {day_line_data['time']}")
    
    @classmethod
    def update_hour_line(cls, code, hour_line_data):
        """更新股票小时线数据"""
        mongo_client = MongoClient()
        
        # 检查小时线数据是否已存在
        existing_stock = mongo_client.find_one(
            cls.COLLECTION_NAME, 
            {
                'code': code,
                'hourLine.time': hour_line_data['time']
            }
        )
        
        if existing_stock:
            # 更新现有小时线数据
            mongo_client.update_one(
                cls.COLLECTION_NAME,
                {
                    'code': code,
                    'hourLine.time': hour_line_data['time']
                },
                {'$set': {'hourLine.$': hour_line_data}}
            )
            logger.debug(f"更新股票小时线数据: {code}, 时间: {hour_line_data['time']}")
        else:
            # 添加新的小时线数据
            mongo_client.update_one(
                cls.COLLECTION_NAME,
                {'code': code},
                {'$push': {'hourLine': hour_line_data}}
            )
            logger.debug(f"添加股票小时线数据: {code}, 时间: {hour_line_data['time']}")
    
    @classmethod
    def update_adjust_factor(cls, code, adjust_factor_data):
        """更新股票复权因子数据"""
        mongo_client = MongoClient()
        
        # 检查复权因子数据是否已存在
        existing_stock = mongo_client.find_one(
            cls.COLLECTION_NAME, 
            {
                'code': code,
                'adjustFactor.time': adjust_factor_data['time']
            }
        )
        
        if existing_stock:
            # 更新现有复权因子数据
            mongo_client.update_one(
                cls.COLLECTION_NAME,
                {
                    'code': code,
                    'adjustFactor.time': adjust_factor_data['time']
                },
                {'$set': {'adjustFactor.$': adjust_factor_data}}
            )
            logger.debug(f"更新股票复权因子: {code}, 日期: {adjust_factor_data['time']}")
        else:
            # 添加新的复权因子数据
            mongo_client.update_one(
                cls.COLLECTION_NAME,
                {'code': code},
                {'$push': {'adjustFactor': adjust_factor_data}}
            )
            logger.debug(f"添加股票复权因子: {code}, 日期: {adjust_factor_data['time']}")
    
    @classmethod
    def get_stock_by_code(cls, code):
        """根据股票代码获取股票信息"""
        mongo_client = MongoClient()
        return mongo_client.find_one(cls.COLLECTION_NAME, {'code': code})
    
    @classmethod
    def get_all_stocks(cls, query=None, projection=None):
        """获取所有股票列表"""
        mongo_client = MongoClient()
        return mongo_client.find(cls.COLLECTION_NAME, query, projection)
    
    @classmethod
    def get_latest_trading_date(cls):
        """获取最新交易日期"""
        mongo_client = MongoClient()
        # 按日期降序排序，获取第一条记录的日期
        result = mongo_client.find(
            cls.COLLECTION_NAME,
            projection={'dayLine.time': 1},
            sort=[('dayLine.time', -1)],
            limit=1
        )
        
        if result and result[0].get('dayLine') and len(result[0]['dayLine']) > 0:
            return result[0]['dayLine'][0]['time']
        return None