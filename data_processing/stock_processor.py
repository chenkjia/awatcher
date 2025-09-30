"""
股票数据处理模块，负责处理和转换股票数据
"""
from datetime import datetime, timedelta
from utils.logger import logger
from data_fetch import BaostockClient
from db_operations.stock_model import StockModel
from db_operations.mongo_client import MongoClient

class StockProcessor:
    """股票数据处理类，提供数据处理和转换功能"""
    
    @staticmethod
    def process_stock_list():
        """处理股票列表数据并保存到数据库"""
        try:
            # 获取股票列表
            baostock_client = BaostockClient()
            stock_list = baostock_client.get_stock_list()
            
            # 保存到数据库
            for stock in stock_list:
                StockModel.save_stock(stock)
            
            logger.info(f"成功处理并保存 {len(stock_list)} 只股票的基本信息")
            return len(stock_list)
        except Exception as e:
            logger.error(f"处理股票列表数据失败: {e}")
            raise
    
    @staticmethod
    def process_daily_data(code, start_date=None, end_date=None):
        
        """处理股票日线数据并保存到数据库"""
        try:
            # 检查股票是否存在
            stock = StockModel.get_stock_by_code(code)
            if not stock:
                logger.warning(f"股票 {code} 不存在，无法保存日线数据")
                return 0
                
            # 如果数据库中已有日线数据且未指定开始日期，则从最后一条日线数据的日期开始获取
            if not start_date and stock.get('dayLine') and len(stock['dayLine']) > 0:
                # 获取最后一条日线数据的日期
                last_date = stock['dayLine'][-1]['time']
                start_date = (last_date).strftime('%Y-%m-%d')
                logger.info(f"从最后一条日线数据日期 {last_date.strftime('%Y-%m-%d')} 后开始获取新数据")
            
            # 如果未指定日期，默认获取最近一年的数据
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            # 如果开始日期大于结束日期，则此股票已是最新数据
            if start_date and end_date and start_date > end_date:
                logger.info(f"股票 {code} 的日线数据已是最新，无需更新")
                return 0

            # 获取股票日线数据
            baostock_client = BaostockClient()
            daily_data = baostock_client.get_daily_k_data(code, start_date, end_date)
            
            # 检查股票是否存在
            stock = StockModel.get_stock_by_code(code)
            if not stock:
                logger.warning(f"股票 {code} 不存在，无法保存日线数据")
                return 0
            
            # 保存到数据库
            if start_date:
                # 如果指定了开始日期，逐条更新数据
                for data in daily_data:
                    StockModel.update_day_line(code, data)
            else:
                # 如果没有指定开始日期，批量插入数据
                if daily_data:
                    mongo_client = MongoClient()
                    mongo_client.update_one(
                        StockModel.COLLECTION_NAME,
                        {'code': code},
                        {'$set': {'dayLine': daily_data}}
                    )
                    logger.info(f"批量更新股票 {code} 的日线数据，共 {len(daily_data)} 条记录")
            
            logger.info(f"成功处理并保存股票 {code} 的 {len(daily_data)} 条日线数据")
            return len(daily_data)
        except Exception as e:
            logger.error(f"处理股票 {code} 日线数据失败: {e}")
            raise
    
    @staticmethod
    def process_hourly_data(code, start_date=None, end_date=None):
        """处理股票小时线数据并保存到数据库"""
        try:
            # 检查股票是否存在
            stock = StockModel.get_stock_by_code(code)
            if not stock:
                logger.warning(f"股票 {code} 不存在，无法保存小时数据")
                return 0
                
            # 如果数据库中已有小时线数据且未指定开始日期，则从最后一条小时线数据的日期开始获取
            if not start_date and stock.get('hourLine') and len(stock['hourLine']) > 0:
                # 获取最后一条小时线数据的日期
                last_date = stock['hourLine'][-1]['time']
                start_date = (last_date).strftime('%Y-%m-%d')
                logger.info(f"从最后一条小时线数据日期 {last_date.strftime('%Y-%m-%d')} 后开始获取新数据")
            
            # 如果未指定日期，默认获取最近一年的数据
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            # 如果开始日期大于结束日期，则此股票已是最新数据
            if start_date and end_date and start_date > end_date:
                logger.info(f"股票 {code} 的小时线数据已是最新，无需更新")
                return 0

            # 获取股票小时线数据
            baostock_client = BaostockClient()
            hourly_data = baostock_client.get_hourly_k_data(code, start_date, end_date)
            
            # 检查股票是否存在
            stock = StockModel.get_stock_by_code(code)
            if not stock:
                logger.warning(f"股票 {code} 不存在，无法保存小时线数据")
                return 0
            if start_date:
                # 如果指定了开始日期，逐条更新数据
                for data in hourly_data:
                    StockModel.update_hour_line(code, data)
            else:
                # 如果没有指定开始日期，批量插入数据
                if hourly_data:
                    mongo_client = MongoClient()
                    mongo_client.update_one(
                        StockModel.COLLECTION_NAME,
                        {'code': code},
                        {'$set': {'hourLine': hourly_data}}
                    )
                    logger.info(f"批量更新股票 {code} 的小时线数据，共 {len(hourly_data)} 条记录")
            
            logger.info(f"成功处理并保存股票 {code} 的 {len(hourly_data)} 条小时线数据")
            return len(hourly_data)
        except Exception as e:
            logger.error(f"处理股票 {code} 小时线数据失败: {e}")
            raise
    
    @staticmethod
    def process_adjust_factor(code, start_date=None, end_date=None):
        """处理股票复权因子数据并保存到数据库"""
        try:
            # 获取股票复权因子数据
            baostock_client = BaostockClient()
            adjust_factor_data = baostock_client.get_adjust_factor(code, start_date, end_date)
            
            # 检查股票是否存在
            stock = StockModel.get_stock_by_code(code)
            if not stock:
                logger.warning(f"股票 {code} 不存在，无法保存复权因子数据")
                return 0
            
            # 保存到数据库
            for data in adjust_factor_data:
                StockModel.update_adjust_factor(code, data)
            
            logger.info(f"成功处理并保存股票 {code} 的 {len(adjust_factor_data)} 条复权因子数据")
            return len(adjust_factor_data)
        except Exception as e:
            logger.error(f"处理股票 {code} 复权因子数据失败: {e}")
            raise