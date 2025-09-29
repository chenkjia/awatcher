#!/usr/bin/env python3
"""
A股数据获取与存储工具主程序
"""
import argparse
import sys
from datetime import datetime, timedelta

from utils.logger import logger
from data_processing import StockProcessor
from data_fetch import BaostockClient
from db_operations.stock_model import StockModel

def setup_indexes():
    """设置数据库索引"""
    try:
        StockModel.setup_indexes()
        logger.info("数据库索引设置成功")
    except Exception as e:
        logger.error(f"设置数据库索引失败: {e}")
        sys.exit(1)

def update_stock_list():
    """更新股票列表"""
    try:
        count = StockProcessor.process_stock_list()
        logger.info(f"股票列表更新完成，共处理 {count} 只股票")
    except Exception as e:
        logger.error(f"更新股票列表失败: {e}")
        sys.exit(1)

def update_daily_data(code=None, start_date=None, end_date=None):
    """更新日线数据"""
    try:
        if code:
            # 更新单只股票
            count = StockProcessor.process_daily_data(code, start_date, end_date)
            logger.info(f"股票 {code} 日线数据更新完成，共处理 {count} 条记录")
        else:
            # 更新所有股票
            stocks = StockModel.get_all_stocks(projection={'code': 1})
            total_count = 0
            for stock in stocks:
                count = StockProcessor.process_daily_data(stock['code'], start_date, end_date)
                total_count += count
            logger.info(f"所有股票日线数据更新完成，共处理 {total_count} 条记录")
    except Exception as e:
        logger.error(f"更新日线数据失败: {e}")
        sys.exit(1)

def update_hourly_data(code=None, start_date=None, end_date=None):
    """更新小时线数据"""
    try:
        if code:
            # 更新单只股票
            count = StockProcessor.process_hourly_data(code, start_date, end_date)
            logger.info(f"股票 {code} 小时线数据更新完成，共处理 {count} 条记录")
        else:
            # 更新所有股票
            stocks = StockModel.get_all_stocks(projection={'code': 1})
            total_count = 0
            for stock in stocks:
                count = StockProcessor.process_hourly_data(stock['code'], start_date, end_date)
                total_count += count
            logger.info(f"所有股票小时线数据更新完成，共处理 {total_count} 条记录")
    except Exception as e:
        logger.error(f"更新小时线数据失败: {e}")
        sys.exit(1)

def update_adjust_factor(code=None, start_date=None, end_date=None):
    """更新复权因子数据"""
    try:
        if code:
            # 更新单只股票
            count = StockProcessor.process_adjust_factor(code, start_date, end_date)
            logger.info(f"股票 {code} 复权因子数据更新完成，共处理 {count} 条记录")
        else:
            # 更新所有股票
            stocks = StockModel.get_all_stocks(projection={'code': 1})
            total_count = 0
            for stock in stocks:
                count = StockProcessor.process_adjust_factor(stock['code'], start_date, end_date)
                total_count += count
            logger.info(f"所有股票复权因子数据更新完成，共处理 {total_count} 条记录")
    except Exception as e:
        logger.error(f"更新复权因子数据失败: {e}")
        sys.exit(1)

def cleanup():
    """清理资源"""
    try:
        BaostockClient().logout()
        logger.info("资源清理完成")
    except Exception as e:
        logger.error(f"资源清理失败: {e}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='A股数据获取与存储工具')
    
    # 添加子命令
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 股票列表更新命令
    stock_list_parser = subparsers.add_parser('update-stock-list', help='更新股票列表')
    
    # 日线数据更新命令
    daily_parser = subparsers.add_parser('update-daily', help='更新日线数据')
    daily_parser.add_argument('--code', help='股票代码，如不指定则更新所有股票')
    daily_parser.add_argument('--start-date', help='开始日期，格式：YYYY-MM-DD')
    daily_parser.add_argument('--end-date', help='结束日期，格式：YYYY-MM-DD')
    
    # 小时线数据更新命令
    hourly_parser = subparsers.add_parser('update-hourly', help='更新小时线数据')
    hourly_parser.add_argument('--code', help='股票代码，如不指定则更新所有股票')
    hourly_parser.add_argument('--start-date', help='开始日期，格式：YYYY-MM-DD')
    hourly_parser.add_argument('--end-date', help='结束日期，格式：YYYY-MM-DD')
    
    # 复权因子数据更新命令
    adjust_parser = subparsers.add_parser('update-adjust-factor', help='更新复权因子数据')
    adjust_parser.add_argument('--code', help='股票代码，如不指定则更新所有股票')
    adjust_parser.add_argument('--start-date', help='开始日期，格式：YYYY-MM-DD')
    adjust_parser.add_argument('--end-date', help='结束日期，格式：YYYY-MM-DD')
    
    # 初始化命令
    init_parser = subparsers.add_parser('init', help='初始化数据库')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    try:
        # 根据命令执行相应操作
        if args.command == 'update-stock-list':
            update_stock_list()
        elif args.command == 'update-daily':
            update_daily_data(args.code, args.start_date, args.end_date)
        elif args.command == 'update-hourly':
            update_hourly_data(args.code, args.start_date, args.end_date)
        elif args.command == 'update-adjust-factor':
            update_adjust_factor(args.code, args.start_date, args.end_date)
        elif args.command == 'init':
            setup_indexes()
            logger.info("数据库初始化完成")
        else:
            parser.print_help()
    finally:
        cleanup()

if __name__ == '__main__':
    main()