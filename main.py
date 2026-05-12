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

def update_stock_list(limit=None):
    """更新股票列表"""
    try:
        count = StockProcessor.process_stock_list(limit)
        deleted_count = StockProcessor.process_cleanup_unwanted_stocks()
        logger.info(f"股票列表更新完成，共处理 {count} 只股票，并清理 {deleted_count} 条非目标标的")
    except Exception as e:
        logger.error(f"更新股票列表失败: {e}")
        sys.exit(1)

def cleanup_stock_pool():
    """清理科创板、指数、基金等非目标标的"""
    try:
        deleted_count = StockProcessor.process_cleanup_unwanted_stocks()
        logger.info(f"清理完成，共删除 {deleted_count} 条记录")
    except Exception as e:
        logger.error(f"清理股票池失败: {e}")

def clear_stocks():
    """清空所有股票记录"""
    try:
        count = StockModel.clear_all_stocks()
        logger.info(f"已清空 {count} 条股票记录")
    except Exception as e:
        logger.error(f"清空股票记录失败: {e}")
        sys.exit(1)

def get_codes_from_file(file_path):
    """从文件中读取股票代码列表"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            codes = [line.strip() for line in f if line.strip()]
        logger.info(f"从文件 {file_path} 中读取到 {len(codes)} 个股票代码")
        return codes
    except Exception as e:
        logger.error(f"读取文件 {file_path} 失败: {e}")
        return []

def parse_codes(raw_codes):
    """解析股票代码输入"""
    if not raw_codes:
        return []
    parsed_codes = []
    for raw_code in raw_codes:
        code_items = raw_code.split(',')
        for code in code_items:
            normalized_code = code.strip()
            if normalized_code:
                parsed_codes.append(normalized_code)
    return parsed_codes

def add_stocks(raw_codes=None, file_path=None):
    """批量添加新股票到列表"""
    try:
        if file_path:
            codes = get_codes_from_file(file_path)
        else:
            codes = parse_codes(raw_codes)
        result = StockProcessor.process_add_stocks(codes)
        logger.info(
            f"批量添加股票完成，请求 {result['requested_count']} 只，成功 {result['added_count']} 只"
        )
        if result['missing_codes']:
            logger.warning(f"以下股票代码未找到，跳过: {', '.join(result['missing_codes'])}")
    except Exception as e:
        logger.error(f"批量添加股票失败: {e}")
        sys.exit(1)

def update_daily_data(code=None, file_path=None, start_date=None, end_date=None):
    """更新日线数据"""
    try:
        if code:
            # 更新单只股票
            count = StockProcessor.process_daily_data(code, start_date, end_date)
            logger.info(f"股票 {code} 日线数据更新完成，共处理 {count} 条记录")
        elif file_path:
            codes = get_codes_from_file(file_path)
            total_count = 0
            total_stocks = len(codes)
            for index, stock_code in enumerate(codes, start=1):
                logger.info(f"日线更新进度 [{index}/{total_stocks}] 开始处理 {stock_code}")
                count = StockProcessor.process_daily_data(stock_code, start_date, end_date)
                total_count += count
            logger.info(f"指定列表股票日线数据更新完成，共处理 {total_count} 条记录")
        else:
            stocks = StockModel.get_all_stocks(projection={'code': 1})
            total_count = 0
            total_stocks = len(stocks)
            for index, stock in enumerate(stocks, start=1):
                logger.info(f"日线更新进度 [{index}/{total_stocks}] 开始处理 {stock['code']}")
                count = StockProcessor.process_daily_data(stock['code'], start_date, end_date)
                total_count += count
            logger.info(f"所有股票日线数据更新完成，共处理 {total_count} 条记录")
    except Exception as e:
        logger.error(f"更新日线数据失败: {e}")
        sys.exit(1)

def update_hourly_data(code=None, file_path=None, start_date=None, end_date=None):
    """更新小时线数据"""
    try:
        if code:
            # 更新单只股票
            count = StockProcessor.process_hourly_data(code, start_date, end_date)
            logger.info(f"股票 {code} 小时线数据更新完成，共处理 {count} 条记录")
        elif file_path:
            codes = get_codes_from_file(file_path)
            total_count = 0
            total_stocks = len(codes)
            for index, stock_code in enumerate(codes, start=1):
                logger.info(f"小时线更新进度 [{index}/{total_stocks}] 开始处理 {stock_code}")
                count = StockProcessor.process_hourly_data(stock_code, start_date, end_date)
                total_count += count
            logger.info(f"指定列表股票小时线数据更新完成，共处理 {total_count} 条记录")
        else:
            stocks = StockModel.get_all_stocks(projection={'code': 1})
            total_count = 0
            total_stocks = len(stocks)
            for index, stock in enumerate(stocks, start=1):
                logger.info(f"小时线更新进度 [{index}/{total_stocks}] 开始处理 {stock['code']}")
                count = StockProcessor.process_hourly_data(stock['code'], start_date, end_date)
                total_count += count
            logger.info(f"所有股票小时线数据更新完成，共处理 {total_count} 条记录")
    except Exception as e:
        logger.error(f"更新小时线数据失败: {e}")
        sys.exit(1)

def update_adjust_factor(code=None, file_path=None, start_date=None, end_date=None):
    """更新复权因子数据"""
    try:
        if code:
            # 更新单只股票
            count = StockProcessor.process_adjust_factor(code, start_date, end_date)
            logger.info(f"股票 {code} 复权因子数据更新完成，共处理 {count} 条记录")
        elif file_path:
            # 从文件更新股票列表
            codes = get_codes_from_file(file_path)
            total_count = 0
            total_stocks = len(codes)
            for index, stock_code in enumerate(codes, start=1):
                logger.info(f"复权因子更新进度 [{index}/{total_stocks}] 开始处理 {stock_code}")
                count = StockProcessor.process_adjust_factor(stock_code, start_date, end_date)
                total_count += count
            logger.info(f"指定列表股票复权因子数据更新完成，共处理 {total_count} 条记录")
        else:
            # 更新所有股票
            stocks = StockModel.get_all_stocks(projection={'code': 1})
            total_count = 0
            total_stocks = len(stocks)
            for index, stock in enumerate(stocks, start=1):
                logger.info(f"复权因子更新进度 [{index}/{total_stocks}] 开始处理 {stock['code']}")
                count = StockProcessor.process_adjust_factor(stock['code'], start_date, end_date)
                total_count += count
            logger.info(f"所有股票复权因子数据更新完成，共处理 {total_count} 条记录")
    except Exception as e:
        logger.error(f"更新复权因子数据失败: {e}")
        sys.exit(1)

def cleanup_and_update_daily_adjust(code=None, file_path=None, start_date=None, end_date=None):
    """清理股票池后，串行更新日线和复权因子"""
    try:
        logger.info("开始执行整合任务：清理股票池 -> 更新日线 -> 更新复权因子")
        cleanup_stock_pool()
        update_daily_data(code, file_path, start_date, end_date)
        update_adjust_factor(code, file_path, start_date, end_date)
        logger.info("整合任务执行完成")
    except Exception as e:
        logger.error(f"执行整合任务失败: {e}")
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
    stock_list_parser.add_argument('--limit', type=int, help='限制更新股票数量，例如 10')

    # 股票列表新增命令
    add_stocks_parser = subparsers.add_parser('add-stocks', help='批量添加新股票到列表')
    add_stocks_parser.add_argument('--codes', nargs='+', help='股票代码列表，支持空格或逗号分隔')
    add_stocks_parser.add_argument('--file', help='包含股票代码的文件路径')

    # 清理股票池命令
    subparsers.add_parser('cleanup-stock-pool', help='清理科创板、指数、基金等非目标标的')
    
    # 日线数据更新命令
    daily_parser = subparsers.add_parser('update-daily', help='更新日线数据')
    daily_parser.add_argument('--code', help='股票代码，如不指定则更新所有股票')
    daily_parser.add_argument('--file', help='包含股票代码的文件路径')
    daily_parser.add_argument('--start-date', help='开始日期，格式：YYYY-MM-DD')
    daily_parser.add_argument('--end-date', help='结束日期，格式：YYYY-MM-DD')
    
    # 小时线数据更新命令
    hourly_parser = subparsers.add_parser('update-hourly', help='更新小时线数据')
    hourly_parser.add_argument('--code', help='股票代码，如不指定则更新所有股票')
    hourly_parser.add_argument('--file', help='包含股票代码的文件路径')
    hourly_parser.add_argument('--start-date', help='开始日期，格式：YYYY-MM-DD')
    hourly_parser.add_argument('--end-date', help='结束日期，格式：YYYY-MM-DD')
    
    # 复权因子数据更新命令
    adjust_parser = subparsers.add_parser('update-adjust-factor', help='更新复权因子数据')
    adjust_parser.add_argument('--code', help='股票代码，如不指定则更新所有股票')
    adjust_parser.add_argument('--file', help='包含股票代码的文件路径')
    adjust_parser.add_argument('--start-date', help='开始日期，格式：YYYY-MM-DD')
    adjust_parser.add_argument('--end-date', help='结束日期，格式：YYYY-MM-DD')

    # 整合命令：清理（含ST）+ 更新日线 + 更新复权
    combo_parser = subparsers.add_parser(
        'cleanup-and-update-daily-adjust',
        help='先清理股票池（含ST），再更新日线与复权因子'
    )
    combo_parser.add_argument('--code', help='股票代码，如不指定则更新所有股票')
    combo_parser.add_argument('--file', help='包含股票代码的文件路径')
    combo_parser.add_argument('--start-date', help='开始日期，格式：YYYY-MM-DD')
    combo_parser.add_argument('--end-date', help='结束日期，格式：YYYY-MM-DD')
    
    # 初始化命令
    init_parser = subparsers.add_parser('init', help='初始化数据库')

    # 清空股票列表命令
    clear_parser = subparsers.add_parser('clear-stocks', help='清空所有股票记录')

    # 解析命令行参数
    args = parser.parse_args()
    
    try:
        # 根据命令执行相应操作
        if args.command == 'update-stock-list':
            update_stock_list(args.limit)
        elif args.command == 'add-stocks':
            add_stocks(args.codes, args.file)
        elif args.command == 'cleanup-stock-pool':
            cleanup_stock_pool()
        elif args.command == 'update-daily':
            update_daily_data(args.code, args.file, args.start_date, args.end_date)
        elif args.command == 'update-hourly':
            update_hourly_data(args.code, args.file, args.start_date, args.end_date)
        elif args.command == 'update-adjust-factor':
            update_adjust_factor(args.code, args.file, args.start_date, args.end_date)
        elif args.command == 'cleanup-and-update-daily-adjust':
            cleanup_and_update_daily_adjust(args.code, args.file, args.start_date, args.end_date)
        elif args.command == 'init':
            setup_indexes()
            logger.info("数据库初始化完成")
        elif args.command == 'clear-stocks':
            clear_stocks()
        else:
            parser.print_help()
    finally:
        cleanup()

if __name__ == '__main__':
    main()
