import os
import sys

# 添加项目根目录到 sys.path 以便导入模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_operations.stock_model import StockModel
from data_processing.rule_engine import RuleEngine, MacdRule
from utils.logger import logger

def main():
    logger.info("开始执行股票打标任务...")
    
    # 1. 初始化规则引擎并注册规则
    engine = RuleEngine()
    engine.register_rule(MacdRule())
    logger.info("已注册规则：MacdRule")
    
    # 2. 从数据库获取所有股票的 code 列表
    # 使用 projection 仅查询 code 字段以降低内存占用
    stocks_cursor = StockModel.get_all_stocks(projection={'code': 1})
    codes = [stock['code'] for stock in stocks_cursor]
    logger.info(f"共获取到 {len(codes)} 只股票，开始遍历打标...")
    
    # 3. 遍历 code 列表，逐个获取数据并打标
    success_count = 0
    error_count = 0
    
    for i, code in enumerate(codes):
        try:
            # 获取该股票的完整信息（包含 dayLine）
            stock_data = StockModel.get_stock_by_code(code)
            if not stock_data or 'dayLine' not in stock_data:
                logger.debug(f"[{i+1}/{len(codes)}] 股票 {code} 无日线数据，跳过")
                continue
                
            day_line = stock_data['dayLine']
            
            # 将日线数据传入规则引擎进行评估
            tags = engine.evaluate_all(day_line)
            
            # 调用 StockModel.update_tags 将结果保存回数据库
            StockModel.update_tags(code, tags)
            
            logger.debug(f"[{i+1}/{len(codes)}] 股票 {code} 打标成功: {tags}")
            success_count += 1
            
        except Exception as e:
            logger.error(f"[{i+1}/{len(codes)}] 处理股票 {code} 时出错: {e}")
            error_count += 1
            
    logger.info(f"股票打标任务执行完毕！总计: {len(codes)}, 成功: {success_count}, 失败: {error_count}")

if __name__ == '__main__':
    main()
