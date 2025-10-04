# 初始化数据库（创建索引）
python main.py init

# 更新股票列表
python main.py update-stock-list

# 更新指定股票的日线数据
python main.py update-daily --code sh.600000 --start-date 2023-01-01 --end-date 2023-12-31

# 更新所有股票的日线数据
python main.py update-daily

# 更新指定股票的小时线数据
python main.py update-hourly --code sh.600000 --start-date 2023-01-01 --end-date 2023-12-31

# 更新指定股票的复权因子数据
python main.py update-adjust-factor --code sh.600000