# 初始化数据库（创建索引）
python main.py init

# 更新股票列表
python main.py update-stock-list

# 清空所有股票记录
python main.py clear-stocks

# 从文件列表添加股票到数据库
python main.py add-stocks --file list.txt

# 更新指定股票的日线数据
python main.py update-daily --code sh.600000

# 更新所有股票的日线数据
python main.py update-daily

# 从文件列表更新日线数据
python main.py update-daily --file list.txt

# 更新指定股票的小时线数据
python main.py update-hourly --code sh.600000 --start-date 2023-01-01 --end-date 2023-12-31

# 从文件列表更新小时线数据
python main.py update-hourly --file list.txt

# 更新所有股票的小时线数据
python main.py update-hourly

# 更新指定股票的复权因子数据
python main.py update-adjust-factor --code sh.600000

# 从文件列表更新复权因子数据
python main.py update-adjust-factor --file list.txt