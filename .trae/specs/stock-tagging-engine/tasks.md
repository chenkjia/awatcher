# Tasks
- [x] Task 1: 更新数据库模型支持标签
  - [x] SubTask 1.1: 在 `StockModel` 中添加 `update_tags` 方法，用于更新股票文档的 `tags` 数组字段。
- [x] Task 2: 搭建可扩展的规则引擎框架
  - [x] SubTask 2.1: 创建 `data_processing/rule_engine.py`，定义 `BaseRule` 接口（包含 `evaluate` 方法，返回标签列表）。
  - [x] SubTask 2.2: 实现 `RuleEngine` 类，用于注册和执行多个规则。
- [x] Task 3: 实现 MACD 规则
  - [x] SubTask 3.1: 引入或实现 MACD 计算逻辑（可使用 pandas/numpy 计算 DIF 和 DEA）。
  - [x] SubTask 3.2: 创建 `MacdRule` 继承自 `BaseRule`，实现 DIF > DEA 的判断逻辑，并返回特定标签。
- [x] Task 4: 编写独立执行脚本
  - [x] SubTask 4.1: 创建 `scripts/run_tagger.py`，实现从数据库仅获取所有股票的 `code`。
  - [x] SubTask 4.2: 遍历 `code` 列表，逐个获取该股票的 `dayLine` 数据。
  - [x] SubTask 4.3: 将日线数据传入规则引擎进行评估，获取标签列表。
  - [x] SubTask 4.4: 调用 `StockModel.update_tags` 将结果保存回数据库。

# Task Dependencies
- [Task 2] depends on [Task 1]
- [Task 3] depends on [Task 2]
- [Task 4] depends on [Task 1, Task 2, Task 3]
