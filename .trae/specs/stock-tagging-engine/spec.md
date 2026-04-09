# Stock Tagging Engine Spec

## Why
用户需要基于股票的技术指标（如MACD的DIF和DEA关系）对股票进行筛选和打标签。这要求系统具备一个可扩展的规则引擎，能够读取股票的日线数据，应用不同的规则，并将结果以标签（Tags）的形式保存回数据库，以便后续在UI界面中快速筛选。

## What Changes
- 添加独立的 Python 脚本作为策略运行入口（新建 `scripts/run_tagger.py`）。
- 在 `data_processing` 下新建 `rule_engine.py`，实现可扩展的规则基类 `BaseRule` 和具体的 `MacdRule`。
- **MacdRule**: 读取股票的日线数据，计算 MACD（DIF和DEA），判断最近一日的 DIF 是否在 DEA 之上，如果是，则打上对应的标签（如 `MACD_金叉` 或 `DIF_UP_DEA`）。
- 更新 `db_operations/stock_model.py`，增加更新股票标签 `tags` 的方法。
- 保证拉取股票列表时只拉取 `code` 字段，然后遍历时逐个拉取该股票的 `dayLine` 数据以节省内存。

## Impact
- Affected specs: 股票数据模型（新增 `tags` 数组字段）。
- Affected code:
  - `db_operations/stock_model.py` (新增更新标签的方法)
  - `data_processing/rule_engine.py` (新建规则引擎)
  - `scripts/run_tagger.py` (新建独立执行脚本)

## ADDED Requirements
### Requirement: 股票规则引擎与打标
系统需要提供一个可扩展的框架，允许用户定义不同的选股规则，对股票进行计算并持久化标签。

#### Scenario: 成功运行MACD打标
- **WHEN** 用户运行打标脚本
- **THEN** 系统从数据库拉取所有股票的code，逐个拉取日线数据，计算MACD。如果某股票DIF > DEA，则为其在数据库中添加相应的标签。
