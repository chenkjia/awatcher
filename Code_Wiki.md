# Awatcher 项目 Code Wiki

## 1. 项目简介
Awatcher 是一个基于 Python 开发的 A股数据获取与存储工具。项目通过接入 BaoStock 免费开源金融数据接口，实现对 A股股票列表、日线 K线数据、小时线 K线数据以及复权因子等核心数据的自动化抓取、增量更新和本地化存储（MongoDB）。

## 2. 项目整体架构
项目的目录结构按功能模块划分清晰，主要包括以下几个核心部分：
- `config/`：配置模块，负责读取与解析系统配置项（如 MongoDB 和 BaoStock 连接信息、日志设定）。
- `data_fetch/`：数据抓取模块，封装了外部接口通信层。
- `data_processing/`：数据处理与业务逻辑层，负责协调数据抓取和持久化存储之间的业务逻辑（例如增量判定）。
- `db_operations/`：数据库操作模块，封装了 MongoDB 的底层交互与抽象后的数据模型。
- `utils/`：工具模块，提供如统一日志记录等通用基础功能。
- `main.py`：项目的 CLI 命令行入口点。

## 3. 主要模块职责及关键类说明

### 3.1 核心数据获取层 (`data_fetch/`)
**文件**: `baostock_client.py`
**核心类**: `BaostockClient`
- **职责**: 作为 BaoStock API 的客户端代理（使用单例模式），管理登录/登出状态，并封装各类数据查询的接口调用。
- **关键函数**:
  - `_login()`: 内部登录方法，读取配置进行 BaoStock 登录。
  - `get_stock_list()`: 获取所有股票列表（过滤掉指数和基金，只保留纯股票类型 `1`）。
  - `get_daily_k_data(code, start_date, end_date)`: 获取指定股票、时间范围的日K线数据（不复权），包含开、高、低、收、成交量、成交额、换手率。
  - `get_hourly_k_data(code, start_date, end_date)`: 获取指定股票、时间范围的小时K线数据。
  - `get_adjust_factor(code, start_date, end_date)`: 获取指定股票的复权因子数据。

### 3.2 业务逻辑调度层 (`data_processing/`)
**文件**: `stock_processor.py`
**核心类**: `StockProcessor`
- **职责**: 充当业务调度的中枢。连接数据抓取（`BaostockClient`）和数据存储（`StockModel`），并实现了数据**增量更新**的核心逻辑。
- **关键函数**:
  - `process_stock_list()`: 获取全量股票列表并同步到数据库中。
  - `process_daily_data(code, start_date, end_date)`: 处理日线数据。如果数据库已有数据且未指定开始日期，自动从最后一条数据的日期向后进行**增量拉取**；如果指定日期或数据库无数据，则执行全量或区间拉取。
  - `process_hourly_data(...)`: 处理小时线数据，增量更新逻辑同日线。
  - `process_adjust_factor(...)`: 处理复权因子数据。拉取到新数据后，会先清除该股票原有的复权因子记录，再全量批量插入更新。

### 3.3 数据库访问层 (`db_operations/`)
该模块将底层连接和上层模型剥离。

**文件 1**: `mongo_client.py`
**核心类**: `MongoClient`
- **职责**: 封装 `pymongo`，提供 MongoDB 的连接管理（带自动重试机制）及 CRUD 底层操作（单例模式）。
- **关键函数**:
  - `_connect()`: 建立 MongoDB 连接。
  - `insert_one() / update_one() / find() / create_index()` 等: 标准化封装的数据操作接口。

**文件 2**: `stock_model.py`
**核心类**: `StockModel`
- **职责**: 定义了股票数据（默认集合名称为 `stocks`）的结构，及针对性的业务数据操作方法。
- **关键函数**:
  - `setup_indexes()`: 创建数据库索引，提高查询性能（基于 `code`, `time` 等字段）。
  - `save_stock(stock_data)`: 保存/更新股票的基础信息（不会覆盖原有的 K线数组数据）。
  - `update_day_line() / update_hour_line()`: 针对单个股票向对应的数组字段（`dayLine` 或 `hourLine`）推送 (`$push`) 或更新 (`$set`) 新的K线数据。
  - `batch_update_adjust_factor()`: 批量更新复权因子。

### 3.4 命令行入口 (`main.py`)
- **职责**: 解析命令行参数并调用对应的方法。使用 `argparse` 实现了丰富的子命令和参数组合支持。
- **主要命令处理函数**:
  - `setup_indexes()`: 初始化数据库索引。
  - `update_stock_list()`: 触发更新股票列表。
  - `update_daily_data() / update_hourly_data() / update_adjust_factor()`: 更新对应的数据，支持按单只股票(`--code`)、从文件读取列表(`--file`)或全量股票更新。

## 4. 数据结构设计 (MongoDB)
项目在 MongoDB 中的 `alib.stocks` 集合采用**单个文档嵌套数组**的形式，每只股票对应一个 Document。

```javascript
{
  "code": "sh.600000",             // 股票代码
  "name": "浦发银行",              // 股票名称
  "market": "sh",                  // 所属市场
  "isFocused": false,              // 重点关注标记
  "isHourFocused": false,          // 小时线关注标记
  "focusedDays": 0,                // 连续关注天数
  "hourFocusedDays": 0,            // 小时线连续关注天数
  "isStar": false,                 // 是否星标
  
  // 日线数据数组
  "dayLine": [{
    "time": ISODate("2023-01-01T00:00:00Z"),
    "open": 7.1, "high": 7.2, "low": 7.0, "close": 7.15,
    "volume": 100000, "amount": 715000, "turn": 0.5 // 换手率
  }],
  
  // 小时线数据数组
  "hourLine": [{
    "time": ISODate("2023-01-01T10:30:00Z"),
    "open": 7.1, "high": 7.2, "low": 7.0, "close": 7.15,
    "volume": 50000, "amount": 357500
  }],
  
  // 复权因子数据数组
  "adjustFactor": [{
    "time": ISODate("2023-01-01T00:00:00Z"),
    "foreAdjustFactor": 1.0,       // 前复权因子
    "backAdjustFactor": 1.5,       // 后复权因子
    "adjustFactor": 1.5            // 复权因子
  }]
}
```

## 5. 项目依赖与环境
项目使用 Python 开发，核心依赖库（参见 `requirements.txt`）：
- **baostock** (`>=0.8.8`): 提供免费 A股历史行情数据。
- **pymongo** (`>=4.3.3`): 官方 MongoDB 驱动程序。
- **python-dotenv** (`>=1.0.0`): 环境变量管理。
- **loguru** (`>=0.7.0`): 现代化的日志记录库，取代内置 `logging` 以提供更好的开箱即用体验。
- **pytest** (`>=7.3.1`): 单元测试框架。

## 6. 项目运行与使用方式
在项目根目录运行 `main.py` 以执行各项操作，以下为常用的命令列表：

```bash
# 1. 初始化数据库（仅首次运行或重建索引时需要）
python main.py init

# 2. 更新/拉取全量股票列表基础信息
python main.py update-stock-list

# 3. 更新日线数据
python main.py update-daily                    # 更新所有股票的日线数据（自动增量）
python main.py update-daily --code sh.600000   # 仅更新单只股票的日线数据
python main.py update-daily --file list.txt    # 从文本文件中读取股票代码列表进行更新

# 4. 更新小时线数据 (同理支持 --code 和 --file 选项，以及指定日期范围)
python main.py update-hourly
python main.py update-hourly --code sh.600000 --start-date 2023-01-01 --end-date 2023-12-31

# 5. 更新复权因子数据
python main.py update-adjust-factor
python main.py update-adjust-factor --code sh.600000
```

> **提示**：如果使用 `--file` 选项，例如 `list.txt`，该文件中应保证每行包含一个标准的股票代码（例如 `sh.600000`）。
