"""
BaoStock API客户端模块，提供股票数据获取功能
"""
import baostock as bs
from datetime import datetime, timedelta
import time

from utils.logger import logger
from config import config

class BaostockClient:
    """BaoStock API客户端类，提供股票数据获取功能"""
    
    _instance = None
    _is_logged_in = False
    _preferred_server_host = "public-api.baostock.com"

    @staticmethod
    def _is_st_name(name):
        upper_name = (name or "").upper()
        return (
            upper_name.startswith('ST')
            or upper_name.startswith('*ST')
            or upper_name.startswith('S*ST')
            or upper_name.startswith('SST')
        )

    @classmethod
    def _is_excluded_stock(cls, code, stock_type, status, name):
        # 非股票、非上市、科创板、北交所、ST 均排除
        if stock_type != '1' or status != '1':
            return True
        if code.startswith('sh.688'):
            return True
        if code.startswith('bj.'):
            return True
        if cls._is_st_name(name):
            return True
        return False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(BaostockClient, cls).__new__(cls)
            cls._login()
        return cls._instance
    
    @classmethod
    def _login(cls):
        """登录BaoStock（兼容旧版地址并增加重试）"""
        if cls._is_logged_in:
            return

        cls._patch_server_host_if_needed()

        baostock_config = config.get_baostock_config()
        user = baostock_config.get('login_user', 'anonymous')
        password = baostock_config.get('login_password', '123456')
        max_retry = 3
        retry_delay = 2
        last_error = None

        for retry in range(1, max_retry + 1):
            logger.info(f"正在登录BaoStock... (第 {retry}/{max_retry} 次)")
            try:
                result = bs.login(user_id=user, password=password)
                if result.error_code == '0':
                    cls._is_logged_in = True
                    logger.info("BaoStock登录成功")
                    return
                last_error = result.error_msg
                logger.error(f"BaoStock登录失败: {result.error_msg}")
            except Exception as e:
                last_error = str(e)
                logger.error(f"BaoStock登录异常: {e}")

            # 清理连接状态，避免残留坏连接影响重试
            try:
                bs.logout()
            except Exception:
                pass
            cls._is_logged_in = False

            if retry < max_retry:
                logger.info(f"等待 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)

        raise ConnectionError(f"BaoStock登录失败，重试 {max_retry} 次后仍失败: {last_error}")

    @classmethod
    def _patch_server_host_if_needed(cls):
        """
        兼容旧版 baostock 的默认服务器地址。
        旧版本默认使用 www.baostock.com，当前应使用 public-api.baostock.com。
        """
        try:
            import baostock.common.contants as bs_constants
        except Exception as e:
            logger.warning(f"无法读取baostock服务端配置，继续使用默认配置: {e}")
            return

        current_host = getattr(bs_constants, "BAOSTOCK_SERVER_IP", "")
        if current_host != cls._preferred_server_host:
            logger.warning(
                f"检测到BaoStock默认地址为 {current_host}，自动切换为 {cls._preferred_server_host}"
            )
            setattr(bs_constants, "BAOSTOCK_SERVER_IP", cls._preferred_server_host)
    
    @classmethod
    def logout(cls):
        """登出BaoStock"""
        if cls._is_logged_in:
            bs.logout()
            cls._is_logged_in = False
            logger.info("BaoStock已登出")
    
    @classmethod
    def get_stock_list(cls):
        """获取股票列表"""
        cls._login()
        
        logger.info("正在获取股票列表...")
        rs = bs.query_stock_basic()
        
        if rs.error_code != '0':
            logger.error(f"获取股票列表失败: {rs.error_msg}")
            raise Exception(f"获取股票列表失败: {rs.error_msg}")
        
        stock_list = []
        while (rs.next()):
            data = rs.get_row_data()
            # query_stock_basic 返回字段:
            # code, code_name, ipoDate, outDate, type, status
            # type='1' 为股票；status='1' 为上市状态
            if len(data) > 5:
                code = data[0]
                stock_type = data[4]
                status = data[5]
                name = data[1]
                if cls._is_excluded_stock(code, stock_type, status, name):
                    continue
                market = code.split('.')[0]
                stock = {
                    'code': code,
                    'name': data[1],
                    'market': market,
                    'securityType': stock_type,
                    'isFocused': False,
                    'isHourFocused': False,
                    'focusedDays': 0,
                    'hourFocusedDays': 0,
                    'isStar': False
                }
                stock_list.append(stock)
        
        logger.info(f"成功获取 {len(stock_list)} 只股票的基本信息")
        return stock_list

    @classmethod
    def get_excluded_stock_codes(cls):
        """
        获取当前应被排除的股票代码集合。
        用于清理历史存量中“已变更为 ST 但旧名称未更新”的记录。
        """
        cls._login()
        rs = bs.query_stock_basic()
        if rs.error_code != '0':
            raise Exception(f"获取股票列表失败: {rs.error_msg}")

        excluded_codes = set()
        while rs.next():
            data = rs.get_row_data()
            if len(data) <= 5:
                continue
            code = data[0]
            name = data[1]
            stock_type = data[4]
            status = data[5]
            if cls._is_excluded_stock(code, stock_type, status, name):
                excluded_codes.add(code)
        logger.info(f"识别出 {len(excluded_codes)} 条应排除股票代码")
        return excluded_codes
    
    @classmethod
    def get_daily_k_data(cls, code, start_date=None, end_date=None):
        """获取股票日K线数据"""
        cls._login()
        
        if not start_date:
            start_date = "1990-01-01"
        
        logger.info(f"正在获取股票 {code} 的日K线数据 ({start_date} 至 {end_date})...")
        
        # 查询日K线数据
        rs = bs.query_history_k_data_plus(
            code,
            "date,open,high,low,close,volume,amount,turn,peTTM",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3"  # 不复权
        )
        
        if rs.error_code != '0':
            logger.error(f"获取股票 {code} 日K线数据失败: {rs.error_msg}")
            raise Exception(f"获取股票 {code} 日K线数据失败: {rs.error_msg}")
        
        daily_data = []
        while (rs.next()):
            data = rs.get_row_data()
            k_data = {
                'time': datetime.strptime(data[0], '%Y-%m-%d'),
                'open': float(data[1]) if data[1] else 0,
                'high': float(data[2]) if data[2] else 0,
                'low': float(data[3]) if data[3] else 0,
                'close': float(data[4]) if data[4] else 0,
                'volume': float(data[5]) if data[5] else 0,
                'amount': float(data[6]) if data[6] else 0,
                'turn': float(data[7]) if data[7] else 0,  # 换手率
                'peTTM': float(data[8]) if data[8] and data[8] != "" else 0
            }
            daily_data.append(k_data)
        
        logger.info(f"成功获取股票 {code} 的 {len(daily_data)} 条日K线数据")
        return daily_data
    
    @classmethod
    def get_hourly_k_data(cls, code, start_date=None, end_date=None):
        """获取股票小时K线数据"""
        cls._login()
        
        if not start_date:
            start_date = '1990-01-01'
        
        logger.info(f"正在获取股票 {code} 的小时K线数据 ({start_date} 至 {end_date})...")
        
        # 查询小时K线数据
        rs = bs.query_history_k_data_plus(
            code,
            "date,time,open,high,low,close,volume,amount",
            start_date=start_date,
            end_date=end_date,
            frequency="60",
            adjustflag="3"  # 不复权
        )
        
        if rs.error_code != '0':
            logger.error(f"获取股票 {code} 小时K线数据失败: {rs.error_msg}")
            raise Exception(f"获取股票 {code} 小时K线数据失败: {rs.error_msg}")
        
        hourly_data = []
        while (rs.next()):
            data = rs.get_row_data()
            # 打印原始数据，查看格式
            logger.debug(f"小时线原始数据: {data}")
            try:
                time_obj = datetime.strptime(data[1], '%Y%m%d%H%M%S000')
                k_data = {
                    'time': time_obj,
                    'open': float(data[2]) if data[2] else 0,
                    'high': float(data[3]) if data[3] else 0,
                    'low': float(data[4]) if data[4] else 0,
                    'close': float(data[5]) if data[5] else 0,
                    'volume': float(data[6]) if data[6] else 0,
                    'amount': float(data[7]) if data[7] else 0
                }
                hourly_data.append(k_data)
            except Exception as e:
                logger.error(f"处理小时线数据时出错: {e}, 数据: {data}")
        
        logger.info(f"成功获取股票 {code} 的 {len(hourly_data)} 条小时K线数据")
        return hourly_data
    
    @classmethod
    def get_adjust_factor(cls, code, start_date=None, end_date=None):
        """获取股票复权因子数据"""
        cls._login()
        
        # 如果未指定日期，默认获取最近一年的数据
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = '1990-01-01'
        
        logger.info(f"正在获取股票 {code} 的复权因子数据 ({start_date} 至 {end_date})...")
        
        # 查询复权因子数据
        rs = bs.query_adjust_factor(
            code=code,
            start_date=start_date,
            end_date=end_date
        )
        
        if rs.error_code != '0':
            logger.error(f"获取股票 {code} 复权因子数据失败: {rs.error_msg}")
            raise Exception(f"获取股票 {code} 复权因子数据失败: {rs.error_msg}")
        
        adjust_factor_data = []
        while (rs.next()):
            data = rs.get_row_data()
            factor = {
                'time': datetime.strptime(data[1], '%Y-%m-%d'),
                'foreAdjustFactor': float(data[2]) if data[2] else 1.0,
                'backAdjustFactor': float(data[3]) if data[3] else 1.0,
                'adjustFactor': float(data[4]) if data[4] else 1.0
            }
            adjust_factor_data.append(factor)
        
        logger.info(f"成功获取股票 {code} 的 {len(adjust_factor_data)} 条复权因子数据")
        return adjust_factor_data
