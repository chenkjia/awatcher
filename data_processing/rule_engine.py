import pandas as pd
from typing import List, Dict, Any
from abc import ABC, abstractmethod

class BaseRule(ABC):
    """规则基类，所有具体的规则类都应该继承该类"""
    @abstractmethod
    def evaluate(self, day_line: List[Dict[str, Any]]) -> List[str]:
        """
        评估规则，返回标签列表
        :param day_line: 股票的日线数据列表
        :return: 标签列表
        """
        pass

class MacdRule(BaseRule):
    """MACD规则：判断最新的 DIF 是否大于 DEA"""
    def evaluate(self, day_line: List[Dict[str, Any]]) -> List[str]:
        if not day_line or len(day_line) < 35:
            # 数据量不足以计算准确的 MACD
            return []
            
        df = pd.DataFrame(day_line)
        if 'close' not in df.columns:
            return []
            
        # 确保 close 是数值类型
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        
        # 计算 MACD(12, 26, 9)
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        dif = exp1 - exp2
        dea = dif.ewm(span=9, adjust=False).mean()
        
        latest_dif = dif.iloc[-1]
        latest_dea = dea.iloc[-1]
        
        if pd.notna(latest_dif) and pd.notna(latest_dea) and latest_dif > latest_dea:
            return ['DIF_UP_DEA']
            
        return []

class RuleEngine:
    """规则引擎：用于注册和执行多个规则"""
    def __init__(self):
        self.rules: List[BaseRule] = []
        
    def register_rule(self, rule: BaseRule):
        """注册规则"""
        self.rules.append(rule)
        
    def evaluate_all(self, day_line: List[Dict[str, Any]]) -> List[str]:
        """执行所有注册的规则并汇总标签"""
        tags = set()
        for rule in self.rules:
            try:
                rule_tags = rule.evaluate(day_line)
                if rule_tags:
                    tags.update(rule_tags)
            except Exception as e:
                # 如果单个规则执行出错，记录日志并跳过
                import logging
                logging.error(f"Error evaluating rule {rule.__class__.__name__}: {e}")
        return list(tags)
