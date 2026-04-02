import re
from typing import Optional, Tuple


class SQLValidator:
    """SQL 校验器，用于验证 ADS/DWS 层 SQL 的聚合规则"""
    
    @staticmethod
    def is_ads_dws_layer(sql: str) -> bool:
        """
        判断 SQL 是否涉及 ADS/DWS 层表
        
        Args:
            sql: SQL 语句
            
        Returns:
            bool: 如果是 ADS/DWS 层返回 True
        """
        if not sql:
            return False
        
        sql_upper = sql.upper()
        # 匹配表名中包含 ads 或 dws 的情况
        # 例如：yz_datawarehouse_ads.table_name, ads_table, dws_table
        pattern = r'(ADS|DWS)[_\.]'
        return bool(re.search(pattern, sql_upper))
    
    @staticmethod
    def has_group_by(sql: str) -> bool:
        """
        判断 SQL 是否包含 GROUP BY 子句
        
        Args:
            sql: SQL 语句
            
        Returns:
            bool: 如果包含 GROUP BY 返回 True
        """
        if not sql:
            return False
        
        # 使用正则匹配 GROUP BY（不区分大小写）
        pattern = r'\bGROUP\s+BY\b'
        return bool(re.search(pattern, sql, re.IGNORECASE))
    
    @staticmethod
    def has_aggregate_function(sql: str) -> bool:
        """
        判断 SQL 是否包含聚合函数
        
        Args:
            sql: SQL 语句
            
        Returns:
            bool: 如果包含聚合函数返回 True
        """
        if not sql:
            return False
        
        # 常见聚合函数
        aggregate_functions = [
            'SUM', 'COUNT', 'AVG', 'MAX', 'MIN',
            'GROUP_CONCAT', 'ARRAY_AGG', 'STRING_AGG'
        ]
        
        for func in aggregate_functions:
            pattern = rf'\b{func}\s*\('
            if re.search(pattern, sql, re.IGNORECASE):
                return True
        
        return False
    
    @staticmethod
    def validate_ads_dws_sql(sql: str) -> Tuple[bool, Optional[str]]:
        """
        校验 ADS/DWS 层 SQL 是否符合聚合规则
        
        规则：
        1. 如果 SQL 涉及 ADS/DWS 层表，必须包含 GROUP BY
        2. 如果 SQL 涉及 ADS/DWS 层表，SELECT 字段必须使用聚合函数包裹
        
        Args:
            sql: SQL 语句
            
        Returns:
            Tuple[bool, Optional[str]]: 
                - (True, None): 校验通过
                - (False, error_message): 校验失败，返回错误信息
        """
        if not sql:
            return False, "SQL 为空"
        
        # 1. 判断是否为 ADS/DWS 层查询
        if not SQLValidator.is_ads_dws_layer(sql):
            # 不是 ADS/DWS 层，直接通过
            return True, None
        
        # 2. 检查是否有 GROUP BY
        has_group = SQLValidator.has_group_by(sql)
        has_agg = SQLValidator.has_aggregate_function(sql)
        
        # 3. 严格校验：既有 GROUP BY 又有聚合函数
        if not has_group and not has_agg:
            error_msg = "❌ ADS/DWS 层 SQL 校验失败：缺少 GROUP BY 和聚合函数。汇总层 (ADS/DWS) 必须使用聚合函数 + GROUP BY，请重新生成 SQL。"
            return False, error_msg
        
        if not has_group and has_agg:
            error_msg = "❌ ADS/DWS 层 SQL 校验失败：有聚合函数但缺少 GROUP BY。汇总层 (ADS/DWS) 必须同时包含聚合函数和 GROUP BY，请重新生成 SQL。"
            return False, error_msg
        
        if has_group and not has_agg:
            error_msg = "❌ ADS/DWS 层 SQL 校验失败：有 GROUP BY 但缺少聚合函数。汇总层 (ADS/DWS) 必须使用聚合函数，请重新生成 SQL。"
            return False, error_msg
        
        # 4. 校验通过
        return True, None
