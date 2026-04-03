"""
SQL 校验引擎
负责 SQL 的完整性校验和自动修复
"""
import orjson
import re
from typing import Optional, Tuple, List, Dict, Any
from langchain_core.messages import SystemMessage, HumanMessage

from apps.extend.utils.utils import Utils
from common.utils.utils import SQLBotLogUtil


class SQLValidator:
    """SQL 校验引擎类
    
    职责：
    1. 执行 SQL 规则校验
    2. 协调大模型进行 SQL 修复
    3. 提供统一的校验接口
    """
    
    def __init__(self, llm=None):
        """
        初始化 SQL 校验引擎
        
        Args:
            llm: LLM 实例，用于 SQL 修复时的大模型调用
        """
        self.llm = llm
    
    @staticmethod
    def _validate_sql(sql: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        校验 SQL 是否符合规则
        
        Args:
            sql: 待校验的 SQL
            
        Returns:
            Tuple[bool, Optional[str], Optional[str]]:
                - (True, None, None): 校验通过
                - (False, error_type, error_msg): 校验失败
                    - error_type: 错误类型
                        - 'LAYER_VIOLATION': 层级违规（查询 DWD/ODS）→ 不需要调用大模型
                        - 'MISSING_CLAUSE': 缺少必要子句（GROUP BY 或聚合函数）→ 需要调用大模型
                    - error_msg: 具体错误信息
        
        校验规则：
        1. 如果 SQL 涉及 DWD/ODS 层表，直接拦截（错误类型：LAYER_VIOLATION）
        2. 如果 SQL 涉及 ADS/DWS 层表，必须包含 GROUP BY 和聚合函数（错误类型：MISSING_CLAUSE）
        3. 检查是否使用了常用聚合函数（仅做提示性检查，不拦截）
        """
        if not sql:
            return False, 'EMPTY_SQL', "SQL 为空"
        
        sql_upper = sql.upper()
        
        # 1. 检查是否涉及 DWD/ODS 层（直接拦截）
        dwd_ods_pattern = r'(DWD|ODS)[_\.]'
        if re.search(dwd_ods_pattern, sql_upper):
            return False, 'LAYER_VIOLATION', "❌ 层级违规：不允许直接查询 DWD/ODS 层明细数据。请使用 ADS/DWS 汇总层或重新生成 SQL。"
        
        # 2. 检查是否为 ADS/DWS 层查询
        ads_dws_pattern = r'(ADS|DWS)[_\.]'
        if re.search(ads_dws_pattern, sql_upper):
            # 检查是否有 GROUP BY
            group_by_pattern = r'\bGROUP\s+BY\b'
            has_group = bool(re.search(group_by_pattern, sql, re.IGNORECASE))
            
            # 检查是否有聚合函数
            aggregate_functions = [
                'SUM', 'COUNT', 'AVG', 'MAX', 'MIN',
                'GROUP_CONCAT', 'ARRAY_AGG', 'STRING_AGG'
            ]
            has_agg = False
            for func in aggregate_functions:
                pattern = rf'\b{func}\s*\('
                if re.search(pattern, sql_upper):
                    has_agg = True
                    break
            
            # 严格校验：必须同时包含 GROUP BY 和聚合函数
            if not has_group and not has_agg:
                return False, 'MISSING_CLAUSE', "❌ 缺少 GROUP BY 和聚合函数：汇总层 (ADS/DWS) 必须使用聚合函数 + GROUP BY，请添加 GROUP BY 子句和聚合函数（如 SUM、COUNT 等）。"
            
            if not has_group and has_agg:
                return False, 'MISSING_CLAUSE', "❌ 缺少 GROUP BY：有聚合函数但缺少 GROUP BY 子句。汇总层 (ADS/DWS) 必须同时包含聚合函数和 GROUP BY，请添加 GROUP BY 子句。"
            
            if has_group and not has_agg:
                return False, 'MISSING_CLAUSE', "❌ 缺少聚合函数：有 GROUP BY 但缺少聚合函数。汇总层 (ADS/DWS) 必须使用聚合函数（如 SUM、COUNT、AVG 等），请修改 SELECT 字段。"
        
        # 3. 检查常用聚合函数（仅做提示性检查，不拦截）
        aggregate_functions = [
            'SUM',      # 求和
            'COUNT',    # 计数
            'AVG',      # 平均值
            'MAX',      # 最大值
            'MIN',      # 最小值
            'GROUP_CONCAT',  # 分组拼接（MySQL）
            'ARRAY_AGG',     # 数组聚合（PostgreSQL）
            'STRING_AGG'     # 字符串聚合（PostgreSQL/SQL Server）
        ]
        
        found_aggregates = []
        for func in aggregate_functions:
            pattern = rf'\b{func}\s*\('
            if re.search(pattern, sql_upper):
                found_aggregates.append(func)
        
        if found_aggregates:
            SQLBotLogUtil.info(f"检测到聚合函数：{', '.join(found_aggregates)}")
        
        return True, None, None
    
    def set_llm(self, llm):
        """设置 LLM 实例"""
        self.llm = llm
    
    def retry_generate(self, original_sql: str, error_info: str) -> Optional[str]:
        """
        当 SQL 校验失败或执行失败时，调用大模型重新生成 SQL
        
        Args:
            original_sql: 原始 SQL
            error_info: 错误信息
            
        Returns:
            Optional[str]: 修复后的 SQL（JSON 格式），无法修复返回 None
        """
        if not self.llm:
            SQLBotLogUtil.error("未设置 LLM 实例，无法进行 SQL 修复")
            return None
        
        try:
            # 构建错误修复提示词
            retry_message = [
                SystemMessage(content="""你是一个 SQL 专家，擅长修复执行失败的 SQL 语句。
我会提供：
1. 原始 SQL 语句
2. 执行时的错误信息

请你根据错误信息分析原因，并生成修复后的正确 SQL。

要求：
- 必须使用 JSON 格式返回，格式为：{"success": true, "sql": "修复后的 SQL"}
- 如果无法修复，返回：{"success": false, "message": "无法修复的原因"}
- 保持原有查询意图不变
- 确保语法符合数据库规范"""),
                HumanMessage(content=f"""原始 SQL:
```sql
{original_sql}
```

执行错误:
{error_info}

请修复这个 SQL，并以 JSON 格式返回：""")
            ]
            
            SQLBotLogUtil.info("开始重试生成 SQL...")
            full_retry_text = ''
            token_usage = {}
            
            from apps.chat.task.llm import process_stream
            res = process_stream(self.llm.stream(retry_message), token_usage)
            for chunk in res:
                if chunk.get('content'):
                    full_retry_text += chunk.get('content')
            
            SQLBotLogUtil.info(f"重试生成结果：{full_retry_text}")
            
            # 提取 JSON（去除可能的 markdown 标记）
            return self._extract_json_from_response(full_retry_text)
            
        except Exception as e:
            SQLBotLogUtil.error(f"重试生成 SQL 失败：{e}")
            return None
    
    def _extract_json_from_response(self, response_text: str) -> Optional[str]:
        """
        从响应文本中提取 JSON
        
        Args:
            response_text: 响应文本
            
        Returns:
            Optional[str]: JSON 字符串，如果无效返回 None
        """
        retry_json_str = response_text.strip()
        
        # 去除 markdown 标记
        if retry_json_str.startswith('```json'):
            retry_json_str = retry_json_str[7:]
        if retry_json_str.startswith('```'):
            retry_json_str = retry_json_str[3:]
        if retry_json_str.endswith('```'):
            retry_json_str = retry_json_str[:-3]
        retry_json_str = retry_json_str.strip()
        
        # 验证是否为有效 JSON
        try:
            data = orjson.loads(retry_json_str)
            if data.get('success') and data.get('sql'):
                return retry_json_str  # 返回完整的 JSON 字符串
        except:
            pass
        
        # 如果不是 JSON，尝试提取 SQL
        if any(keyword in retry_json_str.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE']):
            # 构造 JSON 格式返回
            return orjson.dumps({'success': True, 'sql': retry_json_str}).decode()
        
        # 如果返回 FAILED 或空，说明无法修复
        if not retry_json_str or retry_json_str.upper() == 'FAILED':
            return None
        
        return retry_json_str
    
    def validate_and_fix(self, sql: str) -> Tuple[bool, str, Optional[str]]:
        """
        校验 SQL 并尝试自动修复（如果失败）
        
        Args:
            sql: 待校验的 SQL
            
        Returns:
            Tuple[bool, str, Optional[str]]: 
                - (True, original_sql, None): 校验通过
                - (True, fixed_sql, None): 校验失败但修复成功
                - (False, original_sql, error_msg): 校验失败且无法修复
        """
        # 第一步：校验
        is_valid, error_type, error_msg = self._validate_sql(sql)
        
        if is_valid:
            return True, sql, None
        
        # 第二步：根据错误类型判断是否需要调用大模型
        # LAYER_VIOLATION: 层级违规，不需要调用大模型（应该重新选择表）
        # MISSING_CLAUSE: 缺少子句，需要调用大模型修复
        if error_type == 'LAYER_VIOLATION':
            # 层级违规，不调用大模型，直接返回
            SQLBotLogUtil.warning(f"SQL 层级违规，无需修复：{error_msg}")
            return False, sql, error_msg
        
        # MISSING_CLAUSE 或其他需要修复的错误
        SQLBotLogUtil.info(f"SQL 校验失败（类型：{error_type}）：{error_msg}，尝试自动修复...")
        fixed_sql_json = self.retry_generate(sql, error_msg)
        
        if fixed_sql_json:
            try:
                fixed_data = orjson.loads(fixed_sql_json)
                if fixed_data.get('success') and fixed_data.get('sql'):
                    fixed_sql = fixed_data['sql']
                    SQLBotLogUtil.info(f"SQL 自动修复成功：{fixed_sql}")
                    return True, fixed_sql_json, None
            except Exception as e:
                SQLBotLogUtil.error(f"解析修复后的 SQL 失败：{e}")
        
        # 第三步：无法修复
        SQLBotLogUtil.warning(f"SQL 校验失败且无法自动修复：{error_msg}")
        return False, sql, error_msg

if __name__ == '__main__':
    llm = Utils.create_llm_client()
    sql_validator = SQLValidator(llm)

    sql = """
SELECT `org_id` AS `pig_farm_id`,
       `back_female` AS `back_female_count`,
       `dt_date` AS `business_date`
FROM `yz_datawarehouse_dws`.`dws_pig_inventory_day`
WHERE `dt_date` >= DATE_SUB('2026-04-03', INTERVAL 6 MONTH)
group by org_id,dt_date
ORDER BY `org_id`,
         `dt_date`
LIMIT 1000
    """
    is_valid, fixed_sql, error_msg = sql_validator.validate_and_fix(sql)
    print(f"is_valid:{is_valid}, fixed_sql:{fixed_sql}, error_msg:{error_msg}")