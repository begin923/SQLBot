"""
指标下钻分析器 - 集成 MD 文档解析的下钻分析功能

核心流程:
    1. 用户问题包含"下钻/钻取/drill"关键词 → 触发下钻分析
    2. LLM 判断是查询当前表还是上游表
    3. 如果查询当前表 → 直接设置静态 SQL 模式并执行
    4. 如果查询上游表 → 提取血缘信息，生成 SQL 后执行

使用方法:
    1. 配置 MD 文档基础目录路径
    2. 调用 handle_drilldown_for_llm() 处理下钻逻辑
    3. 根据返回值决定后续操作
"""
import os
import re
import json
from typing import Dict, List, Optional, Any, Tuple

from common.utils.utils import SQLBotLogUtil
from apps.extend.parse_md_to_json import ParseMDToJson


class MetricDrilldownHandler:
    """指标下钻分析器，集成 MD 文档解析能力"""
    
    def __init__(self):
        """
        初始化指标下钻分析器
        
        Args:
        """
        self.parser = ParseMDToJson()

    # ========== 公共静态方法：llm.py 调用入口 ==========
    
    @staticmethod
    def handle_drilldown_for_llm(llm_service, question: str, llm_client) -> bool:
        """
        为 llm.py 提供的下钻处理辅助方法 (主入口)
        
        流程:
        1. 调用 decide_table_scope 判断意图并提取表名
        2. 返回 False，让 llm.py 继续处理（添加表到数据源后由大模型生成 SQL）
        
        Args:
            llm_service: LLMService 实例 (用于设置 is_static_sql 和 provided_sql)
            question: 用户问题
            llm_client: LLM 客户端实例
            
        Returns:
            bool: 是否已处理下钻 (始终返回 False，因为只提取表名，不设置静态 SQL)
        """
        try:
            # 1. 调用 decide_table_scope LLM 判断当前表/上游表并提取表名
            extract_result = MetricDrilldownHandler.get_user_intent_and_table_scope(llm_client, question)
            
            # 2. 返回 False，让 llm.py 继续处理（添加表到数据源后由大模型生成 SQL）
            SQLBotLogUtil.info('Extracted table name from drilldown query, let llm.py handle SQL generation')
            return False
            
        except Exception as e:
            SQLBotLogUtil.error(f'Error in handle_drilldown_for_llm: {str(e)}')
            return False
    
    # ========== 辅助工具方法 ==========
    def get_user_intent_and_table_scope(self, llm_client, question: str) -> Dict[str, Any]:
        """
        分析用户意图和表范围
        
        功能：直接根据用户问题判断用户意图和表查询范围并返回结果，不做其他操作
        
        Args:
            llm_client: LLM 客户端实例
            question: 用户问题
            
        Returns:
            dict: {
                "is_current_table": bool,  # 是否查询当前表 (下钻且有 SQL 时为 true)
                "table_name": "表名",  # 非下钻查询或需要查询上游表时的表名
                "metrics": List[str],  # 指标列表
                "reason": "判断理由"
            }
            
        注意：
        - 删除了 sql 字段，避免触发静态 SQL 模式
        - 只用于提取表名，供后续添加表到数据源使用
            
        与 extract_blood_from_md 的关系:
        - decide_table_scope: 负责判断"查询什么表"
        - extract_blood_from_md: 负责"解析该表的血缘数据"
        - 先调用 decide_table_scope 获取表名，再调用 extract_blood_from_md 解析血缘
        """
        from langchain_core.messages import HumanMessage

        # TODO 新增判断sql是对当前表下钻还是查询上游表
        # 构建提示词：让 LLM 分析用户问题的查询范围
        prompt = f"""# 角色
你是数据查询意图分析专家。

# 任务
1、分析用户问题，判断是下钻查询还是查询明细
2、基于第一点，判断已执行sql类型
3、基于第二点，判断指标血缘指向当前表还是上游表，并根据意图准确标识指标名称

# 用户问题
{question}

# 分析要点
1. 判断是否是下钻查询（关键词：下钻、钻取、drill）
2. 判断是否是查询明细（关键词：查询明细、明细查询）
3. **关键血缘逻辑**（必须严格遵守）：
   - **若指标血缘指向当前表** → `is_current_table = true`（查询当前表）
   - **若指标血缘指向上游表** → `is_current_table = false`（查询上游表）
4. 提取信息：
   - 表名：指标血缘指定的目标表
   - 指标：指标字段名（从问题或SQL提取）

# 输出格式
严格按照以下JSON格式输出:
{{
    "is_current_table": true/false,
    "table_name": "指标血缘指定的目标表名",
    "metrics": ["指标1", "指标2"],
    "reason": "判断理由说明（基于血缘配置）"
}}

# 示例 1 - 聚合指标（血缘指向当前表）
输入："按月下钻指标 d7_sum ; 已执行sql: select dt_year, sum(d7_estrus) as d7_sum from yz_datawarehouse_ads.ads_pig_feed_day group by dt_year"
输出：
{{
    "is_current_table": true,
    "table_name": "yz_datawarehouse_ads.ads_pig_feed_day",
    "metrics": ["d7_sum"],
    "reason": "根据sql分析指标d7_sum是聚合指标，且来源 d7_estrus ,因为表结构有关于按月度统计维度，所以血缘指向当前表（yz_datawarehouse_ads.ads_pig_feed_day）"
}}

# 示例 2 - 聚合指标（血缘指向当前表）
输入："按天下钻指标 d7_sum ; 已执行sql: select dt_year, sum(d7_estrus) as d7_sum from yz_datawarehouse_ads.ads_pig_feed_day group by dt_year"
输出：
{{
    "is_current_table": true,
    "table_name": "yz_datawarehouse_ads.ads_pig_feed_day",
    "metrics": ["d7_estrus"],
    "reason": "根据sql分析指标d7_sum是聚合指标，且来源 d7_estrus ,因为表结构的最细粒度是按的统计维度，所以血缘指向当前表（yz_datawarehouse_ads.ads_pig_feed_day）"
}}

# 示例 3 - 明细指标（血缘指向当前表）
输入："查询指标明细数据 ; 已执行sql: select dt_month, sum(app_feed) as app_feed_sum from yz_datawarehouse_ads.ads_pig_feed_day group by dt_month"
输出：
{{
    "is_current_table": true,
    "table_name": "yz_datawarehouse_ads.ads_pig_feed_day",
    "metrics": ["app_feed"],
    "reason": "由于当前sql是聚合查询且且需求是查询指标app_feed_sum是明细指标app_feed字段的明细数据，只需要调整为查询当前的明细，血缘指向当前表"
}}

# 示例 4 - 明细指标（血缘指向上游表）
输入："查询指标明细数据 ; 已执行sql: select dt_day, app_feed from yz_datawarehouse_ads.ads_pig_feed_day"
输出：
{{
    "is_current_table": false,
    "table_name": "yz_datawarehouse_ads.ads_pig_feed_day",
    "metrics": ["app_feed"],
    "reason": "由于当前sql是明细查询且需求是查询指标app_feed是明细指标，血缘指向上游表"
}}"""
        
        # 调用 LLM 分析
        response = llm_client.invoke([HumanMessage(content=prompt)])
        response_text = response.content if hasattr(response, 'content') else str(response)
        SQLBotLogUtil.info(f"User intent and table scope response: {response_text}")
        
        # 解析 JSON 结果
        try:
            result = json.loads(response_text, strict=False)
            # 删除 sql 字段（如果存在），确保不会触发静态 SQL 模式
            if 'sql' in result:
                del result['sql']
            return result
        except Exception as e:
            SQLBotLogUtil.error(f"Failed to parse LLM response: {str(e)}")
            return {
                "is_current_table": False,
                "table_name": "",
                "metrics": [],
                "reason": f"解析失败：{str(e)}"
            }


    # ========== MD 文档解析相关方法 ==========
    def extract_metric_blood_from_md(self, table_name: str) -> Dict[str, Any]:
        """
        从指定表的 MD 文档 ，提取指标血缘数据
        
        Args:
            table_name: 表名 (如："ads_sales_summary")

        Returns:
            解析结果字典
        """
        
        # 直接调用 ParseMD.read_md_by_table 方法
        result = self.parser.parse_md_to_json(table_name)
        
        if result["success"]:
            SQLBotLogUtil.info(f"Successfully parsed MD for table: {table_name}")
        else:
            SQLBotLogUtil.warning(f"Failed to parse MD for table {table_name}: {result.get('error')}")
        
        return result
    
    def _build_upstream_tables_info(self, field_blood: Dict) -> str:
        """
        构建上游表信息 XML
        
        Args:
            field_blood: 血缘字段数据
            
        Returns:
            上游表信息 XML 字符串
        """
        upstream_map = {}
        
        for target_field, blood_records in field_blood.items():
            for record in blood_records:
                source_field = record.get("源字段")
                if not source_field:
                    continue
                
                # 从源字段中提取表名
                source_parts = source_field.split('.')
                if len(source_parts) >= 2:
                    source_table = '.'.join(source_parts[:-1])
                else:
                    continue
                
                # 只关注 ads/dws/dwd/ods 层的表 (上游表，更明细的数据)
                if not any(prefix in source_table.lower() for prefix in ['ads','dws', 'dwd', 'dim', 'ods']):
                    continue
                
                if source_table not in upstream_map:
                    upstream_map[source_table] = {
                        'table_name': source_table,
                        'fields': [],
                        'metrics': []
                    }
                
                upstream_map[source_table]['fields'].append(source_field)
                
                # 判断是否是指标
                field_type = record.get("字段类型", "")
                if field_type == "指标":
                    upstream_map[source_table]['metrics'].append({
                        'field': source_field.split('.')[-1],
                        'aggregation': record.get("聚合方式", "")
                    })
        
        # 构建 XML
        xml_lines = [f"<upstream-table-count>{len(upstream_map)}</upstream-table-count>"]
        
        for i, us_table in enumerate(upstream_map.values(), 1):
            xml_lines.append(f"<upstream-table-{i}>")
            xml_lines.append(f"  <table-name>{us_table['table_name']}</table-name>")
            xml_lines.append(f"  <available-fields>{len(us_table['fields'])}</available-fields>")
            if us_table['metrics']:
                xml_lines.append(f"  <metrics-count>{len(us_table['metrics'])}</metrics-count>")
            xml_lines.append(f"</upstream-table-{i}>")
        
        return "\n".join(xml_lines)



    # ========== 核心业务方法：下钻分析完整流程 ==========
    
    @staticmethod
    def handle_drilldown_analysis(llm_service, session):
        """
        处理下钻分析的完整流程 (主入口)
        
        流程:
        1. 检测"下钻/钻取/drill"关键词
        2. 调用 handle_drilldown_for_llm() 判断意图
        3. 如果返回 True → 已设置静态 SQL 模式，直接返回
        4. 如果返回 False → 继续基于血缘生成 SQL
        
        Args:
            llm_service: LLMService 实例
            session: 数据库会话
        """
        from common.utils.utils import SQLBotLogUtil

        # 使用新的静态方法处理下钻逻辑
        handled = MetricDrilldownHandler.handle_drilldown_for_llm(
            llm_service=llm_service,
            question=llm_service.chat_question.question,
            llm_client=llm_service.llm
        )

        extract_result = MetricDrilldownHandler.get_user_intent_and_table_scope(llm_service.llm, llm_service.chat_question.question)

        if handled:
            # 已处理下钻且提供了 SQL，直接返回
            SQLBotLogUtil.info("Drilldown query handled by metric_drilldown module (static SQL mode set)")
            return

        # 未提供 SQL，需要继续处理 (基于血缘生成 SQL)
        SQLBotLogUtil.info("No SQL provided for drilldown, generating SQL based on bloodline")

        # 应用指标下钻技能，提取血缘信息
        try:
            parse_res = MetricDrilldownHandler._apply_metric_drilldown_skill(llm_service)
            SQLBotLogUtil.info(f"Drilldown analysis completed - result: {parse_res}")

            # 从解析结果中提取血缘信息
            field_blood = parse_res.get('field_blood', {}) if isinstance(parse_res, dict) else {}

            # 基于血缘信息调用 LLM 生成 SQL
            if field_blood:
                # 调用 LLM 生成 SQL
                generated_sql = MetricDrilldownHandler.generate_drilldown_sql_by_llm(llm_service, field_blood)

                if generated_sql:
                    # 设置为静态 SQL 模式
                    llm_service.is_static_sql = True
                    llm_service.provided_sql = generated_sql
                    SQLBotLogUtil.info(f"Generated SQL by LLM for drilldown: {generated_sql}")
                    SQLBotLogUtil.info(f"Set is_static_sql={llm_service.is_static_sql}, provided_sql={llm_service.provided_sql}")
                else:
                    SQLBotLogUtil.warning("LLM failed to generate SQL, using fallback simple SQL")

                    # 降级方案：使用简单的 SQL 拼接
                    target_fields = list(field_blood.keys())
                    source_tables = set()
                    for field_name, blood_records in field_blood.items():
                        for record in blood_records:
                            source_table = record.get('源表', '')
                            if source_table:
                                source_tables.add(source_table)

                    if source_tables and target_fields:
                        source_table = list(source_tables)[0]
                        sql = f"SELECT " + ", ".join(target_fields) + f" FROM {source_table}"
                        llm_service.is_static_sql = True
                        llm_service.provided_sql = sql
                        SQLBotLogUtil.info(f"Built fallback SQL: {sql}")
                    else:
                        SQLBotLogUtil.warning("Cannot build fallback SQL due to missing source tables or fields")
            elif not field_blood:
                SQLBotLogUtil.warning("No field_blood found, cannot generate drilldown SQL")

        except Exception as e:
            SQLBotLogUtil.error(f"Failed to apply drilldown skill: {str(e)}")
    
    @staticmethod
    def _apply_metric_drilldown_skill(llm_service):
        """
        应用指标下钻技能到当前查询
        
        流程:
        1. 检测用户问题是否包含下钻需求
        2. 使用 LLM 从问题中提取表名和指标
        3. 解析血缘 MD 文档，找到字段血缘关系
           血缘关系：ods(源数据) -> dwd(明细) -> dws(服务) -> ads(应用/汇总)
        4. 将血缘信息添加到 custom_prompt 中，供后续 SQL 生成使用
        
        Args:
            llm_service: LLMService 实例
            
        Returns:
            dict: 与 parse_md_blood 相同的格式
                  {
                      "table_name": str,
                      "field_blood": {}
                  }
        """
        from common.utils.utils import SQLBotLogUtil
        
        try:
            analyzer = MetricDrilldownHandler()

            # 使用传入的 LLM 客户端进行分析
            parse_res = MetricDrilldownHandler.get_user_intent_and_table_scope(
                llm_client=llm_service.llm,  # 复用 LLMService 中的 llm 实例
                question=llm_service.chat_question.question
            )
            SQLBotLogUtil.info(f"Metric drilldown result: {parse_res}")
            
            # 确保返回格式与 parse_md_blood 一致
            if not parse_res:
                SQLBotLogUtil.warning("Empty parse result from analyze_metric_and_table")
                return {
                    "table_name": "",
                    "field_blood": {}
                }
            
            if not isinstance(parse_res, dict):
                SQLBotLogUtil.warning(f"Invalid parse result type: {type(parse_res)}, expected dict")
                return {
                    "table_name": "",
                    "field_blood": {}
                }
            
            # 提取血缘信息并构建提示词
            field_blood = parse_res.get('field_blood', {})
            table_name = parse_res.get('table_name', '')
            
            # 构建血缘关系描述
            blood_desc_lines = []
            if field_blood and table_name:
                for target_field, blood_records in field_blood.items():
                    for record in blood_records:
                        source_field = record.get('源字段', '')
                        conversion = record.get('转换逻辑', '')
                        
                        if source_field and conversion:
                            blood_desc_lines.append(f"- {target_field} ← {source_field} ({conversion})")
                
                if blood_desc_lines:
                    blood_desc = "\n【字段血缘关系】\n" + "\n".join(blood_desc_lines)
                    # 将血缘信息添加到 custom_prompt 中
                    if llm_service.chat_question.custom_prompt:
                        llm_service.chat_question.custom_prompt += blood_desc
                    else:
                        llm_service.chat_question.custom_prompt = blood_desc
                    SQLBotLogUtil.info(f"Added bloodline info to prompt")
            
            # 返回与 parse_md_blood 相同格式的结果
            return {
                "table_name": table_name,
                "field_blood": field_blood
            }
            
        except Exception as e:
            # 下钻技能应用失败不影响主流程
            SQLBotLogUtil.error(f"Error applying metric drilldown skill: {str(e)}")
            return {
                "table_name": "",
                "field_blood": {}
            }
    
    @staticmethod
    def generate_drilldown_sql_by_llm(llm_service, field_blood: dict):
        """
        基于血缘信息，调用大模型生成下钻 SQL
        
        Args:
            llm_service: LLMService 实例
            field_blood: 字段血缘信息字典，格式为：
                        {
                            "目标字段名": [
                                {
                                    "源字段": "源表。源字段",
                                    "源表": "yz_datawarehouse_dwd.dwd_feed_back",
                                    "转换逻辑": "SUM",
                                    "字段类型": "指标"
                                }
                            ]
                        }
            
        Returns:
            生成的 SQL 语句，如果生成失败则返回 None
        """
        from langchain_core.messages import SystemMessage, HumanMessage
        from common.utils.utils import SQLBotLogUtil
        import orjson
        import traceback
        
        # 辅助函数：提取 JSON
        def extract_nested_json(text: str) -> str:
            """从文本中提取 JSON 字符串"""
            import re
            # 尝试匹配```json 块
            json_pattern = r'```(?:json)?\s*(\{.*?\})\s*```'
            matches = re.findall(json_pattern, text, re.DOTALL)
            if matches:
                return matches[0]
            
            # 尝试直接查找{}之间的内容
            start = text.find('{')
            end = text.rfind('}') + 1
            if start != -1 and end > start:
                return text[start:end]
            
            return ""
        
        try:
            # 从 field_blood 中提取所有源表 (去重)
            source_tables = set()
            blood_desc_lines = []
            target_fields = list(field_blood.keys())
            
            for target_field, blood_records in field_blood.items():
                for record in blood_records:
                    source_field = record.get('源字段', '')
                    conversion = record.get('转换逻辑', '')
                    source_table = record.get('源表', '')
                    field_type = record.get('字段类型', '')
                    
                    # 收集源表
                    if source_table:
                        source_tables.add(source_table)
                    
                    # 构建血缘描述
                    if source_field and conversion:
                        blood_desc_lines.append(
                            f"- {target_field} ← {source_field} ({conversion}) [类型：{field_type}, 源表：{source_table}]"
                        )
            
            blood_desc = "\n".join(blood_desc_lines) if blood_desc_lines else "无"
            source_tables_list = list(source_tables) if source_tables else []
            
            # 构建提示词
            system_prompt = """你是一个 SQL 生成专家，擅长根据字段血缘关系和用户需求生成正确的 SQL 查询。

# 任务
根据提供的字段血缘关系和用户问题，生成符合以下要求的 SQL:
1. **理解下钻的含义**:
   - **目标字段**: 是上一个问题中查询的字段 (通常是聚合后的指标，如 d7_sum)
   - **下钻查询**: 就是查询目标字段的**来源**,即**源字段**(如 d7_estrus)
   - 简单说:**下钻 = 查看目标字段是从哪里来的 = SELECT 源字段**
2. SELECT 字段应该是:
   - 如果是下钻查询:SELECT 源字段 + 原始 SQL 中的维度字段 (如 org_id 等),不要加聚合函数
   - 如果是聚合查询:SELECT 聚合函数 (源字段) + 维度字段
3. 根据用户问题的语义添加合适的 WHERE、ORDER BY 等子句
4. 确保 SQL 语法正确且可执行
5. 只输出 SQL 语句本身，不要包含任何解释或额外文字
6. 以 JSON 格式输出，包含 sql 字段

# 关键判断规则
- 当用户说"下钻指标 XXX"时 = 查看该指标的明细数据 = SELECT 源字段 (不加聚合)
- 当用户说"按 XX 维度统计/汇总/求和"时 = 聚合查询 = SELECT 聚合函数 (源字段) GROUP BY 维度

# 重要提醒
- 如果用户问题中包含原始 SQL，需要**智能处理**:
  * ✅ 保留原始 SQL 的 **WHERE 条件**(如 org_id = xxx)
  * ✅ 保留原始 SQL 的 **维度字段**(如 SELECT org_id 中的 org_id)
  * ❌ 移除原始 SQL 的 **聚合函数**(如 SUM()、COUNT() 等)
  * ❌ 移除原始 SQL 的 **GROUP BY 子句**(因为下钻不需要分组)
- 示例说明:
  * 上个问题:`SELECT SUM(d7_estrus) AS d7_sum FROM ...` → 目标字段是 `d7_sum`
  * 下钻查询:`d7_sum` 从哪里来？→ 来自 `d7_estrus` → 所以 SELECT `d7_estrus`"""

            user_prompt = f"""# 用户问题
{llm_service.chat_question.question}

# 字段血缘关系
{blood_desc}

# 可用信息
目标字段列表:{target_fields}(这些是上个回答中查询的字段)
源表列表:{source_tables_list}

# 输出格式
请严格按照以下 JSON 格式输出:
{{
    "sql": "生成的 SQL 语句"
}}

# 示例对比
## 示例 1 - 下钻查询 (查看明细，无条件)
用户问题：下钻指标 d7_sum
字段血缘关系:
- d7_sum ← d7_estrus (SUM) [类型：指标，源表：yz_datawarehouse_ads.ads_algo_female_batch_production]
目标字段列表:["d7_sum"]
源表列表:["yz_datawarehouse_ads.ads_algo_female_batch_production"]
分析:
- "下钻"=查看明细
- **目标字段**:d7_sum(上个回答查询的字段)
- **源字段**:d7_estrus(d7_sum 的来源字段)
- **下钻查询**:就是查询 d7_sum 从哪里来 → SELECT d7_estrus
- 没有指定条件，直接查询所有明细
- 不要聚合，不要 GROUP BY
输出:{{"sql": "SELECT d7_estrus FROM yz_datawarehouse_ads.ads_algo_female_batch_production"}}

## 示例 2 - 聚合查询 (按维度汇总)
用户问题：按 org_id 汇总 d7_sum
字段血缘关系:
- d7_sum ← d7_estrus (SUM) [类型：指标，源表：yz_datawarehouse_dwd.dwd_feed_back]
目标字段列表:["d7_sum"]
源表列表:["yz_datawarehouse_dwd.dwd_feed_back"]
分析:
- "按 org_id 汇总"=聚合查询
- 需要 GROUP BY org_id
- SELECT SUM(d7_estrus) AS d7_sum
输出:{{"sql": "SELECT org_id, SUM(d7_estrus) AS d7_sum FROM yz_datawarehouse_dwd.dwd_feed_back GROUP BY org_id"}}

## 示例 3 - 带条件的下钻查询
用户问题：查询 org_id 为 709347917181313024 的 d7_sum 明细
字段血缘关系:
- d7_sum ← d7_estrus (SUM) [类型：指标，源表：yz_datawarehouse_ads.ads_algo_female_batch_production]
目标字段列表:["d7_sum"]
源表列表:["yz_datawarehouse_ads.ads_algo_female_batch_production"]
分析:
- "明细"=下钻查询
- **目标字段**:d7_sum(上个回答查询的字段)
- **源字段**:d7_estrus(d7_sum 的来源字段)
- **下钻查询**:查询 d7_sum 从哪里来 → SELECT d7_estrus
- 用户明确要求"org_id 为 709347917181313024"的条件
- 所以需要 WHERE，但不需要 GROUP BY
输出:{{"sql": "SELECT d7_estrus FROM yz_datawarehouse_ads.ads_algo_female_batch_production WHERE org_id = '709347917181313024'"}}

## 示例 4 - 下钻查询 (保留原始 SQL 的条件和维度，去除聚合)
用户问题：下钻指标 d7_sum {{"sql": "SELECT org_id, SUM(d7_estrus) AS d7_sum FROM yz_datawarehouse_ads.ads_algo_female_batch_production WHERE org_id = 709347917181313024 GROUP BY org_id"}}
字段血缘关系:
- d7_sum ← d7_estrus (SUM) [类型：指标，源表：yz_datawarehouse_ads.ads_algo_female_batch_production]
目标字段列表:["d7_sum"]
源表列表:["yz_datawarehouse_ads.ads_algo_female_batch_production"]
分析:
- 用户要求"下钻"=查看明细
- **目标字段**:d7_sum(上个回答查询的字段)
- **源字段**:d7_estrus(d7_sum 的来源字段)
- **下钻查询**:查询 d7_sum 从哪里来 → SELECT d7_estrus
- 原始 SQL 中有:
  * 维度字段:org_id(需要保留)
  * WHERE 条件:org_id = 709347917181313024(需要保留)
  * 聚合函数:SUM()(需要移除，因为下钻不要聚合)
  * GROUP BY:org_id(需要移除，因为下钻不要分组)
- 最终 SQL:SELECT org_id + 源字段 d7_estrus + WHERE 条件
输出:{{"sql": "SELECT org_id, d7_estrus FROM yz_datawarehouse_ads.ads_algo_female_batch_production WHERE org_id = 709347917181313024"}}

请根据上述要求，为当前用户问题生成 SQL。特别注意:
1. 如果用户问题中包含"下钻",请查询**源字段**(目标字段的来源),不要聚合，不要 GROUP BY!
2. 如果用户问题中包含原始 SQL，需要**智能处理**:
   - ✅ 保留 WHERE 条件
   - ✅ 保留维度字段
   - ❌ 移除聚合函数和 GROUP BY"""

            # 调用 LLM
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_prompt)
            ]
            
            response = llm_service.llm.invoke(messages)
            response_text = response.content if hasattr(response, 'content') else str(response)
            
            SQLBotLogUtil.info(f"LLM generated SQL response: {response_text}")
            
            # 解析 JSON 结果
            json_str = extract_nested_json(response_text)
            if not json_str:
                SQLBotLogUtil.warning(f"Failed to extract JSON from LLM response: {response_text}")
                return None
            
            result = orjson.loads(json_str)
            sql = result.get('sql', '').strip()
            
            if not sql:
                SQLBotLogUtil.warning("Generated SQL is empty")
                return None
            
            SQLBotLogUtil.info(f"Successfully generated SQL by LLM: {sql}")
            return sql
            
        except Exception as e:
            SQLBotLogUtil.error(f"Error generating SQL by LLM: {str(e)}, traceback: {traceback.format_exc()}")
            return None

# ===================== 使用示例 =====================
if __name__ == "__main__":
    """测试指标下钻分析器"""
    
    # 配置 MD 文档路径
    MD_BASE_DIR = r"D:\codes\AIDataEasy\data_governance_agent\sql_to_md\data_governance_md"
    
    # 创建分析器
    analyzer = MetricDrilldownHandler(MD_BASE_DIR)
    
    # 测试表名
    test_table = "yz_datawarehouse_ads.ads_algo_female_batch_production"
    
    # 测试 1: 解析 MD 文档
    print("\n===== 测试 MD 文档解析 =====")
    blood_result = analyzer.extract_metric_blood_from_md(test_table, "blood")
    print(json.dumps(blood_result, ensure_ascii=False, indent=2))
