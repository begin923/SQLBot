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
import json
from typing import Dict, Any, List

from common.utils.utils import SQLBotLogUtil
from apps.extend.format.parse_md_to_json import ParseMDToJson
from apps.extend.drilldown.drill_agg_rule_engine import DrillAggRuleEngine
from typing import Dict, Any, List, Tuple


class MetricDrilldownHandler:
    """指标下钻分析器，集成 MD 文档解析能力"""
    
    def __init__(self):
        """
        初始化指标下钻分析器
        
        Args:
        """
        self.parser = ParseMDToJson()
        self.rule_engine = DrillAggRuleEngine()  # 新增：聚合规则引擎

    def extract_metrics_and_intent(self, llm_client, question: str) -> Tuple[List[str], Dict[str, Any]]:
        """
        从用户问题中同时提取指标名称和用户意图
        
        功能：
        1. 分析用户问题，提取其中提到的指标字段名
        2. 分析用户意图（下钻类型、聚合规则）
        
        Args:
            llm_client: LLM 客户端实例
            question: 用户问题
            
        Returns:
            Tuple[List[str], Dict[str, Any]]: (指标名称列表，用户意图字典)
            用户意图字典格式：{
                "is_granular": bool,      # 是否粒度下钻
                "is_raw": bool,           # 是否穿透下钻（查明细）
                "need_agg": bool,         # 是否需要聚合
                "agg_rule_desc": str,     # 聚合规则描述
                "drill_type_reason": str  # 判断理由
            }
            
        示例:
            输入："按月下钻指标 d7_sum 的明细数据"
            输出：(
                ["d7_sum"],
                {
                    "is_granular": True,
                    "is_raw": True,
                    "need_agg": False,
                    "agg_rule_desc": "不聚合，查原始明细",
                    "drill_type_reason": "包含'明细'关键词，触发穿透下钻"
                }
            )
        """
        from langchain_core.messages import HumanMessage
        import json
        
        # ========== 步骤 1：LLM 提取指标名称和初步意图 ==========
        prompt = f"""# 角色
你是数据查询意图分析专家。

# 任务
从用户问题中提取：
1. 所有提到的指标名称（字段名）
2. 用户查询意图（下钻类型、是否要明细）

# 用户问题
{question}

# 提取规则
## 1. 识别指标字段
- 提取用户提到的具体指标名称（中文或英文）
- 保留原始名称（用户说中文就输出中文，说英文就输出英文）

## 2. 判断用户意图
- **粒度下钻**：包含"下钻"、"钻取"、"drill"、"按维度拆分"等词
- **穿透下钻**：包含"明细"、"明细数据"、"明细查询"、"raw"、"detail"等词
- **聚合查询**：包含"汇总"、"合计"、"统计"、"总和"、"平均值"等词

## 3. 排除非指标词汇
- 动词类：查询、统计、汇总、分析、查看、展示、显示
- 名词类：数据、指标、明细、维度、总和、平均值、最大值、最小值
- 时间类：年、月、日、季度、年度、月度（除非是字段名如 dt_year）
- 其他：按、的、和、与、及、等

# 输出格式
严格按照以下 JSON 格式输出:
{{
    "metrics": ["指标 1", "指标 2", ...],
    "intent": {{
        "is_granular": true/false,      // 是否粒度下钻
        "is_raw": true/false,           // 是否穿透下钻（查明细）
        "need_agg": true/false,         // 是否需要聚合
        "reason": "判断理由说明"
    }}
}}

# 示例
## 示例 1 - 粒度下钻
输入："按月下钻指标 d7_sum"
输出：
{{
    "metrics": ["d7_sum"],
    "intent": {{
        "is_granular": true,
        "is_raw": false,
        "need_agg": true,
        "reason": "包含'下钻'关键词，需要按维度拆分并聚合"
    }}
}}

## 示例 2 - 穿透下钻（查明细）
输入："查询 d7_sum 的明细数据"
输出：
{{
    "metrics": ["d7_sum"],
    "intent": {{
        "is_granular": false,
        "is_raw": true,
        "need_agg": false,
        "reason": "包含'明细'关键词，查询原始明细数据，不需要聚合"
    }}
}}

## 示例 3 - 聚合查询
输入："汇总统计销售额和销售量"
输出：
{{
    "metrics": ["销售额", "销售量"],
    "intent": {{
        "is_granular": false,
        "is_raw": false,
        "need_agg": true,
        "reason": "包含'汇总'、'统计'关键词，需要聚合查询"
    }}
}}

请根据上述规则，从用户问题中提取指标名称和意图。"""
        
        # 调用 LLM 提取
        response = llm_client.invoke([HumanMessage(content=prompt)])
        response_text = response.content if hasattr(response, 'content') else str(response)
        SQLBotLogUtil.info(f"Metric and intent extraction response: {response_text}")
        
        # 解析 JSON 结果
        try:
            result = json.loads(response_text, strict=False)
            metrics = result.get("metrics", [])
            intent = result.get("intent", {})
            
            # 确保 metrics 是列表
            if not isinstance(metrics, list):
                metrics = []
            
            # ========== 步骤 2：使用规则引擎校验意图 ==========
            # 基于 drill_agg_rule_engine.py 的规则进行二次判断
            rule_is_granular, rule_is_raw = self.rule_engine.judge_drill_type(question)
            
            # 统一计算 need_agg 和 agg_rule_desc（只调用一次）
            agg_rule_desc, need_agg = self.rule_engine.get_agg_rule(
                question=question,
                curr_layer="unknown",  # 暂时未知，后续根据元数据更新
                is_granular=rule_is_granular,
                is_raw=rule_is_raw
            )
            
            # 如果 LLM 判断和规则引擎不一致，以规则引擎为准（更可靠）
            if intent:
                intent['is_granular'] = rule_is_granular
                intent['is_raw'] = rule_is_raw
                intent['need_agg'] = need_agg
                intent['agg_rule_desc'] = agg_rule_desc
            else:
                intent = {
                    "is_granular": rule_is_granular,
                    "is_raw": rule_is_raw,
                    "need_agg": need_agg,
                    "agg_rule_desc": "",
                    "reason": ""
                }
            
            SQLBotLogUtil.info(f"Extracted metrics: {metrics}, intent: {intent}")
            return metrics, intent
            
        except Exception as e:
            SQLBotLogUtil.error(f"Failed to parse LLM response: {str(e)}")
            # 降级处理：只提取指标，使用规则引擎判断意图
            metrics = self.get_metric_from_question(llm_client, question)
            rule_is_granular, rule_is_raw = self.rule_engine.judge_drill_type(question)
            agg_rule_desc, need_agg = self.rule_engine.get_agg_rule(
                question=question,
                curr_layer="unknown",
                is_granular=rule_is_granular,
                is_raw=rule_is_raw
            )
            intent = {
                "is_granular": rule_is_granular,
                "is_raw": rule_is_raw,
                "need_agg": need_agg,
                "agg_rule_desc": agg_rule_desc,
                "reason": f"LLM 解析失败，使用规则引擎判断：{str(e)}"
            }
            return metrics, intent

    def get_metric_from_question(self, llm_client, question: str) -> List[str]:
        """
        从用户问题中提取指标名称列表（保留原有方法作为降级方案）
        
        功能：分析用户问题，提取其中提到的指标字段名
        
        Args:
            llm_client: LLM 客户端实例
            question: 用户问题
            
        Returns:
            List[str]: 指标名称列表
            
        示例:
            输入："查询 d7_sum 和 app_feed 的明细数据"
            输出：["d7_sum", "app_feed"]
        """
        from langchain_core.messages import HumanMessage
        import json
        
        # 构建提示词：让 LLM 提取指标名称
        prompt = f"""# 角色
你是数据字段提取专家。

# 任务
从用户问题中提取所有提到的指标名称（字段名）。

# 用户问题
{question}

# 提取规则
1. **识别指标字段**：提取用户提到的具体指标名称（中文或英文）
2. **排除非指标词汇**：
   - 动词类：查询、统计、汇总、分析、查看、展示、显示
   - 名词类：数据、指标、明细、维度、总和、平均值、最大值、最小值
   - 时间类：年、月、日、季度、年度、月度（除非是字段名如 dt_year）
   - 其他：按、的、和、与、及、等
3. **保留原始名称**：
   - 如果用户说的是中文字段名（如：销售额），就输出中文
   - 如果用户说的是英文字段名（如：d7_sum），就输出英文
   - 不要翻译或转换用户使用的名称
4. **注意上下文**：
   - 如果用户说"XX 指标"，XX 就是要提取的字段
   - 如果用户说"XX 字段"，XX 就是要提取的字段
   - 如果用户说"XX 的明细/汇总/统计"，XX 就是要提取的字段
5. **返回纯指标名称列表**：不要包含其他信息

# 输出格式
严格按照以下 JSON 格式输出:
{{
    "metrics": ["指标 1", "指标 2", ...]
}}

# 示例（全部是纯中文提问）
## 示例 1 - 中文指标名
输入："查看销售额和销售量的趋势"
输出：{{"metrics": ["销售额", "销售量"]}}

## 示例 2 - 带维度的查询
输入："按部门统计员工人数和平均工资"
输出：{{"metrics": ["员工人数", "平均工资"]}}

## 示例 3 - 单个指标
输入："下钻指标销售额"
输出：{{"metrics": ["销售额"]}}

## 示例 4 - 泛指业务词汇
输入："查看月度销售数据"
输出：{{"metrics": ["销售"]}}

请根据上述规则，从用户问题中提取指标名称。保留用户使用的原始名称（中文或英文）。"""
        
        # 调用 LLM 提取
        response = llm_client.invoke([HumanMessage(content=prompt)])
        response_text = response.content if hasattr(response, 'content') else str(response)
        SQLBotLogUtil.info(f"Metric extraction response: {response_text}")
        
        # 解析 JSON 结果
        try:
            result = json.loads(response_text, strict=False)
            metrics = result.get("metrics", [])
            return metrics if isinstance(metrics, list) else []
        except Exception as e:
            SQLBotLogUtil.error(f"Failed to parse LLM response: {str(e)}")
            return []

# ===================== 使用示例 =====================
if __name__ == "__main__":
    """测试指标下钻分析器"""
    
    # 配置 MD 文档路径
    MD_BASE_DIR = r"D:\codes\AIDataEasy\data_governance_agent\sql_to_md\data_governance_md"
    
    # 创建分析器
    analyzer = MetricDrilldownHandler()
    
    # 测试表名
    test_table = "yz_datawarehouse_ads.ads_algo_female_batch_production"
    
