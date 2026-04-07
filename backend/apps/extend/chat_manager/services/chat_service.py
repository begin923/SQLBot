"""
ChatService - 聊天业务逻辑服务

负责组织和编排业务流程：
1. 从用户问题中提取指标、维度和过滤条件
2. 自动从聊天历史中补充缺失的字段
3. 提供统一的字段提取接口
"""

from typing import Dict, Any, Tuple, List
from langchain_core.messages import HumanMessage
import json

from apps.extend.chat_manager.services.chat_state_service import ChatStateService
from apps.extend.chat_manager.models.chat_state_model import ChatStateInfo
from apps.extend.utils.utils import Utils
from common.core.deps import SessionDep
from common.utils.utils import SQLBotLogUtil


class ChatService:
    """
    聊天服务 - 字段提取业务逻辑
    
    职责：
    1. 使用 LLM 从用户问题中提取指标、维度和过滤条件
    2. 当问题中无法提取时，自动从聊天历史中补充
    3. 提供统一的字段提取接口 extract_fields_from_question
    """
    
    def __init__(self, llm):
        """
        初始化服务
        
        Args:
            llm: 大模型客户端实例
        """
        self.llm = llm
        self.state_service = ChatStateService(llm=llm)
    
    def extract_metrics_and_dimensions(self, question: str) -> Dict[str, Any]:
        """
        从用户问题中提取指标、维度和过滤条件
        
        Args:
            question: 用户问题
        
        Returns:
            包含以下字段的字典：
            - metrics: List[str] 指标列表
            - dimensions: List[str] 维度列表
            - filters: List[str] 过滤条件列表（如：["地区=北京", "时间=最近一个月"]）
            - need_chat_state: bool 是否需要查询 chat_state（当提取不到指标维度时为 True）
        """
        # 构建提示词：让 LLM 提取指标、维度和过滤条件
        prompt = f"""# 角色
你是数据字段提取专家。

# 任务
从用户问题中提取所有提到的指标名称（字段名）、维度名称和过滤条件。

# 用户问题
{question}

# 提取规则
## 1. 识别指标字段
- 提取用户提到的具体指标名称（中文或英文）
- 保留原始名称（用户说中文就输出中文，说英文就输出英文）
- **常见业务指标**：销售额、销售量、订单量、利润、收入、成本、数量、金额、单价、均价、比率、占比、增长率、完成率等

## 2. 识别维度字段
- **时间维度**：年、月、日、季度、年度、月度、周、星期、日期、时间等
- **组织维度**：部门、地区、区域、分公司、门店、工厂、车间、班组等
- **分类维度**：类别、品类、产品线、渠道、品牌、系列等
- **其他维度**：性别、年龄段、会员等级、客户类型、产品等级等
- **注意**："按 X"结构中的 X 通常是维度，如"按天"的"天"、"按部门"的"部门"

## 3. 识别过滤条件
- **时间范围**：最近一个月、今年、上周、2024 年、近 7 天等
- **地区限制**：北京、上海、广东省、华东区等
- **部门/组织**：销售部、技术部、北京分公司等
- **数值范围**：大于 100、小于 50、在 10 到 20 之间等
- **状态条件**：已付款、已完成、有效客户等
- **其他筛选**：男性、VIP 客户、产品线 A 等
- **输出格式**：将过滤条件整理为字符串列表，每个元素是一个完整的过滤描述

## 4. 排除非指标词汇（这些词不要作为指标或维度）
- **动词类**：查询、统计、汇总、分析、查看、展示、显示、下钻、钻取、计算、求、得到
- **模糊名词类**：数据、明细、记录、信息、情况、结果、内容、资料
- **连接词类**：按、的、和、与、及、等、还有、以及
- **泛指类**：所有、全部、每个、各个、哪些、什么、如何、怎么

# 输出格式
严格按照以下 JSON 格式输出:
{{
    "metrics": ["指标 1", "指标 2", ...],
    "dimensions": ["维度 1", "维度 2", ...],
    "filters": ["过滤条件 1", "过滤条件 2", ...]
}}

# 示例
## 示例 1 - 有指标、维度和过滤条件
输入："按月下钻北京和上海地区的销售额和销售量，最近一个月的数据"
输出：{{"metrics": ["销售额", "销售量"], "dimensions": ["月", "地区"], "filters": ["地区=北京", "地区=上海", "时间=最近一个月"]}}

## 示例 2 - 只有指标和过滤条件
输入："查询 2024 年销售部的订单量和收入"
输出：{{"metrics": ["订单量", "收入"], "dimensions": [], "filters": ["年份=2024 年", "部门=销售部"]}}

## 示例 3 - 有多个维度和过滤条件
输入："按部门和地区统计员工人数，只看大于 100 人的部门"
输出：{{"metrics": ["员工人数"], "dimensions": ["部门", "地区"], "filters": ["员工人数>100"]}}

## 示例 4 - 什么都提取不到
输入："你好"
输出：{{"metrics": [], "dimensions": [], "filters": []}}

请根据上述规则，从用户问题中提取指标、维度和过滤条件。保留用户使用的原始名称（中文或英文）。"""

        try:
            response = self.llm.invoke([HumanMessage(content=prompt)])
            response_text = response.content if hasattr(response, 'content') else str(response)
            SQLBotLogUtil.info(f"指标维度提取响应：{response_text}")
            
            result = json.loads(response_text, strict=False)
            metrics = result.get("metrics", [])
            dimensions = result.get("dimensions", [])
            filters = result.get("filters", [])
            
            # 确保列表类型
            if not isinstance(metrics, list):
                metrics = []
            if not isinstance(dimensions, list):
                dimensions = []
            if not isinstance(filters, list):
                filters = []
            
            # 判断是否需要查询 chat_state
            need_chat_state = False
            if len(metrics) == 0 or len(dimensions) == 0:
                need_chat_state = True
            
            SQLBotLogUtil.info(f"提取结果：metrics={metrics}, dimensions={dimensions}, filters={filters}, need_chat_state={need_chat_state}")
            return {
                'metrics': metrics,
                'dimensions': dimensions,
                'filters': filters,
                'need_chat_state': need_chat_state
            }
            
        except Exception as e:
            SQLBotLogUtil.error(f"LLM 提取指标维度失败：{e}，返回空列表")
            return {
                'metrics': [],
                'dimensions': [],
                'filters': [],
                'need_chat_state': True
            }
    
    def merge_metrics_and_dimensions_from_history(
        self,
        session: SessionDep,
        extracted_result: Dict[str, Any],
        chat_id: int,
        datasource_id: int = None,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        根据 extract_metrics_and_dimensions 的返回值，决定是否需要从聊天历史中补充指标和维度
        
        Args:
            session: 数据库会话
            extracted_result: extract_metrics_and_dimensions 返回的结果字典
            chat_id: 聊天 ID
            datasource_id: 数据源 ID（可选）
            limit: 查询历史记录数量限制
        
        Returns:
            合并后的结果字典，包含：
            - metrics: List[str] 合并后的指标列表
            - dimensions: List[str] 合并后的维度列表
            - filters: List[str] 过滤条件列表
            - sql_generation_hint: str SQL 生成提示（当同时有维度和指标时，提示必须使用 GROUP BY 和聚合函数）
        """
        metrics = extracted_result.get('metrics', [])
        dimensions = extracted_result.get('dimensions', [])
        filters = extracted_result.get('filters', [])
        need_chat_state = extracted_result.get('need_chat_state', False)
        
        result = {
            'metrics': metrics.copy(),
            'dimensions': dimensions.copy(),
            'filters': filters.copy()
        }
                
        # 如果需要查询 chat_state
        if need_chat_state:
            SQLBotLogUtil.info("当前问题未提取到指标维度,开始从聊天历史中获取...")
                    
            # 查询聊天历史状态
            chat_state = self.state_service.extract_chat_state_from_history(
                session=session,
                chat_id=chat_id,
                datasource_id=datasource_id,
                limit=limit
            )
        
            SQLBotLogUtil.info(f"从历史中获取的 chat_state:{chat_state}")
        
            if chat_state and chat_state.metrics:
                SQLBotLogUtil.info(f"从历史中提取到指标:{chat_state.metrics}")
                        
                # 如果没有提取到指标,直接使用历史指标
                if not result['metrics']:
                    result['metrics'].append(chat_state.metrics)
                        
                # 如果有维度信息,也一并添加
                if hasattr(chat_state, 'dimensions') and chat_state.dimensions:
                    result['dimensions'].extend(chat_state.dimensions)
        
                if hasattr(chat_state, 'filters') and chat_state.filters:
                    result['filters'].extend(chat_state.filters)
                        
                SQLBotLogUtil.info(f"合并后的指标:{result['metrics']}, 维度:{result['dimensions']}")
            else:
                SQLBotLogUtil.warning("未能从聊天历史中提取到有效数据")
        else:
            SQLBotLogUtil.info(f"当前问题已提取到指标:{metrics}, 维度:{dimensions},无需查询历史")
                
        # 添加 SQL 生成提示:当有维度和指标时,必须使用 GROUP BY 和聚合函数
        if result['dimensions'] and result['metrics']:
            result['sql_generation_hint'] = '生成SQL描述:必须要使用group by 分组维度字段,指标计算必须要使用聚合函数'
        else:
            result['sql_generation_hint'] = ''
                
        return result
    
    def extract_metric_and_dim_from_question(
        self,
        session: SessionDep,
        question: str,
        chat_id: int,
        datasource_id: int = None,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        从用户问题中提取指标、维度和过滤条件（自动处理历史数据补充）
        
        功能：
        1. 调用 extract_metrics_and_dimensions 提取当前问题的指标、维度和过滤条件
        2. 如果提取不到，自动调用 merge_metrics_and_dimensions_from_history 从聊天历史中补充
        3. 返回完整的指标、维度和过滤条件
        
        Args:
            session: 数据库会话
            question: 用户问题
            chat_id: 聊天 ID
            datasource_id: 数据源 ID（可选）
            limit: 查询历史记录数量限制
        
        Returns:
            包含以下字段的字典：
            - metrics: List[str] 指标列表（当前提取 + 历史补充）
            - dimensions: List[str] 维度列表（当前提取 + 历史补充）
            - filters: List[str] 过滤条件列表
            - sql_generation_hint: str SQL 生成提示（当同时有维度和指标时，提示必须使用 GROUP BY 和聚合函数）
        """
        # 步骤 1：从当前问题中提取指标、维度和过滤条件
        extracted_result = self.extract_metrics_and_dimensions(question)
        
        # 步骤 2：如果需要，从历史中补充
        merged_result = self.merge_metrics_and_dimensions_from_history(
            session=session,
            extracted_result=extracted_result,
            chat_id=chat_id,
            datasource_id=datasource_id,
            limit=limit
        )
        
        SQLBotLogUtil.info(f"最终结果：metrics={merged_result['metrics']}, dimensions={merged_result['dimensions']}, filters={merged_result['filters']}")
        return merged_result


if __name__ == '__main__':
    llm = Utils.create_llm_client()
    session = Utils.create_local_session()
    chat_service = ChatService(llm=llm)
    result = chat_service.extract_metric_and_dim_from_question(session, "按天查询最近一个月的明细数据", 32)
    print(f"提取结果：{str(result)}")
