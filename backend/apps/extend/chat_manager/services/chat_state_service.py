"""
ChatStateService - 会话状态管理服务

专注于 session_state 的 CRUD 操作：
1. 从历史对话中提取会话状态
2. 初始化状态到数据库
3. 管理 state 的生命周期
"""

from typing import List, Optional, Dict, Any

from apps.ai_model.model_factory import LLMFactory, LLMConfig
from langchain.chat_models.base import BaseChatModel
from langchain_core.messages import HumanMessage
import json

from apps.chat.curd.chat import get_recent_records_by_chat_id
from apps.extend.chat_manager.curd.chat_state import update_chat_state
from apps.extend.chat_manager.models.chat_state_model import ChatStateInfo
from apps.extend.utils.utils import Utils
from common.core.deps import SessionDep
from common.utils.utils import SQLBotLogUtil


class ChatStateService:
    """
    会话状态管理服务
    
    职责：
    1. 从历史对话记录中提取会话并初始化状态
    2. 调用 CRUD 将提取的状态写入数据库
    3. 管理 session_state 的生命周期
    """
    
    def __init__(self, llm):
        """
        初始化服务
        
        Args:
            llm: 大模型客户端实例（可选）
        """
        self.llm = llm
    
    def extract_chat_state_from_history(
        self, 
        session: SessionDep, 
        chat_id: int, 
        datasource_id: int = None,
        limit: int = 10
    ) -> Optional[ChatStateInfo]:
        """
        从历史对话记录中提取聊天状态
        
        功能：
        1. 调用 get_recent_records_by_chat_id 获取最近 N 条对话记录
        2. 使用 LLM 分析对话内容，提取指标、维度、过滤条件等信息
        3. 构建并返回 ChatStateInfo 对象
        
        Args:
            session: 数据库会话
            chat_id: 聊天 ID
            datasource_id: 数据源 ID（可选，用于过滤）
            limit: 查询历史记录数量限制
        
        Returns:
            提取的聊天状态信息对象，如果无法提取则返回 None
        """
        if not self.llm:
            SQLBotLogUtil.error("LLM 客户端未初始化，无法提取聊天状态")
            return None
        
        # ========== 步骤 1：获取历史对话记录 ==========
        records = get_recent_records_by_chat_id(
            session=session, 
            chat_id=chat_id, 
            datasource_id=datasource_id,
            limit=limit
        )
        
        if not records or len(records) == 0:
            SQLBotLogUtil.info(f"未找到 chat_id={chat_id} 的历史对话记录")
            return None

        
        # ========== 步骤 2：构建对话上下文文本 ==========
        conversation_context = self._build_conversation_context(records)

        if not conversation_context.strip():
            SQLBotLogUtil.info(f"对话记录中没有有效内容，无法提取聊天状态")
            return None
        
        # ========== 步骤 3：使用 LLM 提取聊天状态 ==========
        session_state_data = self._extract_state_with_llm(conversation_context)
        
        if not session_state_data:
            SQLBotLogUtil.info("LLM 未能从对话中提取有效的聊天状态")
            return None
        
        # ========== 步骤 4：构建并返回 ChatStateInfo 对象 ==========
        chat_state_info = ChatStateInfo(
            chat_id=chat_id,
            metrics=session_state_data.get('metrics'),
            dimensions=session_state_data.get('dimensions'),
            filters=session_state_data.get('filters'),
            tables=session_state_data.get('tables', []),
            resolved_names=session_state_data.get('resolved_names'),
            context=session_state_data.get('context')
        )
        
        SQLBotLogUtil.info(f"成功从历史对话中提取聊天状态：chat_id={chat_id}")
        return chat_state_info
    
    def initialize_chat_state_from_history(
        self, 
        session: SessionDep, 
        chat_id: int, 
        datasource_id: int = None,
        limit: int = 10
    ) -> Optional[int]:
        """
        从历史对话初始化会话状态并写入数据库
        
        功能：
        1. 调用 extract_session_state_from_history 提取状态
        2. 调用 update_session_state 将状态写入数据库
        
        Args:
            session: 数据库会话
            chat_id: 聊天 ID
            datasource_id: 数据源 ID（可选）
            limit: 查询历史记录数量限制
        
        Returns:
            创建的记录 ID，如果失败则返回 None
        """
        # 提取会话状态
        session_state_info = self.extract_chat_state_from_history(
            session=session,
            chat_id=chat_id,
            datasource_id=datasource_id,
            limit=limit
        )


        if not session_state_info:
            SQLBotLogUtil.error(f"未能从历史对话中提取会话状态：chat_id={chat_id}")
            return None

        print(f"session_state_info:{session_state_info}")
        # 写入数据库
        try:
            update_chat_state(session, session_state_info)
            SQLBotLogUtil.info(f"成功初始化会话状态到数据库：chat_id={chat_id}")
            return session_state_info.chat_id
        except Exception as e:
            SQLBotLogUtil.error(f"写入会话状态失败：{e}")
            return None
    
    def _build_conversation_context(self, records: List) -> str:
        """
        构建对话上下文字符串（仅使用用户问题，避免 AI 回答的误导）
        
        Args:
            records: 对话记录列表
        
        Returns:
            格式化后的对话上下文字符串
        """
        context_parts = []
        
        for i, record in enumerate(records, 1):
            # 只提取用户问题（AI 回答可能包含错误信息，会误导）
            question = getattr(record, 'question', None)
            
            if not question:
                continue
            
            # 构建单轮对话（只有用户问题）
            dialogue = f"【第{i}轮】用户问题：{question}\n"
            context_parts.append(dialogue)
        
        return ''.join(context_parts)
    
    def _extract_state_with_llm(self, conversation_context: str) -> Optional[Dict[str, Any]]:
        """
        使用 LLM 从对话上下文中提取会话状态
        
        Args:
            conversation_context: 格式化后的对话上下文字符串
        
        Returns:
            提取的会话状态数据字典，如果失败则返回 None
        """
        prompt = """# 角色
你是数据结构化分析专家。

# 任务
从对话历史中提取关键信息，构建会话状态（session_state）。

# 对话历史
""" + conversation_context + """

# 提取内容
## 1. metrics（指标信息）
- 从对话中提取最新提到的一个指标字段
- 只提取指标的中文名称（字符串）
- 如果提到多个指标，取最后一个（最新的）
- 格式："中文名"

## 2. dimensions（维度信息）
- 提取所有用到的维度字段
- 包括时间维度（年、月、日）、地理维度（省份、城市）、业务维度（部门、产品）等
- 只提取维度的中文名称（字符串数组）
- 格式：["维度名 1", "维度名 2"]

## 3. filters（过滤条件）
- 提取所有的筛选条件
- 包括时间范围、地区限制、类别筛选等
- 格式：["字段名=筛选值", "字段名=筛选值"]

## 4. tables（涉及的表）
- 从 SQL 中提取表名列表
- 只提取主表和关联表

## 5. resolved_names（名称映射）
- 用户使用的术语与数据库字段名的映射关系
- 格式：{"用户术语": "数据库字段名"}

## 6. context（其他上下文信息）
- 排序规则
- 分组方式
- 聚合函数
- 其他重要上下文

# 输出格式
严格按照以下 JSON 格式输出:
{
    "metrics": [],
    "dimensions": [],
    "filters": [],
    "tables": [],
    "resolved_names": {},
    "context": {}
}

# 示例
## 示例输入
对话历史：
用户问题：查询北京和上海两地的销售额
SQL 回答：SELECT region, SUM(sales_amount) FROM sales_fact WHERE region IN ('北京', '上海') GROUP BY region

## 示例输出
{
    "metrics": "销售额",
    "dimensions": "地区",
    "filters": [
        "region=北京",
        "region=上海"
    ],
    "tables": ["sales_fact"],
    "resolved_names": {
        "销售额": "sales_amount",
        "地区": "region"
    },
    "context": {
        "group_by": ["region"],
        "aggregate_functions": ["SUM"]
    }
}

请根据对话历史，提取会话状态信息。如果某些内容为空，可以留空但不要编造。"""

        try:
            response = self.llm.invoke([HumanMessage(content=prompt)])
            response_text = response.content if hasattr(response, 'content') else str(response)
            # SQLBotLogUtil.info(f"会话状态提取响应：{response_text}")
            
            # 尝试从响应中提取 JSON（处理可能的 markdown 格式）
            json_str = response_text.strip()
            
            # 如果响应包含 markdown 代码块，提取 JSON 部分
            if "```json" in json_str:
                import re
                match = re.search(r'```json\s*(.+?)\s*```', json_str, re.DOTALL)
                if match:
                    json_str = match.group(1)
                    SQLBotLogUtil.info("已从 markdown 代码块中提取 JSON")
            elif "```" in json_str:
                import re
                match = re.search(r'```\s*(.+?)\s*```', json_str, re.DOTALL)
                if match:
                    json_str = match.group(1)
                    SQLBotLogUtil.info("已从通用代码块中提取 JSON")
            
            # 解析 JSON 结果
            result = json.loads(json_str, strict=False)
            
            # 验证必要字段
            if not isinstance(result, dict):
                SQLBotLogUtil.error("LLM 返回的不是有效的 JSON 对象")
                return None
            
            SQLBotLogUtil.info("成功解析会话状态数据")
            return result
            
        except Exception as e:
            SQLBotLogUtil.error(f"解析 LLM 响应失败：{e}")
            SQLBotLogUtil.error(f"原始响应内容：{response_text[:500] if 'response_text' in locals() else 'N/A'}")
            return None


if __name__ == '__main__':
    llm = Utils.create_llm_client()
    state_service = ChatStateService(llm)
    session = Utils.create_local_session()
    state_service.initialize_chat_state_from_history(session, 32)
