"""
Chat Manager - 会话状态管理模块

提供多轮对话的上下文管理能力，包括：
1. 会话状态的创建、更新、查询
2. 指标、维度、过滤条件的维护
3. 用户术语与字段名的映射管理
4. 话题切换检测与处理
5. 基于 LLM 的历史对话状态提取
6. 用户意图分析与问题重构
"""

from apps.extend.chat_manager.models.chat_state_model import ChatState, ChatStateInfo
from apps.extend.chat_manager.curd.chat_state import (
    create_chat_state,
    update_chat_state,
    get_latest_chat_state_by_chat_id,
    get_chat_state_history,
    delete_chat_state,
    clear_chat_state_by_chat_id
)
from apps.extend.chat_manager.services.chat_state_service import ChatStateService
from apps.extend.chat_manager.services.chat_service import ChatService

__all__ = [
    # 数据模型
    "ChatState",
    "ChatStateInfo",
    
    # CRUD 操作
    "create_chat_state",
    "update_chat_state",
    "get_latest_chat_state_by_chat_id",
    "get_chat_state_history",
    "delete_chat_state",
    "clear_chat_state_by_chat_id",
    
    # 服务类
    "ChatStateService",      # 聊天状态管理（专注于 state 的 CRUD）
    "ChatService"           # 聊天业务逻辑编排
]