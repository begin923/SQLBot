import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

from sqlalchemy.orm import Session
from sqlalchemy import and_, select, func, update, text

from apps.extend.chat_manager.models.chat_state_model import ChatState, ChatStateInfo
from common.core.deps import SessionDep


def create_chat_state(session: SessionDep, info: ChatStateInfo) -> None:
    """
    创建聊天状态记录
    
    Args:
        session: 数据库会话
        info: 会话状态信息对象
    """
    if not info.chat_id:
        raise Exception("chat_id 不能为空")
    
    current_time = datetime.datetime.now()
    
    # 检查是否已存在（同一个 chat_id 只保留最新的一条）
    existing = get_latest_chat_state_by_chat_id(session, info.chat_id)
    
    if existing:
        # 如果已存在，则更新现有记录
        return update_chat_state(session, info)
    
    # 创建新记录
    state = ChatState(
        chat_id=info.chat_id,
        metrics=info.metrics,
        dimensions=info.dimensions,
        filters=info.filters,
        tables=info.tables,
        resolved_names=info.resolved_names,
        context=info.context,
        create_time=current_time,
        update_time=current_time
    )
    
    session.add(state)
    session.commit()
    session.refresh(state)


def update_chat_state(session: SessionDep, info: ChatStateInfo) -> None:
    """
    更新聊天状态记录（基于 chat_id 更新最新的一条）
    
    Args:
        session: 数据库会话
        info: 会话状态信息对象
    """
    if not info.chat_id:
        raise Exception("chat_id 不能为空")
    
    current_time = datetime.datetime.now()
    
    # 直接删除旧记录（不需要先查询检查）
    delete_stmt = text(f"DELETE FROM {ChatState.__tablename__} WHERE chat_id = :chat_id")
    session.execute(delete_stmt, {"chat_id": info.chat_id})
    session.commit()
    
    # 创建新记录（总是使用 insert）
    state = ChatState(
        chat_id=info.chat_id,
        metrics=info.metrics,
        dimensions=info.dimensions,
        filters=info.filters,
        tables=info.tables,
        resolved_names=info.resolved_names,
        context=info.context,
        create_time=current_time,
        update_time=current_time
    )
    
    session.add(state)
    session.commit()
    session.refresh(state)


def get_latest_chat_state_by_chat_id(session: SessionDep, chat_id: int) -> Optional[ChatStateInfo]:
    """
    根据 chat_id 查询最新的聊天状态
    
    Args:
        session: 数据库会话
        chat_id: 聊天 ID
    
    Returns:
        最新的会话状态信息对象
    """
    # 查询最新的记录（按 update_time 降序）
    state = session.query(ChatState).filter(
        ChatState.chat_id == chat_id
    ).order_by(ChatState.update_time.desc()).first()
    
    if not state:
        return None
    
    return _convert_to_info(state)


def get_chat_state_history(session: SessionDep, chat_id: int, limit: int = 10) -> List[ChatStateInfo]:
    """
    查询聊天状态历史（用于上下文回溯）
    
    Args:
        session: 数据库会话
        chat_id: 聊天 ID
        limit: 返回记录数量限制
    
    Returns:
        会话状态历史列表（按 update_time 降序）
    """
    states = session.query(ChatState).filter(
        ChatState.chat_id == chat_id
    ).order_by(ChatState.update_time.desc()).limit(limit).all()
    
    return [_convert_to_info(state) for state in states]


def delete_chat_state(session: SessionDep, chat_ids: List[int]):
    """
    删除聊天状态记录
    
    Args:
        session: 数据库会话
        chat_ids: 要删除的聊天 ID 列表
    """
    from sqlalchemy import delete as sql_delete
    stmt = sql_delete(ChatState).where(ChatState.chat_id.in_(chat_ids))
    session.execute(stmt)
    session.commit()


def clear_chat_state_by_chat_id(session: SessionDep, chat_id: int):
    """
    清空某个聊天的所有状态记录（用于重置对话）
    
    Args:
        session: 数据库会话
        chat_id: 聊天 ID
    """
    from sqlalchemy import delete as sql_delete
    stmt = sql_delete(ChatState).where(ChatState.chat_id == chat_id)
    session.execute(stmt)
    session.commit()


def _merge_json_data(existing: Optional[Dict[str, Any]], new: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    合并两个 JSON 字典（新数据覆盖旧数据）
    
    Args:
        existing: 现有数据
        new: 新数据
    
    Returns:
        合并后的数据
    """
    if not existing:
        return new
    if not new:
        return existing
    
    merged = existing.copy()
    merged.update(new)
    return merged


def _merge_lists(existing: Optional[List[str]], new: Optional[List[str]]) -> Optional[List[str]]:
    """
    合并两个列表（去重）
    
    Args:
        existing: 现有列表
        new: 新列表
    
    Returns:
        合并后的列表
    """
    if not existing:
        return new
    if not new:
        return existing
    
    # 合并并去重，保持顺序
    merged = list(existing)
    for item in new:
        if item not in merged:
            merged.append(item)
    
    return merged


def _convert_to_info(state: ChatState) -> ChatStateInfo:
    """
    将 ChatState 对象转换为 ChatStateInfo 对象
    
    Args:
        state: ChatState 对象
    
    Returns:
        ChatStateInfo 对象
    """
    return ChatStateInfo(
        chat_id=state.chat_id,
        metrics=state.metrics,
        dimensions=state.dimensions,
        filters=state.filters,
        tables=state.tables,
        resolved_names=state.resolved_names,
        context=state.context,
        create_time=state.create_time,
        update_time=state.update_time
    )
