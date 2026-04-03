from typing import List, Optional

from fastapi import APIRouter, Query

from apps.extend.chat_manager.curd.chat_state import (
    create_chat_state,
    update_chat_state,
    get_latest_chat_state_by_chat_id,
    get_chat_state_history,
    delete_chat_state,
    clear_chat_state_by_chat_id
)
from apps.extend.chat_manager.models.chat_state_model import ChatStateInfo
from common.core.deps import SessionDep

router = APIRouter(tags=["Chat Manager"], prefix="/extend/chat-manager")


@router.get("/state/{chat_id}")
async def get_chat_state(
    session: SessionDep,
    chat_id: int
):
    """
    获取指定会话的最新状态
    
    Args:
        chat_id: 聊天 ID
    
    Returns:
        会话状态信息
    """
    result = get_latest_chat_state_by_chat_id(session, chat_id)
    
    if not result:
        return {
            "success": False,
            "message": "该会话暂无状态记录",
            "data": None
        }
    
    return {
        "success": True,
        "data": result
    }


@router.post("/state")
async def create_or_update_chat_state(
    session: SessionDep,
    info: ChatStateInfo
):
    """
    创建或更新会话状态
    
    Args:
        info: 会话状态信息对象
    
    Returns:
        操作结果
    """
    try:
        update_chat_state(session, info)
        return {
            "success": True,
            "chat_id": info.chat_id,
            "action": "update" if get_latest_chat_state_by_chat_id(session, info.chat_id) else "create"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@router.get("/state/history/{chat_id}")
async def get_state_history(
    session: SessionDep,
    chat_id: int,
    limit: int = Query(default=10, description="返回记录数量限制")
):
    """
    获取会话状态历史（用于上下文回溯）
    
    Args:
        chat_id: 聊天 ID
        limit: 返回记录数量限制
    
    Returns:
        会话状态历史列表
    """
    history = get_chat_state_history(session, chat_id, limit)
    
    return {
        "success": True,
        "data": history,
        "count": len(history)
    }


@router.delete("/state/{chat_id}")
async def clear_chat_state(
    session: SessionDep,
    chat_id: int
):
    """
    清空会话的所有状态记录（重置对话）
    
    Args:
        chat_id: 聊天 ID
    
    Returns:
        操作结果
    """
    try:
        clear_chat_state_by_chat_id(session, chat_id)
        return {
            "success": True,
            "message": f"已清空会话 {chat_id} 的所有状态记录"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@router.delete("/state/by-ids")
async def delete_states_by_ids(
    session: SessionDep,
    chat_ids: List[int]
):
    """
    根据聊天 ID 列表删除会话状态记录
    
    Args:
        chat_ids: 要删除的聊天 ID 列表
    
    Returns:
        操作结果
    """
    try:
        delete_chat_state(session, chat_ids)
        return {
            "success": True,
            "deleted_count": len(chat_ids)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


# ========== 本地测试接口 ==========

@router.get("/test/sample-data")
async def test_sample_data():
    """
    【本地测试】返回示例会话状态数据
    
    Returns:
        示例数据
    """
    sample_data = {
        "chat_id": 123456,
        "metrics": {
            "sales": {"name": "销售额", "column": "amount"},
            "order_count": {"name": "订单量", "column": "order_id"}
        },
        "dimensions": {
            "date": {"name": "日期", "column": "order_date"},
            "region": {"name": "地区", "column": "region_name"}
        },
        "filters": {
            "date_range": {"start": "2024-01-01", "end": "2024-12-31"},
            "region": ["北京", "上海"]
        },
        "tables": ["orders", "users", "regions"],
        "resolved_names": {
            "卖的钱": "amount",
            "下单数": "order_id",
            "时间": "order_date"
        },
        "context": {
            "last_question": "北京的销售额是多少？",
            "topic": "sales_analysis"
        }
    }
    
    return {
        "sample_data": sample_data,
        "description": "这是可以用于测试的示例会话状态数据"
    }


@router.post("/test/create-sample")
async def test_create_sample(session: SessionDep):
    """
    【本地测试】创建示例会话状态
    
    Returns:
        创建结果
    """
    sample_info = ChatStateInfo(
        chat_id=123456,
        metrics={
            "sales": {"name": "销售额", "column": "amount"},
            "order_count": {"name": "订单量", "column": "order_id"}
        },
        dimensions={
            "date": {"name": "日期", "column": "order_date"},
            "region": {"name": "地区", "column": "region_name"}
        },
        filters={
            "date_range": {"start": "2024-01-01", "end": "2024-12-31"},
            "region": ["北京", "上海"]
        },
        tables=["orders", "users", "regions"],
        resolved_names={
            "卖的钱": "amount",
            "下单数": "order_id",
            "时间": "order_date"
        },
        context={
            "last_question": "北京的销售额是多少？",
            "topic": "sales_analysis"
        }
    )
    
    try:
        update_chat_state(session, sample_info)
        return {
            "success": True,
            "chat_id": sample_info.chat_id,
            "message": "成功创建示例会话状态"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "traceback": __import__('traceback').format_exc()
        }


@router.get("/test/query/{chat_id}")
async def test_query(session: SessionDep, chat_id: int = 123456):
    """
    【本地测试】查询会话状态
    
    Args:
        chat_id: 聊天 ID
    
    Returns:
        查询结果
    """
    try:
        result = get_latest_chat_state_by_chat_id(session, chat_id)
        return {
            "success": True,
            "data": result if result else None,
            "message": "查询成功" if result else "未找到会话状态"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@router.post("/test/update/{chat_id}")
async def test_update(session: SessionDep, chat_id: int):
    """
    【本地测试】更新会话状态（添加新指标）
    
    Args:
        chat_id: 聊天 ID
    
    Returns:
        更新结果
    """
    # 先查询现有状态
    existing = get_latest_chat_state_by_chat_id(session, chat_id)
    
    if not existing:
        return {
            "success": False,
            "message": "会话不存在，请先创建"
        }
    
    # 更新指标
    update_info = ChatStateInfo(
        chat_id=chat_id,
        metrics={
            "profit": {"name": "利润", "column": "profit_amount"}  # 新增利润指标
        },
        dimensions=existing.dimensions,
        filters=existing.filters,
        tables=existing.tables,
        resolved_names=existing.resolved_names,
        context=existing.context
    )
    
    try:
        update_chat_state(session, update_info)
        return {
            "success": True,
            "chat_id": chat_id,
            "message": "成功更新会话状态（添加利润指标）"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@router.delete("/test/clear/{chat_id}")
async def test_clear(session: SessionDep, chat_id: int):
    """
    【本地测试】清空会话状态（危险操作！）
    
    Args:
        chat_id: 聊天 ID
    
    Returns:
        清空结果
    """
    try:
        clear_chat_state_by_chat_id(session, chat_id)
        return {
            "success": True,
            "message": f"已清空会话 {chat_id} 的所有状态记录"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
