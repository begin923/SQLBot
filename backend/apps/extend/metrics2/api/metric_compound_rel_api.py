from typing import List, Optional
from fastapi import APIRouter, Query

from apps.extend.metrics2.crud.metric_compound_rel_curd import (
    create_metric_compound_rel,
    batch_create_metric_compound_rel,
    update_metric_compound_rel,
    delete_metric_compound_rel,
    get_metric_compound_rel_by_id,
    get_metric_compound_rel_by_metric_id,
    page_metric_compound_rel,
    get_all_metric_compound_rel
)
from apps.extend.metrics2.models.metric_compound_rel_model import MetricCompoundRelInfo

router = APIRouter(tags=["Metric Compound Relation"], prefix="/extend/metric-compound-rel")


@router.get("/page/{current_page}/{page_size}")
async def page_relations(
    current_page: int,
    page_size: int,
    metric_id: Optional[str] = Query(None, description="复合指标ID（可选）")
):
    """
    分页查询复合指标子指标关联

    Returns:
        分页结果
    """
    current_page, page_size, total_count, total_pages, _list = page_metric_compound_rel(
        current_page, page_size, metric_id
    )

    return {
        "current_page": current_page,
        "page_size": page_size,
        "total_count": total_count,
        "total_pages": total_pages,
        "data": _list
    }


@router.get("/list")
async def list_relations(
    metric_id: Optional[str] = Query(None, description="复合指标ID（可选）")
):
    """
    获取所有复合指标子指标关联（不分页）

    Returns:
        复合指标子指标关联列表
    """
    _list = get_all_metric_compound_rel(metric_id)
    return {"data": _list}


@router.get("/metric/{metric_id}")
async def get_relations_by_metric(metric_id: str):
    """
    根据复合指标ID查询所有子指标关联

    Returns:
        复合指标子指标关联列表
    """
    _list = get_metric_compound_rel_by_metric_id(metric_id)
    return {"data": _list}


@router.get("/{id}")
async def get_relation(id: int):
    """
    根据ID查询复合指标子指标关联

    Returns:
        复合指标子指标关联详情
    """
    result = get_metric_compound_rel_by_id(id)
    if not result:
        return {"success": False, "message": "复合指标子指标关联不存在"}
    return {"success": True, "data": result}


@router.put("")
async def create_or_update(info: MetricCompoundRelInfo):
    """
    创建或更新复合指标子指标关联

    Args:
        info: 复合指标子指标关联信息对象

    Returns:
        创建的 ID 或更新的 ID
    """
    if info.id:
        # 更新
        rel_id = update_metric_compound_rel(info)
        return {"success": True, "id": rel_id, "action": "update"}
    else:
        # 创建
        rel_id = create_metric_compound_rel(info)
        return {"success": True, "id": rel_id, "action": "create"}


@router.post("/batch")
async def batch_create(info_list: List[MetricCompoundRelInfo]):
    """
    批量创建复合指标子指标关联

    Args:
        info_list: 复合指标子指标关联列表

    Returns:
        处理结果统计
    """
    result = batch_create_metric_compound_rel(info_list)
    return result


@router.delete("")
async def delete(ids: List[int]):
    """
    删除复合指标子指标关联

    Args:
        ids: 要删除的记录ID列表

    Returns:
        删除结果
    """
    delete_metric_compound_rel(ids)
    return {"success": True, "deleted_count": len(ids)}