from typing import List, Optional
from fastapi import APIRouter, Query

from apps.extend.metrics2.crud.metric_definition_curd import (
    create_metric_definition,
    batch_create_metric_definition,
    update_metric_definition,
    delete_metric_definition,
    get_metric_definition_by_id,
    get_metric_definition_by_code,
    page_metric_definition,
    get_all_metric_definition
)
from apps.extend.metrics2.models.metric_definition_model import MetricDefinitionInfo

router = APIRouter(tags=["Metric Definition"], prefix="/extend/metric-definition")


@router.get("/page/{current_page}/{page_size}")
async def page_metrics(
    current_page: int,
    page_size: int,
    metric_name: Optional[str] = Query(None, description="指标名称（支持模糊查询）"),
    metric_code: Optional[str] = Query(None, description="指标编码（支持模糊查询）"),
    metric_type: Optional[str] = Query(None, description="指标类型"),
    biz_domain: Optional[str] = Query(None, description="业务域")
):
    """
    分页查询指标定义

    Returns:
        分页结果
    """
    current_page, page_size, total_count, total_pages, _list = page_metric_definition(
        current_page, page_size, metric_name, metric_code, metric_type, biz_domain
    )

    return {
        "current_page": current_page,
        "page_size": page_size,
        "total_count": total_count,
        "total_pages": total_pages,
        "data": _list
    }


@router.get("/list")
async def list_metrics(
    metric_name: Optional[str] = Query(None, description="指标名称（支持模糊查询）"),
    metric_code: Optional[str] = Query(None, description="指标编码（支持模糊查询）"),
    metric_type: Optional[str] = Query(None, description="指标类型"),
    biz_domain: Optional[str] = Query(None, description="业务域")
):
    """
    获取所有指标定义（不分页）

    Returns:
        指标列表
    """
    _list = get_all_metric_definition(metric_name, metric_code, metric_type, biz_domain)
    return {"data": _list}


@router.get("/{metric_id}")
async def get_metric(metric_id: str):
    """
    根据ID查询指标定义

    Returns:
        指标详情
    """
    result = get_metric_definition_by_id(metric_id)
    if not result:
        return {"success": False, "message": "指标不存在"}
    return {"success": True, "data": result}


@router.get("/code/{metric_code}")
async def get_metric_by_code(metric_code: str):
    """
    根据编码查询指标定义

    Returns:
        指标详情
    """
    result = get_metric_definition_by_code(metric_code)
    if not result:
        return {"success": False, "message": "指标不存在"}
    return {"success": True, "data": result}


@router.put("")
async def create_or_update(info: MetricDefinitionInfo):
    """
    创建或更新指标定义

    Args:
        info: 指标定义信息对象

    Returns:
        创建的 ID 或更新的 ID
    """
    if info.metric_id:
        # 更新
        metric_id = update_metric_definition(info)
        return {"success": True, "id": metric_id, "action": "update"}
    else:
        # 创建
        metric_id = create_metric_definition(info)
        return {"success": True, "id": metric_id, "action": "create"}


@router.post("/batch")
async def batch_create(info_list: List[MetricDefinitionInfo]):
    """
    批量创建指标定义

    Args:
        info_list: 指标定义列表

    Returns:
        处理结果统计
    """
    result = batch_create_metric_definition(info_list)
    return result


@router.delete("")
async def delete(metric_ids: List[str]):
    """
    删除指标定义

    Args:
        metric_ids: 要删除的指标ID列表

    Returns:
        删除结果
    """
    delete_metric_definition(metric_ids)
    return {"success": True, "deleted_count": len(metric_ids)}