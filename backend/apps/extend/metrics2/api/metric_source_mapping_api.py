from typing import List, Optional
from fastapi import APIRouter, Query

from apps.extend.metrics2.crud.metric_source_mapping_curd import (
    create_metric_source_mapping,
    batch_create_metric_source_mapping,
    update_metric_source_mapping,
    delete_metric_source_mapping,
    get_metric_source_mapping_by_id,
    get_metric_source_mapping_by_metric_id,
    page_metric_source_mapping,
    get_all_metric_source_mapping
)
from apps.extend.metrics2.models.metric_source_mapping_model import MetricSourceMappingInfo

router = APIRouter(tags=["Metric Source Mapping"], prefix="/extend/metric-source-mapping")


@router.get("/page/{current_page}/{page_size}")
async def page_mappings(
    current_page: int,
    page_size: int,
    metric_id: Optional[str] = Query(None, description="指标ID（可选）"),
    datasource: Optional[str] = Query(None, description="数据源标识（可选）"),
    source_type: Optional[str] = Query(None, description="数据源类型（可选）")
):
    """
    分页查询指标源映射

    Returns:
        分页结果
    """
    current_page, page_size, total_count, total_pages, _list = page_metric_source_mapping(
        current_page, page_size, metric_id, datasource, source_type
    )

    return {
        "current_page": current_page,
        "page_size": page_size,
        "total_count": total_count,
        "total_pages": total_pages,
        "data": _list
    }


@router.get("/list")
async def list_mappings(
    metric_id: Optional[str] = Query(None, description="指标ID（可选）"),
    datasource: Optional[str] = Query(None, description="数据源标识（可选）"),
    source_type: Optional[str] = Query(None, description="数据源类型（可选）")
):
    """
    获取所有指标源映射（不分页）

    Returns:
        指标源映射列表
    """
    _list = get_all_metric_source_mapping(metric_id, datasource, source_type)
    return {"data": _list}


@router.get("/metric/{metric_id}")
async def get_mappings_by_metric(metric_id: str):
    """
    根据指标ID查询所有源映射

    Returns:
        指标源映射列表
    """
    _list = get_metric_source_mapping_by_metric_id(metric_id)
    return {"data": _list}


@router.get("/{map_id}")
async def get_mapping(map_id: str):
    """
    根据ID查询指标源映射

    Returns:
        指标源映射详情
    """
    result = get_metric_source_mapping_by_id(map_id)
    if not result:
        return {"success": False, "message": "指标源映射不存在"}
    return {"success": True, "data": result}


@router.put("")
async def create_or_update(info: MetricSourceMappingInfo):
    """
    创建或更新指标源映射

    Args:
        info: 指标源映射信息对象

    Returns:
        创建的 ID 或更新的 ID
    """
    if info.map_id:
        # 更新
        map_id = update_metric_source_mapping(info)
        return {"success": True, "id": map_id, "action": "update"}
    else:
        # 创建
        map_id = create_metric_source_mapping(info)
        return {"success": True, "id": map_id, "action": "create"}


@router.post("/batch")
async def batch_create(info_list: List[MetricSourceMappingInfo]):
    """
    批量创建指标源映射

    Args:
        info_list: 指标源映射列表

    Returns:
        处理结果统计
    """
    result = batch_create_metric_source_mapping(info_list)
    return result


@router.delete("")
async def delete(map_ids: List[str]):
    """
    删除指标源映射

    Args:
        map_ids: 要删除的映射ID列表

    Returns:
        删除结果
    """
    delete_metric_source_mapping(map_ids)
    return {"success": True, "deleted_count": len(map_ids)}