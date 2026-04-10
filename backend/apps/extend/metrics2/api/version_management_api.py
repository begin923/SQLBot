from datetime import datetime
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional

from common.core.deps import SessionDep
from apps.extend.metrics2.curd import (
    create_metric_version,
    get_metric_version_by_id,
    get_metric_versions_by_metric_id,
    get_current_version_by_metric_id,
    get_version_by_time,
    update_metric_version,
    delete_metric_version,
    page_metric_version,
    get_all_metric_version,
    create_dim_version,
    get_dim_version_by_id,
    get_dim_versions_by_dim_id,
    get_current_version_by_dim_id,
    update_dim_version,
    delete_dim_version,
    page_dim_version,
    get_all_dim_version
)
from apps.extend.metrics2.models import MetricVersionInfo, DimVersionInfo

router = APIRouter(prefix="/api/metrics/v2/version", tags=["指标版本管理"])


# ==================== 指标版本管理 ====================

@router.post("/metric/create", summary="创建指标版本")
def create_metric_version_api(
    info: MetricVersionInfo,
    session: SessionDep
):
    """创建指标版本记录"""
    try:
        version_id = create_metric_version(session, info)
        return {
            'success': True,
            'version_id': version_id,
            'message': '版本创建成功'
        }
    except Exception as e:
        return {'success': False, 'message': str(e)}


@router.get("/metric/list", summary="查询指标版本列表")
def get_metric_versions_api(
    metric_id: str = Query(..., description="指标ID"),
    session: SessionDep = None
):
    """查询指标的所有版本"""
    versions = get_metric_versions_by_metric_id(session, metric_id)
    return {
        'success': True,
        'data': versions,
        'count': len(versions)
    }


@router.get("/metric/current", summary="获取指标当前版本")
def get_current_metric_version_api(
    metric_id: str = Query(..., description="指标ID"),
    session: SessionDep = None
):
    """获取指标的当前版本"""
    version = get_current_version_by_metric_id(session, metric_id)
    return {
        'success': True,
        'data': version
    }


@router.get("/metric/history", summary="根据时间匹配历史版本")
def get_metric_version_by_time_api(
    metric_id: str = Query(..., description="指标ID"),
    query_time: datetime = Query(..., description="查询时间"),
    session: SessionDep = None
):
    """根据查询时间匹配历史版本"""
    version = get_version_by_time(session, metric_id, query_time)
    return {
        'success': True,
        'data': version
    }


@router.put("/metric/update", summary="更新指标版本")
def update_metric_version_api(
    info: MetricVersionInfo,
    session: SessionDep
):
    """更新指标版本"""
    try:
        version_id = update_metric_version(session, info)
        return {
            'success': True,
            'version_id': version_id,
            'message': '版本更新成功'
        }
    except Exception as e:
        return {'success': False, 'message': str(e)}


@router.delete("/metric/delete", summary="删除指标版本")
def delete_metric_version_api(
    version_ids: list[str] = Query(..., description="版本ID列表"),
    session: SessionDep = None
):
    """删除指标版本"""
    try:
        delete_metric_version(session, version_ids)
        return {
            'success': True,
            'message': '版本删除成功'
        }
    except Exception as e:
        return {'success': False, 'message': str(e)}


@router.get("/metric/page", summary="分页查询指标版本")
def page_metric_version_api(
    current_page: int = Query(1, description="当前页码"),
    page_size: int = Query(10, description="每页数量"),
    metric_id: Optional[str] = Query(None, description="指标ID"),
    session: SessionDep = None
):
    """分页查询指标版本"""
    result = page_metric_version(session, current_page, page_size, metric_id)
    current_page, page_size, total_count, total_pages, data = result
    return {
        'success': True,
        'data': {
            'current_page': current_page,
            'page_size': page_size,
            'total_count': total_count,
            'total_pages': total_pages,
            'list': data
        }
    }


# ==================== 维度版本管理 ====================

@router.post("/dimension/create", summary="创建维度版本")
def create_dim_version_api(
    info: DimVersionInfo,
    session: SessionDep
):
    """创建维度版本记录"""
    try:
        version_id = create_dim_version(session, info)
        return {
            'success': True,
            'version_id': version_id,
            'message': '版本创建成功'
        }
    except Exception as e:
        return {'success': False, 'message': str(e)}


@router.get("/dimension/list", summary="查询维度版本列表")
def get_dim_versions_api(
    dim_id: str = Query(..., description="维度ID"),
    session: SessionDep = None
):
    """查询维度的所有版本"""
    versions = get_dim_versions_by_dim_id(session, dim_id)
    return {
        'success': True,
        'data': versions,
        'count': len(versions)
    }


@router.get("/dimension/current", summary="获取维度当前版本")
def get_current_dim_version_api(
    dim_id: str = Query(..., description="维度ID"),
    session: SessionDep = None
):
    """获取维度的当前版本"""
    version = get_current_version_by_dim_id(session, dim_id)
    return {
        'success': True,
        'data': version
    }


@router.put("/dimension/update", summary="更新维度版本")
def update_dim_version_api(
    info: DimVersionInfo,
    session: SessionDep
):
    """更新维度版本"""
    try:
        version_id = update_dim_version(session, info)
        return {
            'success': True,
            'version_id': version_id,
            'message': '版本更新成功'
        }
    except Exception as e:
        return {'success': False, 'message': str(e)}


@router.delete("/dimension/delete", summary="删除维度版本")
def delete_dim_version_api(
    version_ids: list[str] = Query(..., description="版本ID列表"),
    session: SessionDep = None
):
    """删除维度版本"""
    try:
        delete_dim_version(session, version_ids)
        return {
            'success': True,
            'message': '版本删除成功'
        }
    except Exception as e:
        return {'success': False, 'message': str(e)}


@router.get("/dimension/page", summary="分页查询维度版本")
def page_dim_version_api(
    current_page: int = Query(1, description="当前页码"),
    page_size: int = Query(10, description="每页数量"),
    dim_id: Optional[str] = Query(None, description="维度ID"),
    session: SessionDep = None
):
    """分页查询维度版本"""
    result = page_dim_version(session, current_page, page_size, dim_id)
    current_page, page_size, total_count, total_pages, data = result
    return {
        'success': True,
        'data': {
            'current_page': current_page,
            'page_size': page_size,
            'total_count': total_count,
            'total_pages': total_pages,
            'list': data
        }
    }
