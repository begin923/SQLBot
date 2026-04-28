from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional

from common.core.deps import SessionDep
from apps.extend.metrics2.services import LineageAnalysisService
from apps.extend.metrics2.services import CircularDependencyService

router = APIRouter(prefix="/api/metrics/v2/lineage", tags=["血缘分析"])


@router.get("/table/upstream", summary="获取表的上游血缘")
def get_table_upstream_api(
    target_table: str = Query(..., description="目标表名"),
    max_depth: int = Query(10, description="最大递归深度"),
    session: SessionDep = None
):
    """获取表的所有上游表（递归）"""
    service = LineageAnalysisService(session)
    upstream_tables = service.get_upstream_tables(target_table, max_depth)
    
    return {
        'success': True,
        'data': upstream_tables,
        'count': len(upstream_tables)
    }


@router.get("/table/downstream", summary="获取表的下游血缘")
def get_table_downstream_api(
    source_table: str = Query(..., description="源表名"),
    max_depth: int = Query(10, description="最大递归深度"),
    session: SessionDep = None
):
    """获取表的所有下游表（影响分析）"""
    service = LineageAnalysisService(session)
    downstream_tables = service.get_downstream_tables(source_table, max_depth)
    
    return {
        'success': True,
        'data': downstream_tables,
        'count': len(downstream_tables)
    }


@router.get("/metric/full", summary="获取指标完整血缘链路")
def get_metric_full_lineage_api(
    metric_id: str = Query(..., description="指标ID"),
    session: SessionDep = None
):
    """获取指标的完整血缘链路"""
    service = LineageAnalysisService(session)
    lineage = service.get_full_lineage(metric_id)
    
    return {
        'success': True,
        'data': lineage
    }


@router.get("/dimension/lineage", summary="获取维度血缘映射")
def get_dimension_lineage_api(
    dim_id: str = Query(..., description="维度ID"),
    session: SessionDep = None
):
    """获取维度的血缘映射"""
    service = LineageAnalysisService(session)
    lineage = service.get_dimension_lineage(dim_id)
    
    return {
        'success': True,
        'data': lineage,
        'count': len(lineage)
    }


@router.get("/impact/analysis", summary="影响分析")
def get_impact_analysis_api(
    table_name: str = Query(..., description="表名"),
    session: SessionDep = None
):
    """分析表变更会影响哪些指标"""
    service = LineageAnalysisService(session)
    impact = service.get_impact_analysis(table_name)
    
    return {
        'success': True,
        'data': impact
    }


@router.get("/graph/build", summary="构建完整血缘图")
def build_lineage_graph_api(
    session: SessionDep = None
):
    """构建完整血缘图"""
    service = LineageAnalysisService(session)
    graph = service.build_lineage_graph()
    
    return {
        'success': True,
        'data': graph
    }


@router.get("/metric/validate", summary="校验指标血缘完整性")
def validate_metric_lineage_api(
    metric_id: str = Query(..., description="指标ID"),
    session: SessionDep = None
):
    """校验指标血缘完整性"""
    service = LineageAnalysisService(session)
    result = service.validate_lineage_completeness(metric_id)
    
    return {
        'success': True,
        'data': result
    }


@router.post("/dependency/check", summary="检测循环依赖")
def check_circular_dependency_api(
    relations: list[dict],
    session: SessionDep = None
):
    """
    检测循环依赖
    
    relations 格式：
    [
        {'metric_id': 'M001', 'sub_metric_id': 'M002'},
        {'metric_id': 'M002', 'sub_metric_id': 'M003'},
        {'metric_id': 'M003', 'sub_metric_id': 'M001'}
    ]
    """
    has_cycle, cycle_path = CircularDependencyService.detect_cycle_from_relations(relations)
    
    return {
        'success': True,
        'data': {
            'has_cycle': has_cycle,
            'cycle_path': cycle_path
        },
        'message': '发现循环依赖' if has_cycle else '无循环依赖'
    }


@router.post("/dependency/validate-metric", summary="验证单个指标依赖")
def validate_metric_dependency_api(
    metric_id: str = Query(..., description="指标ID"),
    sub_metrics: list[str] = Query(..., description="子指标ID列表"),
    session: SessionDep = None
):
    """验证单个指标的依赖是否形成循环"""
    service = CircularDependencyService()
    has_cycle, cycle_path = service.check_single_metric(metric_id, sub_metrics)
    
    return {
        'success': True,
        'data': {
            'metric_id': metric_id,
            'has_cycle': has_cycle,
            'cycle_path': cycle_path
        },
        'message': '发现循环依赖' if has_cycle else '依赖关系正常'
    }


@router.get("/field/validate", summary="校验字段合法性")
def validate_field_api(
    db_table: str = Query(..., description="物理表名"),
    metric_column: str = Query(..., description="指标字段名"),
    session: SessionDep = None
):
    """校验字段是否存在于血缘映射中"""
    from apps.extend.metrics2.crud import validate_field_exists
    
    is_valid = validate_field_exists(session, db_table, metric_column)
    
    return {
        'success': True,
        'data': {
            'db_table': db_table,
            'metric_column': metric_column,
            'is_valid': is_valid
        },
        'message': '字段合法' if is_valid else '字段不存在于血缘映射中'
    }
