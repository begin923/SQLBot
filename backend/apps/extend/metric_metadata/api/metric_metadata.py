from typing import List, Optional

from fastapi import APIRouter, Query

from apps.extend.metric_metadata.curd.metric_metadata import (
    create_metric_metadata,
    batch_create_metric_metadata,
    update_metric_metadata,
    delete_metric_metadata,
    get_metric_metadata_by_id,
    page_metric_metadata,
    get_all_metric_metadata,
    fill_empty_embeddings
)
from apps.extend.metric_metadata.models.metric_metadata_model import MetricMetadataInfo
from common.core.deps import SessionDep

router = APIRouter(tags=["Metric Metadata"], prefix="/extend/metric-metadata")


@router.get("/page/{current_page}/{page_size}")
async def page_metrics(
    session: SessionDep,
    current_page: int,
    page_size: int,
    metric_name: Optional[str] = Query(None, description="指标名称（支持模糊查询）"),
    datasource_id: Optional[int] = Query(None, description="数据源 ID")
):
    """
    分页查询指标元数据
    
    Returns:
        分页结果
    """
    current_page, page_size, total_count, total_pages, _list = page_metric_metadata(
        session, current_page, page_size, metric_name, datasource_id
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
    session: SessionDep,
    metric_name: Optional[str] = Query(None, description="指标名称（支持模糊查询）"),
    datasource_id: Optional[int] = Query(None, description="数据源 ID")
):
    """
    获取所有指标元数据（不分页）
    
    Returns:
        指标列表
    """
    _list = get_all_metric_metadata(session, metric_name, datasource_id)
    return {"data": _list}


@router.get("/{id}")
async def get_metric(session: SessionDep, id: int):
    """
    根据 ID 查询指标元数据
    
    Returns:
        指标详情
    """
    result = get_metric_metadata_by_id(session, id)
    if not result:
        return {"success": False, "message": "指标不存在"}
    return {"success": True, "data": result}


@router.put("")
async def create_or_update(session: SessionDep, info: MetricMetadataInfo):
    """
    创建或更新指标元数据
    
    Args:
        info: 指标元数据信息对象
    
    Returns:
        创建的 ID 或更新的 ID
    """
    if info.id:
        # 更新
        metric_id = update_metric_metadata(session, info)
        return {"success": True, "id": metric_id, "action": "update"}
    else:
        # 创建
        metric_id = create_metric_metadata(session, info)
        return {"success": True, "id": metric_id, "action": "create"}


@router.post("/batch")
async def batch_create(session: SessionDep, info_list: List[MetricMetadataInfo]):
    """
    批量创建指标元数据
    
    Args:
        info_list: 指标元数据列表
    
    Returns:
        处理结果统计
    """
    result = batch_create_metric_metadata(session, info_list)
    return result


@router.delete("")
async def delete(session: SessionDep, ids: List[int]):
    """
    删除指标元数据
    
    Args:
        ids: 要删除的记录 ID 列表
    
    Returns:
        删除结果
    """
    delete_metric_metadata(session, ids)
    return {"success": True, "deleted_count": len(ids)}


@router.post("/fill-embeddings")
async def fill_embeddings():
    """
    填充缺失的 embedding 向量（后台任务）
    
    Returns:
        执行结果
    """
    try:
        from common.core.db import engine
        from sqlalchemy.orm import sessionmaker, scoped_session
        session_maker = scoped_session(sessionmaker(bind=engine))
        
        fill_empty_embeddings()
        return {"success": True, "message": "开始填充 embedding，请稍后查看日志"}
    except Exception as e:
        return {"success": False, "message": str(e)}


# ========== 本地测试接口 ==========

@router.get("/test/sample-data")
async def test_sample_data():
    """
    【本地测试】返回示例数据
    
    Returns:
        示例数据列表
    """
    sample_data = [
        {
            "metric_name": "销售额",
            "synonyms": "营收，销售收入，卖钱额",
            "datasource_id": 1,
            "table_name": "orders",
            "core_fields": "order_id, amount, create_time",
            "calc_logic": "SUM(amount)",
            "upstream_table": "order_items",
            "dw_layer": "DWS"
        },
        {
            "metric_name": "订单量",
            "synonyms": "订单数，下单数量",
            "datasource_id": 1,
            "table_name": "orders",
            "core_fields": "order_id, user_id, create_time",
            "calc_logic": "COUNT(DISTINCT order_id)",
            "upstream_table": None,
            "dw_layer": "DWS"
        },
        {
            "metric_name": "客单价",
            "synonyms": "人均消费，ARPU",
            "datasource_id": 1,
            "table_name": "orders",
            "core_fields": "user_id, amount",
            "calc_logic": "SUM(amount) / COUNT(DISTINCT user_id)",
            "upstream_table": "orders",
            "dw_layer": "ADS"
        },
        {
            "metric_name": "毛利率",
            "synonyms": "毛利，利润率",
            "datasource_id": 2,
            "table_name": "sales_summary",
            "core_fields": "revenue, cost",
            "calc_logic": "(revenue - cost) / revenue * 100",
            "upstream_table": "cost_detail",
            "dw_layer": "ADS"
        },
        {
            "metric_name": "日活用户",
            "synonyms": "DAU，活跃用户数",
            "datasource_id": 1,
            "table_name": "user_login",
            "core_fields": "user_id, login_date",
            "calc_logic": "COUNT(DISTINCT user_id)",
            "upstream_table": "user_info",
            "dw_layer": "DWS"
        }
    ]
    
    return {
        "sample_data": sample_data,
        "count": len(sample_data),
        "description": "这些是可以直接用于测试的示例数据"
    }


@router.post("/test/insert-sample")
async def test_insert_sample(session: SessionDep):
    """
    【本地测试】插入示例数据到数据库
    
    Returns:
        插入结果
    """
    sample_data = [
        MetricMetadataInfo(
            metric_name="销售额",
            synonyms="营收，销售收入，卖钱额",
            datasource_id=1,
            table_name="orders",
            core_fields="order_id, amount, create_time",
            calc_logic="SUM(amount)",
            upstream_table="order_items",
            dw_layer="DWS"
        ),
        MetricMetadataInfo(
            metric_name="订单量",
            synonyms="订单数，下单数量",
            datasource_id=1,
            table_name="orders",
            core_fields="order_id, user_id, create_time",
            calc_logic="COUNT(DISTINCT order_id)",
            upstream_table=None,
            dw_layer="DWS"
        ),
        MetricMetadataInfo(
            metric_name="客单价",
            synonyms="人均消费，ARPU",
            datasource_id=1,
            table_name="orders",
            core_fields="user_id, amount",
            calc_logic="SUM(amount) / COUNT(DISTINCT user_id)",
            upstream_table="orders",
            dw_layer="ADS"
        ),
        MetricMetadataInfo(
            metric_name="毛利率",
            synonyms="毛利，利润率",
            datasource_id=2,
            table_name="sales_summary",
            core_fields="revenue, cost",
            calc_logic="(revenue - cost) / revenue * 100",
            upstream_table="cost_detail",
            dw_layer="ADS"
        ),
        MetricMetadataInfo(
            metric_name="日活用户",
            synonyms="DAU，活跃用户数",
            datasource_id=1,
            table_name="user_login",
            core_fields="user_id, login_date",
            calc_logic="COUNT(DISTINCT user_id)",
            upstream_table="user_info",
            dw_layer="DWS"
        )
    ]
    
    try:
        result = batch_create_metric_metadata(session, sample_data)
        return {
            "success": True,
            "result": result,
            "message": f"成功插入 {result['success_count']} 条记录，失败 {len(result['failed_records'])} 条"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "traceback": __import__('traceback').format_exc()
        }


@router.get("/test/query")
async def test_query(session: SessionDep, metric_name: str = "销售额"):
    """
    【本地测试】查询指标元数据
    
    Args:
        metric_name: 指标名称
    
    Returns:
        查询结果
    """
    try:
        results = get_all_metric_metadata(session, metric_name=metric_name)
        return {
            "success": True,
            "data": results,
            "count": len(results)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@router.delete("/test/clear-all")
async def test_clear_all(session: SessionDep):
    """
    【本地测试】清空所有测试数据（危险操作！）
    
    Returns:
        清空结果
    """
    try:
        from sqlalchemy import text
        session.execute(text("TRUNCATE TABLE metric_metadata RESTART IDENTITY CASCADE"))
        session.commit()
        return {
            "success": True,
            "message": "已清空所有数据"
        }
    except Exception as e:
        session.rollback()
        return {
            "success": False,
            "error": str(e)
        }
