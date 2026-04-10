from typing import List, Optional
from fastapi import APIRouter, Query

from apps.extend.metrics2.curd.dim_definition_curd import (
    create_dim_dict,
    batch_create_dim_dict,
    update_dim_dict,
    delete_dim_dict,
    get_dim_dict_by_id,
    get_dim_dict_by_code,
    page_dim_dict,
    get_all_dim_dict
)
from apps.extend.metrics2.models.dim_definition_model import DimDictInfo

router = APIRouter(tags=["Dimension Dictionary"], prefix="/extend/dim-dict")


@router.get("/page/{current_page}/{page_size}")
async def page_dimensions(
    current_page: int,
    page_size: int,
    dim_name: Optional[str] = Query(None, description="维度名称（支持模糊查询）"),
    dim_code: Optional[str] = Query(None, description="维度编码（支持模糊查询）"),
    dim_type: Optional[str] = Query(None, description="维度类型")
):
    """
    分页查询维度定义

    Returns:
        分页结果
    """
    current_page, page_size, total_count, total_pages, _list = page_dim_dict(
        current_page, page_size, dim_name, dim_code, dim_type
    )

    return {
        "current_page": current_page,
        "page_size": page_size,
        "total_count": total_count,
        "total_pages": total_pages,
        "data": _list
    }


@router.get("/list")
async def list_dimensions(
    dim_name: Optional[str] = Query(None, description="维度名称（支持模糊查询）"),
    dim_code: Optional[str] = Query(None, description="维度编码（支持模糊查询）"),
    dim_type: Optional[str] = Query(None, description="维度类型")
):
    """
    获取所有维度定义（不分页）

    Returns:
        维度列表
    """
    _list = get_all_dim_dict(dim_name, dim_code, dim_type)
    return {"data": _list}


@router.get("/{dim_id}")
async def get_dimension(dim_id: str):
    """
    根据ID查询维度定义

    Returns:
        维度详情
    """
    result = get_dim_dict_by_id(dim_id)
    if not result:
        return {"success": False, "message": "维度不存在"}
    return {"success": True, "data": result}


@router.get("/code/{dim_code}")
async def get_dimension_by_code(dim_code: str):
    """
    根据编码查询维度定义

    Returns:
        维度详情
    """
    result = get_dim_dict_by_code(dim_code)
    if not result:
        return {"success": False, "message": "维度不存在"}
    return {"success": True, "data": result}


@router.put("")
async def create_or_update(info: DimDictInfo):
    """
    创建或更新维度定义

    Args:
        info: 维度定义信息对象

    Returns:
        创建的 ID 或更新的 ID
    """
    if info.dim_id:
        # 更新
        dim_id = update_dim_dict(info)
        return {"success": True, "id": dim_id, "action": "update"}
    else:
        # 创建
        dim_id = create_dim_dict(info)
        return {"success": True, "id": dim_id, "action": "create"}


@router.post("/batch")
async def batch_create(info_list: List[DimDictInfo]):
    """
    批量创建维度定义

    Args:
        info_list: 维度定义列表

    Returns:
        处理结果统计
    """
    result = batch_create_dim_dict(info_list)
    return result


@router.delete("")
async def delete(dim_ids: List[str]):
    """
    删除维度定义

    Args:
        dim_ids: 要删除的维度ID列表

    Returns:
        删除结果
    """
    delete_dim_dict(dim_ids)
    return {"success": True, "deleted_count": len(dim_ids)}