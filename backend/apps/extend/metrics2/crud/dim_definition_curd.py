from datetime import datetime
from typing import List, Optional
from sqlalchemy import and_, select, update, delete, func
from sqlmodel import Session

from apps.extend.metrics2.models.dim_definition_model import DimDict, DimDictInfo


def create_dim_dict(session: Session, info: DimDictInfo):
    """
    创建单个维度定义记录

    Args:
        session: 数据库会话
        info: 维度定义信息对象

    Returns:
        创建的记录 ID
    """
    # 基本验证
    if not info.dim_name or not info.dim_name.strip():
        raise Exception("维度名称不能为空")

    if not info.dim_code or not info.dim_code.strip():
        raise Exception("维度编码不能为空")

    # 检查是否已存在
    exists_query = session.query(DimDict).filter(
        and_(
            DimDict.dim_code == info.dim_code.strip(),
            DimDict.dim_name == info.dim_name.strip()
        )
    ).first()

    if exists_query:
        raise Exception(f"维度 {info.dim_name} 已存在")

    # 创建记录
    dim_dict = DimDict(
        dim_id=info.dim_id.strip() if info.dim_id else None,
        dim_name=info.dim_name.strip(),
        dim_code=info.dim_code.strip(),
        dim_type=info.dim_type.strip() if info.dim_type else "普通维度",
        is_valid=1 if info.is_valid else 0
    )

    session.add(dim_dict)
    session.flush()
    session.refresh(dim_dict)

    # ⚠️ 事务提交/回滚由调用方统一管理

    return dim_dict.dim_id


def batch_create_dim_dict(session: Session, info_list: List[DimDictInfo]):
    """
    批量创建维度定义记录

    Args:
        session: 数据库会话
        info_list: 维度定义信息列表

    Returns:
        处理结果统计
    """
    if not info_list:
        return {
            'success_count': 0,
            'failed_records': [],
            'duplicate_count': 0,
            'original_count': 0
        }

    failed_records = []
    success_count = 0
    inserted_ids = []

    # 去重处理
    unique_key_set = set()
    deduplicated_list = []
    duplicate_count = 0

    for info in info_list:
        # 创建唯一标识
        unique_key = (
            info.dim_code.strip().lower() if info.dim_code else '',
            info.dim_name.strip().lower() if info.dim_name else ''
        )

        if unique_key in unique_key_set:
            duplicate_count += 1
            continue

        unique_key_set.add(unique_key)
        deduplicated_list.append(info)

    # 批量插入
    for info in deduplicated_list:
        try:
            dim_id = create_dim_dict(session, info)
            inserted_ids.append(dim_id)
            success_count += 1
        except Exception as e:
            failed_records.append({
                'data': info,
                'errors': [str(e)]
            })

    return {
        'success_count': success_count,
        'failed_records': failed_records,
        'duplicate_count': duplicate_count,
        'original_count': len(info_list),
        'deduplicated_count': len(deduplicated_list)
    }


def update_dim_dict(session: Session, info: DimDictInfo):
    """
    更新维度定义记录

    Args:
        session: 数据库会话
        info: 维度定义信息对象

    Returns:
        更新的记录 ID
    """
    if not info.dim_id:
        raise Exception("ID 不能为空")

    count = session.query(DimDict).filter(
        DimDict.dim_id == info.dim_id
    ).count()

    if count == 0:
        raise Exception("维度定义不存在")

    stmt = update(DimDict).where(
        DimDict.dim_id == info.dim_id
    ).values(
        dim_name=info.dim_name.strip() if info.dim_name else None,
        dim_code=info.dim_code.strip() if info.dim_code else None,
        dim_type=info.dim_type.strip() if info.dim_type else None,
        is_valid=1 if info.is_valid else 0
    )

    session.execute(stmt)
    # ⚠️ 事务提交/回滚由调用方统一管理

    return info.dim_id


def delete_dim_dict(session: Session, dim_ids: List[str]):
    """
    删除维度定义记录

    Args:
        session: 数据库会话
        dim_ids: 要删除的维度ID列表
    """
    stmt = delete(DimDict).where(DimDict.dim_id.in_(dim_ids))
    session.execute(stmt)
    # ⚠️ 事务提交/回滚由调用方统一管理


def get_dim_dict_by_id(session: Session, dim_id: str) -> Optional[DimDictInfo]:
    """
    根据ID查询维度定义

    Args:
        session: 数据库会话
        dim_id: 维度ID

    Returns:
        维度定义信息对象
    """
    dim_dict = session.query(DimDict).filter(DimDict.dim_id == dim_id).first()

    if not dim_dict:
        return None

    return DimDictInfo(
        dim_id=dim_dict.dim_id,
        dim_name=dim_dict.dim_name,
        dim_code=dim_dict.dim_code,
        dim_type=dim_dict.dim_type,
        is_valid=bool(dim_dict.is_valid)
    )


def get_dim_dict_by_code(session: Session, dim_code: str) -> Optional[DimDictInfo]:
    """
    根据编码查询维度定义

    Args:
        session: 数据库会话
        dim_code: 维度编码

    Returns:
        维度定义信息对象
    """
    dim_dict = session.query(DimDict).filter(DimDict.dim_code == dim_code).first()

    if not dim_dict:
        return None

    return DimDictInfo(
        dim_id=dim_dict.dim_id,
        dim_name=dim_dict.dim_name,
        dim_code=dim_dict.dim_code,
        dim_type=dim_dict.dim_type,
        is_valid=bool(dim_dict.is_valid)
    )


def page_dim_dict(session: Session, current_page: int = 1, page_size: int = 10,
                dim_name: Optional[str] = None,
                dim_code: Optional[str] = None,
                dim_type: Optional[str] = None):
    """
    分页查询维度定义

    Args:
        session: 数据库会话
        current_page: 当前页码
        page_size: 每页数量
        dim_name: 维度名称（支持模糊查询）
        dim_code: 维度编码（支持模糊查询）
        dim_type: 维度类型

    Returns:
        分页结果
    """
    # 构建查询条件
    conditions = []
    if dim_name and dim_name.strip():
        conditions.append(DimDict.dim_name.ilike(f"%{dim_name.strip()}%"))
    if dim_code and dim_code.strip():
        conditions.append(DimDict.dim_code.ilike(f"%{dim_code.strip()}%"))
    if dim_type and dim_type.strip():
        conditions.append(DimDict.dim_type == dim_type.strip())

    # 查询总数
    if conditions:
        count_stmt = select(func.count()).select_from(DimDict).where(and_(*conditions))
    else:
        count_stmt = select(func.count()).select_from(DimDict)

    total_count = session.execute(count_stmt).scalar()

    # 分页处理
    page_size = max(10, page_size)
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
    current_page = max(1, min(current_page, total_pages)) if total_pages > 0 else 1

    # 查询数据
    stmt = select(DimDict)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.order_by(DimDict.create_time.desc())
    stmt = stmt.offset((current_page - 1) * page_size).limit(page_size)

    results = session.execute(stmt).scalars().all()

    _list = []
    for dim_dict in results:
        _list.append(DimDictInfo(
            dim_id=dim_dict.dim_id,
            dim_name=dim_dict.dim_name,
            dim_code=dim_dict.dim_code,
            dim_type=dim_dict.dim_type,
            is_valid=bool(dim_dict.is_valid)
        ))

    return current_page, page_size, total_count, total_pages, _list


def get_all_dim_dict(session: Session,
                   dim_name: Optional[str] = None,
                   dim_code: Optional[str] = None,
                   dim_type: Optional[str] = None):
    """
    获取所有维度定义（不分页）

    Args:
        session: 数据库会话
        dim_name: 维度名称（支持模糊查询）
        dim_code: 维度编码（支持模糊查询）
        dim_type: 维度类型

    Returns:
        维度定义列表
    """
    conditions = []
    if dim_name and dim_name.strip():
        conditions.append(DimDict.dim_name.ilike(f"%{dim_name.strip()}%"))
    if dim_code and dim_code.strip():
        conditions.append(DimDict.dim_code.ilike(f"%{dim_code.strip()}%"))
    if dim_type and dim_type.strip():
        conditions.append(DimDict.dim_type == dim_type.strip())

    stmt = select(DimDict)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.order_by(DimDict.create_time.desc())

    results = session.execute(stmt).scalars().all()

    _list = []
    for dim_dict in results:
        _list.append(DimDictInfo(
            dim_id=dim_dict.dim_id,
            dim_name=dim_dict.dim_name,
            dim_code=dim_dict.dim_code,
            dim_type=dim_dict.dim_type,
            is_valid=bool(dim_dict.is_valid)
        ))

    return _list