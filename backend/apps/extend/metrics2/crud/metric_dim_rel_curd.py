from datetime import datetime
from typing import List, Optional
from sqlalchemy import and_, or_, select, insert, update, delete, text, func
from sqlmodel import Session

from apps.extend.metrics2.models.metric_dim_rel_model import MetricDimRel, MetricDimRelInfo


def create_metric_dim_rel(session: Session, info: MetricDimRelInfo):
    """
    创建单个指标维度关联记录

    Args:
        session: 数据库会话
        info: 指标维度关联信息对象

    Returns:
        创建的记录 ID
    """
    # 基本验证
    if not info.metric_id or not info.metric_id.strip():
        raise Exception("指标ID不能为空")

    if not info.dim_id or not info.dim_id.strip():
        raise Exception("维度ID不能为空")

    # 检查是否已存在
    exists_query = session.query(MetricDimRel).filter(
        and_(
            MetricDimRel.metric_id == info.metric_id.strip(),
            MetricDimRel.dim_id == info.dim_id.strip()
        )
    ).first()

    if exists_query:
        raise Exception(f"指标维度关联已存在")

    # 创建记录
    metric_dim_rel = MetricDimRel(
        metric_id=info.metric_id.strip(),
        dim_id=info.dim_id.strip(),
        is_required=1 if info.is_required else 0,
        sort=info.sort if info.sort is not None else 0
    )

    session.add(metric_dim_rel)
    session.flush()
    session.refresh(metric_dim_rel)

    # ⚠️ 事务提交/回滚由调用方统一管理

    return metric_dim_rel.id


def batch_create_metric_dim_rel(session: Session, info_list: List[MetricDimRelInfo]):
    """
    批量创建指标维度关联记录

    Args:
        session: 数据库会话
        info_list: 指标维度关联信息列表

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
            info.metric_id.strip().lower() if info.metric_id else '',
            info.dim_id.strip().lower() if info.dim_id else ''
        )

        if unique_key in unique_key_set:
            duplicate_count += 1
            continue

        unique_key_set.add(unique_key)
        deduplicated_list.append(info)

    # 批量插入
    for info in deduplicated_list:
        try:
            rel_id = create_metric_dim_rel(session, info)
            inserted_ids.append(rel_id)
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


def update_metric_dim_rel(session: Session, info: MetricDimRelInfo):
    """
    更新指标维度关联记录

    Args:
        session: 数据库会话
        info: 指标维度关联信息对象

    Returns:
        更新的记录 ID
    """
    if not info.id:
        raise Exception("ID 不能为空")

    count = session.query(MetricDimRel).filter(
        MetricDimRel.id == info.id
    ).count()

    if count == 0:
        raise Exception("指标维度关联不存在")

    stmt = update(MetricDimRel).where(
        MetricDimRel.id == info.id
    ).values(
        metric_id=info.metric_id.strip() if info.metric_id else None,
        dim_id=info.dim_id.strip() if info.dim_id else None,
        is_required=1 if info.is_required else 0,
        sort=info.sort if info.sort is not None else 0
    )

    session.execute(stmt)
    # ⚠️ 事务提交/回滚由调用方统一管理

    return info.id


def delete_metric_dim_rel(session: Session, ids: List[int]):
    """
    删除指标维度关联记录

    Args:
        session: 数据库会话
        ids: 要删除的记录ID列表
    """
    stmt = delete(MetricDimRel).where(MetricDimRel.id.in_(ids))
    session.execute(stmt)
    # ⚠️ 事务提交/回滚由调用方统一管理


def get_metric_dim_rel_by_id(session: Session, id: int) -> Optional[MetricDimRelInfo]:
    """
    根据ID查询指标维度关联

    Args:
        session: 数据库会话
        id: 记录ID

    Returns:
        指标维度关联信息对象
    """
    metric_dim_rel = session.query(MetricDimRel).filter(MetricDimRel.id == id).first()

    if not metric_dim_rel:
        return None

    return MetricDimRelInfo(
        id=metric_dim_rel.id,
        metric_id=metric_dim_rel.metric_id,
        dim_id=metric_dim_rel.dim_id,
        is_required=bool(metric_dim_rel.is_required)
    )


def get_metric_dim_rel_by_metric_id(session: Session, metric_id: str) -> List[MetricDimRelInfo]:
    """
    根据指标ID查询所有维度关联

    Args:
        session: 数据库会话
        metric_id: 指标ID

    Returns:
        指标维度关联列表
    """
    results = session.query(MetricDimRel).filter(
        MetricDimRel.metric_id == metric_id
    ).all()

    _list = []
    for rel in results:
        _list.append(MetricDimRelInfo(
            id=rel.id,
            metric_id=rel.metric_id,
            dim_id=rel.dim_id,
            is_required=bool(rel.is_required)
        ))

    return _list


def page_metric_dim_rel(session: Session, current_page: int = 1, page_size: int = 10,
                      metric_id: Optional[str] = None,
                      dim_id: Optional[str] = None):
    """
    分页查询指标维度关联

    Args:
        session: 数据库会话
        current_page: 当前页码
        page_size: 每页数量
        metric_id: 指标ID（可选）
        dim_id: 维度ID（可选）

    Returns:
        分页结果
    """
    # 构建查询条件
    conditions = []
    if metric_id and metric_id.strip():
        conditions.append(MetricDimRel.metric_id == metric_id.strip())
    if dim_id and dim_id.strip():
        conditions.append(MetricDimRel.dim_id == dim_id.strip())

    # 查询总数
    if conditions:
        count_stmt = select(func.count()).select_from(MetricDimRel).where(and_(*conditions))
    else:
        count_stmt = select(func.count()).select_from(MetricDimRel)

    total_count = session.execute(count_stmt).scalar()

    # 分页处理
    page_size = max(10, page_size)
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
    current_page = max(1, min(current_page, total_pages)) if total_pages > 0 else 1

    # 查询数据
    stmt = select(MetricDimRel)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.offset((current_page - 1) * page_size).limit(page_size)

    results = session.execute(stmt).scalars().all()

    _list = []
    for rel in results:
        _list.append(MetricDimRelInfo(
            id=rel.id,
            metric_id=rel.metric_id,
            dim_id=rel.dim_id,
            is_required=bool(rel.is_required),
            sort=rel.sort
        ))

    return current_page, page_size, total_count, total_pages, _list


def get_all_metric_dim_rel(session: Session,
                        metric_id: Optional[str] = None,
                        dim_id: Optional[str] = None):
    """
    获取所有指标维度关联（不分页）

    Args:
        session: 数据库会话
        metric_id: 指标ID（可选）
        dim_id: 维度ID（可选）

    Returns:
        指标维度关联列表
    """
    conditions = []
    if metric_id and metric_id.strip():
        conditions.append(MetricDimRel.metric_id == metric_id.strip())
    if dim_id and dim_id.strip():
        conditions.append(MetricDimRel.dim_id == dim_id.strip())

    stmt = select(MetricDimRel)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    results = session.execute(stmt).scalars().all()

    _list = []
    for rel in results:
        _list.append(MetricDimRelInfo(
            id=rel.id,
            metric_id=rel.metric_id,
            dim_id=rel.dim_id,
            is_required=bool(rel.is_required),
            sort=rel.sort
        ))

    return _list