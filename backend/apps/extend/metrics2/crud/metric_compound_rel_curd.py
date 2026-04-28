from datetime import datetime
from typing import List, Optional
from sqlalchemy import and_, or_, select, insert, update, delete, text, func
from sqlmodel import Session

from apps.extend.metrics2.models.metric_compound_rel_model import MetricCompoundRel, MetricCompoundRelInfo


def create_metric_compound_rel(session: Session, info: MetricCompoundRelInfo):
    """
    创建单个复合指标子指标关联记录

    Args:
        session: 数据库会话
        info: 复合指标子指标关联信息对象

    Returns:
        创建的记录 ID
    """
    # 基本验证
    if not info.metric_id or not info.metric_id.strip():
        raise Exception("复合指标ID不能为空")

    if not info.sub_metric_id or not info.sub_metric_id.strip():
        raise Exception("子指标ID不能为空")

    if not info.cal_operator or not info.cal_operator.strip():
        raise Exception("运算符号不能为空")

    # 检查是否已存在
    exists_query = session.query(MetricCompoundRel).filter(
        and_(
            MetricCompoundRel.metric_id == info.metric_id.strip(),
            MetricCompoundRel.sub_metric_id == info.sub_metric_id.strip(),
            MetricCompoundRel.cal_operator == info.cal_operator.strip()
        )
    ).first()

    if exists_query:
        raise Exception(f"复合指标子指标关联已存在")

    # 创建记录
    metric_compound_rel = MetricCompoundRel(
        metric_id=info.metric_id.strip(),
        sub_metric_id=info.sub_metric_id.strip(),
        cal_operator=info.cal_operator.strip(),
        sort=info.sort if info.sort is not None else 0
    )

    session.add(metric_compound_rel)
    session.flush()
    session.refresh(metric_compound_rel)

    # ⚠️ 事务提交/回滚由调用方统一管理

    return metric_compound_rel.id


def batch_create_metric_compound_rel(session: Session, info_list: List[MetricCompoundRelInfo]):
    """
    批量创建复合指标子指标关联记录

    Args:
        session: 数据库会话
        info_list: 复合指标子指标关联信息列表

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
            info.sub_metric_id.strip().lower() if info.sub_metric_id else '',
            info.cal_operator.strip().lower() if info.cal_operator else ''
        )

        if unique_key in unique_key_set:
            duplicate_count += 1
            continue

        unique_key_set.add(unique_key)
        deduplicated_list.append(info)

    # 批量插入
    for info in deduplicated_list:
        try:
            rel_id = create_metric_compound_rel(session, info)
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


def update_metric_compound_rel(session: Session, info: MetricCompoundRelInfo):
    """
    更新复合指标子指标关联记录

    Args:
        session: 数据库会话
        info: 复合指标子指标关联信息对象

    Returns:
        更新的记录 ID
    """
    if not info.id:
        raise Exception("ID 不能为空")

    count = session.query(MetricCompoundRel).filter(
        MetricCompoundRel.id == info.id
    ).count()

    if count == 0:
        raise Exception("复合指标子指标关联不存在")

    stmt = update(MetricCompoundRel).where(
        MetricCompoundRel.id == info.id
    ).values(
        metric_id=info.metric_id.strip() if info.metric_id else None,
        sub_metric_id=info.sub_metric_id.strip() if info.sub_metric_id else None,
        cal_operator=info.cal_operator.strip() if info.cal_operator else None,
        sort=info.sort if info.sort is not None else 0
    )

    session.execute(stmt)
    # ⚠️ 事务提交/回滚由调用方统一管理

    return info.id


def delete_metric_compound_rel(session: Session, ids: List[int]):
    """
    删除复合指标子指标关联记录

    Args:
        session: 数据库会话
        ids: 要删除的记录ID列表
    """
    stmt = delete(MetricCompoundRel).where(MetricCompoundRel.id.in_(ids))
    session.execute(stmt)
    # ⚠️ 事务提交/回滚由调用方统一管理


def get_metric_compound_rel_by_id(session: Session, id: int) -> Optional[MetricCompoundRelInfo]:
    """
    根据ID查询复合指标子指标关联

    Args:
        session: 数据库会话
        id: 记录ID

    Returns:
        复合指标子指标关联信息对象
    """
    metric_compound_rel = session.query(MetricCompoundRel).filter(MetricCompoundRel.id == id).first()

    if not metric_compound_rel:
        return None

    return MetricCompoundRelInfo(
        id=metric_compound_rel.id,
        metric_id=metric_compound_rel.metric_id,
        sub_metric_id=metric_compound_rel.sub_metric_id,
        cal_operator=metric_compound_rel.cal_operator,
        sort=metric_compound_rel.sort
    )


def get_metric_compound_rel_by_metric_id(session: Session, metric_id: str) -> List[MetricCompoundRelInfo]:
    """
    根据复合指标ID查询所有子指标关联

    Args:
        session: 数据库会话
        metric_id: 复合指标ID

    Returns:
        复合指标子指标关联列表
    """
    results = session.query(MetricCompoundRel).filter(
        MetricCompoundRel.metric_id == metric_id
    ).order_by(MetricCompoundRel.sort).all()

    _list = []
    for rel in results:
        _list.append(MetricCompoundRelInfo(
            id=rel.id,
            metric_id=rel.metric_id,
            sub_metric_id=rel.sub_metric_id,
            cal_operator=rel.cal_operator,
            sort=rel.sort
        ))

    return _list


def page_metric_compound_rel(session: Session, current_page: int = 1, page_size: int = 10,
                          metric_id: Optional[str] = None):
    """
    分页查询复合指标子指标关联

    Args:
        session: 数据库会话
        current_page: 当前页码
        page_size: 每页数量
        metric_id: 复合指标ID（可选）

    Returns:
        分页结果
    """
    # 构建查询条件
    conditions = []
    if metric_id and metric_id.strip():
        conditions.append(MetricCompoundRel.metric_id == metric_id.strip())

    # 查询总数
    if conditions:
        count_stmt = select(func.count()).select_from(MetricCompoundRel).where(and_(*conditions))
    else:
        count_stmt = select(func.count()).select_from(MetricCompoundRel)

    total_count = session.execute(count_stmt).scalar()

    # 分页处理
    page_size = max(10, page_size)
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
    current_page = max(1, min(current_page, total_pages)) if total_pages > 0 else 1

    # 查询数据
    stmt = select(MetricCompoundRel)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.order_by(MetricCompoundRel.sort)
    stmt = stmt.offset((current_page - 1) * page_size).limit(page_size)

    results = session.execute(stmt).scalars().all()

    _list = []
    for rel in results:
        _list.append(MetricCompoundRelInfo(
            id=rel.id,
            metric_id=rel.metric_id,
            sub_metric_id=rel.sub_metric_id,
            cal_operator=rel.cal_operator,
            sort=rel.sort
        ))

    return current_page, page_size, total_count, total_pages, _list


def get_all_metric_compound_rel(session: Session,
                            metric_id: Optional[str] = None):
    """
    获取所有复合指标子指标关联（不分页）

    Args:
        session: 数据库会话
        metric_id: 复合指标ID（可选）

    Returns:
        复合指标子指标关联列表
    """
    conditions = []
    if metric_id and metric_id.strip():
        conditions.append(MetricCompoundRel.metric_id == metric_id.strip())

    stmt = select(MetricCompoundRel)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.order_by(MetricCompoundRel.sort)

    results = session.execute(stmt).scalars().all()

    _list = []
    for rel in results:
        _list.append(MetricCompoundRelInfo(
            id=rel.id,
            metric_id=rel.metric_id,
            sub_metric_id=rel.sub_metric_id,
            cal_operator=rel.cal_operator,
            sort=rel.sort
        ))

    return _list