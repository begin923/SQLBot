from datetime import datetime
from typing import List, Optional
from sqlalchemy import and_, or_, select, insert, update, delete, text, func
from sqlmodel import Session

from apps.extend.metrics2.models.metric_source_mapping_model import MetricSourceMapping, MetricSourceMappingInfo


def create_metric_source_mapping(session: Session, info: MetricSourceMappingInfo):
    """
    创建单个指标源映射记录

    Args:
        session: 数据库会话
        info: 指标源映射信息对象

    Returns:
        创建的记录 ID
    """
    # 基本验证
    if not info.metric_id or not info.metric_id.strip():
        raise Exception("指标ID不能为空")

    if not info.datasource or not info.datasource.strip():
        raise Exception("数据源标识不能为空")

    if not info.db_table or not info.db_table.strip():
        raise Exception("物理库表名不能为空")

    if not info.source_type or not info.source_type.strip():
        raise Exception("数据源类型不能为空")

    # 检查是否已存在
    exists_query = session.query(MetricSourceMapping).filter(
        and_(
            MetricSourceMapping.metric_id == info.metric_id.strip(),
            MetricSourceMapping.datasource == info.datasource.strip(),
            MetricSourceMapping.db_table == info.db_table.strip()
        )
    ).first()

    if exists_query:
        raise Exception(f"指标源映射已存在")

    # 创建记录
    metric_source_mapping = MetricSourceMapping(
        map_id=info.map_id.strip() if info.map_id else None,
        metric_id=info.metric_id.strip(),
        source_type=info.source_type.strip(),
        datasource=info.datasource.strip(),
        db_table=info.db_table.strip(),
        metric_column=info.metric_column.strip() if info.metric_column else None,
        filter_condition=info.filter_condition.strip() if info.filter_condition else None,
        agg_func=info.agg_func.strip() if info.agg_func else None,
        priority=info.priority if info.priority is not None else 1,
        is_valid=1 if info.is_valid else 0,
        source_level=info.source_level.strip() if info.source_level else "AUTHORITY"
    )

    session.add(metric_source_mapping)
    session.flush()
    session.refresh(metric_source_mapping)

    # ⚠️ 事务提交/回滚由调用方统一管理

    return metric_source_mapping.map_id


def batch_create_metric_source_mapping(session: Session, info_list: List[MetricSourceMappingInfo]):
    """
    批量创建指标源映射记录

    Args:
        session: 数据库会话
        info_list: 指标源映射信息列表

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
            info.datasource.strip().lower() if info.datasource else '',
            info.db_table.strip().lower() if info.db_table else ''
        )

        if unique_key in unique_key_set:
            duplicate_count += 1
            continue

        unique_key_set.add(unique_key)
        deduplicated_list.append(info)

    # 批量插入
    for info in deduplicated_list:
        try:
            map_id = create_metric_source_mapping(session, info)
            inserted_ids.append(map_id)
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


def update_metric_source_mapping(session: Session, info: MetricSourceMappingInfo):
    """
    更新指标源映射记录

    Args:
        session: 数据库会话
        info: 指标源映射信息对象

    Returns:
        更新的记录 ID
    """
    if not info.map_id:
        raise Exception("ID 不能为空")

    count = session.query(MetricSourceMapping).filter(
        MetricSourceMapping.map_id == info.map_id
    ).count()

    if count == 0:
        raise Exception("指标源映射不存在")

    stmt = update(MetricSourceMapping).where(
        MetricSourceMapping.map_id == info.map_id
    ).values(
        metric_id=info.metric_id.strip() if info.metric_id else None,
        source_type=info.source_type.strip() if info.source_type else None,
        datasource=info.datasource.strip() if info.datasource else None,
        db_table=info.db_table.strip() if info.db_table else None,
        metric_column=info.metric_column.strip() if info.metric_column else None,
        filter_condition=info.filter_condition.strip() if info.filter_condition else None,
        agg_func=info.agg_func.strip() if info.agg_func else None,
        priority=info.priority if info.priority is not None else 1,
        is_valid=1 if info.is_valid else 0,
        source_level=info.source_level.strip() if info.source_level else "AUTHORITY"
    )

    session.execute(stmt)
    # ⚠️ 事务提交/回滚由调用方统一管理

    return info.map_id


def delete_metric_source_mapping(session: Session, map_ids: List[str]):
    """
    删除指标源映射记录

    Args:
        session: 数据库会话
        map_ids: 要删除的映射ID列表
    """
    stmt = delete(MetricSourceMapping).where(MetricSourceMapping.map_id.in_(map_ids))
    session.execute(stmt)
    # ⚠️ 事务提交/回滚由调用方统一管理


def get_metric_source_mapping_by_id(session: Session, map_id: str) -> Optional[MetricSourceMappingInfo]:
    """
    根据ID查询指标源映射

    Args:
        session: 数据库会话
        map_id: 映射ID

    Returns:
        指标源映射信息对象
    """
    metric_source_mapping = session.query(MetricSourceMapping).filter(MetricSourceMapping.map_id == map_id).first()

    if not metric_source_mapping:
        return None

    return MetricSourceMappingInfo(
        map_id=metric_source_mapping.map_id,
        metric_id=metric_source_mapping.metric_id,
        source_type=metric_source_mapping.source_type,
        datasource=metric_source_mapping.datasource,
        db_table=metric_source_mapping.db_table,
        metric_column=metric_source_mapping.metric_column,
        filter_condition=metric_source_mapping.filter_condition,
        agg_func=metric_source_mapping.agg_func,
        priority=metric_source_mapping.priority,
        is_valid=bool(metric_source_mapping.is_valid),
        source_level=metric_source_mapping.source_level
    )


def get_metric_source_mapping_by_metric_id(session: Session, metric_id: str) -> List[MetricSourceMappingInfo]:
    """
    根据指标ID查询所有源映射

    Args:
        session: 数据库会话
        metric_id: 指标ID

    Returns:
        指标源映射列表
    """
    results = session.query(MetricSourceMapping).filter(
        MetricSourceMapping.metric_id == metric_id
    ).order_by(MetricSourceMapping.priority).all()

    _list = []
    for mapping in results:
        _list.append(MetricSourceMappingInfo(
            map_id=mapping.map_id,
            metric_id=mapping.metric_id,
            source_type=mapping.source_type,
            datasource=mapping.datasource,
            db_table=mapping.db_table,
            metric_column=mapping.metric_column,
            filter_condition=mapping.filter_condition,
            agg_func=mapping.agg_func,
            priority=mapping.priority,
            is_valid=bool(mapping.is_valid),
            source_level=mapping.source_level
        ))

    return _list


def page_metric_source_mapping(session: Session, current_page: int = 1, page_size: int = 10,
                            metric_id: Optional[str] = None,
                            datasource: Optional[str] = None,
                            source_type: Optional[str] = None):
    """
    分页查询指标源映射

    Args:
        session: 数据库会话
        current_page: 当前页码
        page_size: 每页数量
        metric_id: 指标ID（可选）
        datasource: 数据源标识（可选）
        source_type: 数据源类型（可选）

    Returns:
        分页结果
    """
    # 构建查询条件
    conditions = []
    if metric_id and metric_id.strip():
        conditions.append(MetricSourceMapping.metric_id == metric_id.strip())
    if datasource and datasource.strip():
        conditions.append(MetricSourceMapping.datasource == datasource.strip())
    if source_type and source_type.strip():
        conditions.append(MetricSourceMapping.source_type == source_type.strip())

    # 查询总数
    if conditions:
        count_stmt = select(func.count()).select_from(MetricSourceMapping).where(and_(*conditions))
    else:
        count_stmt = select(func.count()).select_from(MetricSourceMapping)

    total_count = session.execute(count_stmt).scalar()

    # 分页处理
    page_size = max(10, page_size)
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
    current_page = max(1, min(current_page, total_pages)) if total_pages > 0 else 1

    # 查询数据
    stmt = select(MetricSourceMapping)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.order_by(MetricSourceMapping.priority)
    stmt = stmt.offset((current_page - 1) * page_size).limit(page_size)

    results = session.execute(stmt).scalars().all()

    _list = []
    for mapping in results:
        _list.append(MetricSourceMappingInfo(
            map_id=mapping.map_id,
            metric_id=mapping.metric_id,
            source_type=mapping.source_type,
            datasource=mapping.datasource,
            db_table=mapping.db_table,
            metric_column=mapping.metric_column,
            filter_condition=mapping.filter_condition,
            agg_func=mapping.agg_func,
            priority=mapping.priority,
            is_valid=bool(mapping.is_valid),
            source_level=mapping.source_level
        ))

    return current_page, page_size, total_count, total_pages, _list


def get_all_metric_source_mapping(session: Session,
                              metric_id: Optional[str] = None,
                              datasource: Optional[str] = None,
                              source_type: Optional[str] = None):
    """
    获取所有指标源映射（不分页）

    Args:
        session: 数据库会话
        metric_id: 指标ID（可选）
        datasource: 数据源标识（可选）
        source_type: 数据源类型（可选）

    Returns:
        指标源映射列表
    """
    conditions = []
    if metric_id and metric_id.strip():
        conditions.append(MetricSourceMapping.metric_id == metric_id.strip())
    if datasource and datasource.strip():
        conditions.append(MetricSourceMapping.datasource == datasource.strip())
    if source_type and source_type.strip():
        conditions.append(MetricSourceMapping.source_type == source_type.strip())

    stmt = select(MetricSourceMapping)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.order_by(MetricSourceMapping.priority)

    results = session.execute(stmt).scalars().all()

    _list = []
    for mapping in results:
        _list.append(MetricSourceMappingInfo(
            map_id=mapping.map_id,
            metric_id=mapping.metric_id,
            source_type=mapping.source_type,
            datasource=mapping.datasource,
            db_table=mapping.db_table,
            metric_column=mapping.metric_column,
            filter_condition=mapping.filter_condition,
            agg_func=mapping.agg_func,
            priority=mapping.priority,
            is_valid=bool(mapping.is_valid),
            source_level=mapping.source_level
        ))

    return _list