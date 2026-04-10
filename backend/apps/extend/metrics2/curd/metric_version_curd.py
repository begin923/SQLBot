from datetime import datetime
from typing import List, Optional
from sqlalchemy import and_, or_, select, insert, update, delete, text, func
from sqlmodel import Session

from apps.extend.metrics2.models.metric_version_model import MetricVersion, MetricVersionInfo


def create_metric_version(session: Session, info: MetricVersionInfo):
    """
    创建指标版本记录

    Args:
        session: 数据库会话
        info: 指标版本信息对象

    Returns:
        创建的版本 ID
    """
    if not info.metric_id or not info.metric_id.strip():
        raise Exception("指标ID不能为空")

    if not info.cal_logic or not info.cal_logic.strip():
        raise Exception("口径逻辑不能为空")

    if info.version is None or info.version < 1:
        raise Exception("版本号必须大于0")

    if not info.effective_time:
        raise Exception("生效时间不能为空")

    # 将旧版本标记为非当前版本
    if info.is_current:
        stmt = update(MetricVersion).where(
            and_(
                MetricVersion.metric_id == info.metric_id.strip(),
                MetricVersion.is_current == 1
            )
        ).values(
            is_current=0,
            expire_time=datetime.now()
        )
        session.execute(stmt)

    # 创建新版本
    version = MetricVersion(
        version_id=info.version_id.strip() if info.version_id else None,
        metric_id=info.metric_id.strip(),
        cal_logic=info.cal_logic.strip(),
        version=info.version,
        effective_time=info.effective_time,
        expire_time=info.expire_time,
        is_current=1 if info.is_current else 0
    )

    session.add(version)
    session.flush()
    session.refresh(version)
    session.commit()

    return version.version_id


def batch_create_metric_version(session: Session, info_list: List[MetricVersionInfo]):
    """
    批量创建指标版本记录

    Args:
        session: 数据库会话
        info_list: 指标版本信息列表

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

    for info in info_list:
        try:
            version_id = create_metric_version(session, info)
            inserted_ids.append(version_id)
            success_count += 1
        except Exception as e:
            failed_records.append({
                'data': info,
                'errors': [str(e)]
            })

    return {
        'success_count': success_count,
        'failed_records': failed_records,
        'original_count': len(info_list)
    }


def update_metric_version(session: Session, info: MetricVersionInfo):
    """
    更新指标版本记录

    Args:
        session: 数据库会话
        info: 指标版本信息对象

    Returns:
        更新的版本 ID
    """
    if not info.version_id:
        raise Exception("版本ID不能为空")

    count = session.query(MetricVersion).filter(
        MetricVersion.version_id == info.version_id
    ).count()

    if count == 0:
        raise Exception("指标版本不存在")

    stmt = update(MetricVersion).where(
        MetricVersion.version_id == info.version_id
    ).values(
        cal_logic=info.cal_logic.strip() if info.cal_logic else None,
        effective_time=info.effective_time if info.effective_time else None,
        expire_time=info.expire_time if info.expire_time else None,
        is_current=1 if info.is_current else 0
    )

    session.execute(stmt)
    session.commit()

    return info.version_id


def delete_metric_version(session: Session, version_ids: List[str]):
    """
    删除指标版本记录

    Args:
        session: 数据库会话
        version_ids: 要删除的版本ID列表
    """
    stmt = delete(MetricVersion).where(MetricVersion.version_id.in_(version_ids))
    session.execute(stmt)
    session.commit()


def get_metric_version_by_id(session: Session, version_id: str) -> Optional[MetricVersionInfo]:
    """
    根据ID查询指标版本

    Args:
        session: 数据库会话
        version_id: 版本ID

    Returns:
        指标版本信息对象
    """
    metric_version = session.query(MetricVersion).filter(
        MetricVersion.version_id == version_id
    ).first()

    if not metric_version:
        return None

    return MetricVersionInfo(
        version_id=metric_version.version_id,
        metric_id=metric_version.metric_id,
        cal_logic=metric_version.cal_logic,
        version=metric_version.version,
        effective_time=metric_version.effective_time,
        expire_time=metric_version.expire_time,
        is_current=bool(metric_version.is_current)
    )


def get_metric_versions_by_metric_id(session: Session, metric_id: str) -> List[MetricVersionInfo]:
    """
    根据指标ID查询所有版本

    Args:
        session: 数据库会话
        metric_id: 指标ID

    Returns:
        指标版本列表
    """
    results = session.query(MetricVersion).filter(
        MetricVersion.metric_id == metric_id
    ).order_by(MetricVersion.version.desc()).all()

    _list = []
    for version in results:
        _list.append(MetricVersionInfo(
            version_id=version.version_id,
            metric_id=version.metric_id,
            cal_logic=version.cal_logic,
            version=version.version,
            effective_time=version.effective_time,
            expire_time=version.expire_time,
            is_current=bool(version.is_current)
        ))

    return _list


def get_current_version_by_metric_id(session: Session, metric_id: str) -> Optional[MetricVersionInfo]:
    """
    获取指标的当前版本

    Args:
        session: 数据库会话
        metric_id: 指标ID

    Returns:
        当前版本信息对象
    """
    metric_version = session.query(MetricVersion).filter(
        and_(
            MetricVersion.metric_id == metric_id,
            MetricVersion.is_current == 1
        )
    ).first()

    if not metric_version:
        return None

    return MetricVersionInfo(
        version_id=metric_version.version_id,
        metric_id=metric_version.metric_id,
        cal_logic=metric_version.cal_logic,
        version=metric_version.version,
        effective_time=metric_version.effective_time,
        expire_time=metric_version.expire_time,
        is_current=bool(metric_version.is_current)
    )


def get_version_by_time(session: Session, metric_id: str, query_time: datetime) -> Optional[MetricVersionInfo]:
    """
    根据查询时间匹配历史版本

    Args:
        session: 数据库会话
        metric_id: 指标ID
        query_time: 查询时间

    Returns:
        匹配的版本信息对象
    """
    metric_version = session.query(MetricVersion).filter(
        and_(
            MetricVersion.metric_id == metric_id,
            MetricVersion.effective_time <= query_time,
            or_(
                MetricVersion.expire_time.is_(None),
                MetricVersion.expire_time > query_time
            )
        )
    ).order_by(MetricVersion.version.desc()).first()

    if not metric_version:
        return None

    return MetricVersionInfo(
        version_id=metric_version.version_id,
        metric_id=metric_version.metric_id,
        cal_logic=metric_version.cal_logic,
        version=metric_version.version,
        effective_time=metric_version.effective_time,
        expire_time=metric_version.expire_time,
        is_current=bool(metric_version.is_current)
    )


def page_metric_version(session: Session, current_page: int = 1, page_size: int = 10,
                        metric_id: Optional[str] = None):
    """
    分页查询指标版本

    Args:
        session: 数据库会话
        current_page: 当前页码
        page_size: 每页数量
        metric_id: 指标ID（可选）

    Returns:
        分页结果
    """
    conditions = []
    if metric_id and metric_id.strip():
        conditions.append(MetricVersion.metric_id == metric_id.strip())

    if conditions:
        count_stmt = select(func.count()).select_from(MetricVersion).where(and_(*conditions))
    else:
        count_stmt = select(func.count()).select_from(MetricVersion)

    total_count = session.execute(count_stmt).scalar()

    page_size = max(10, page_size)
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
    current_page = max(1, min(current_page, total_pages)) if total_pages > 0 else 1

    stmt = select(MetricVersion)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.order_by(MetricVersion.version.desc())
    stmt = stmt.offset((current_page - 1) * page_size).limit(page_size)

    results = session.execute(stmt).scalars().all()

    _list = []
    for version in results:
        _list.append(MetricVersionInfo(
            version_id=version.version_id,
            metric_id=version.metric_id,
            cal_logic=version.cal_logic,
            version=version.version,
            effective_time=version.effective_time,
            expire_time=version.expire_time,
            is_current=bool(version.is_current)
        ))

    return current_page, page_size, total_count, total_pages, _list


def get_all_metric_version(session: Session, metric_id: Optional[str] = None):
    """
    获取所有指标版本（不分页）

    Args:
        session: 数据库会话
        metric_id: 指标ID（可选）

    Returns:
        指标版本列表
    """
    conditions = []
    if metric_id and metric_id.strip():
        conditions.append(MetricVersion.metric_id == metric_id.strip())

    stmt = select(MetricVersion)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.order_by(MetricVersion.version.desc())

    results = session.execute(stmt).scalars().all()

    _list = []
    for version in results:
        _list.append(MetricVersionInfo(
            version_id=version.version_id,
            metric_id=version.metric_id,
            cal_logic=version.cal_logic,
            version=version.version,
            effective_time=version.effective_time,
            expire_time=version.expire_time,
            is_current=bool(version.is_current)
        ))

    return _list
