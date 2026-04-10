from datetime import datetime
from typing import List, Optional
from sqlalchemy import and_, or_, select, insert, update, delete, text, func
from sqlmodel import Session

from apps.extend.metrics2.models.dim_version_model import DimVersion, DimVersionInfo


def create_dim_version(session: Session, info: DimVersionInfo):
    """
    创建维度版本记录

    Args:
        session: 数据库会话
        info: 维度版本信息对象

    Returns:
        创建的版本 ID
    """
    if not info.dim_id or not info.dim_id.strip():
        raise Exception("维度ID不能为空")

    if not info.dim_name or not info.dim_name.strip():
        raise Exception("维度名称不能为空")

    if info.version is None or info.version < 1:
        raise Exception("版本号必须大于0")

    if not info.effective_time:
        raise Exception("生效时间不能为空")

    # 将旧版本标记为非当前版本
    if info.is_current:
        stmt = update(DimVersion).where(
            and_(
                DimVersion.dim_id == info.dim_id.strip(),
                DimVersion.is_current == 1
            )
        ).values(is_current=0)
        session.execute(stmt)

    # 创建新版本
    version = DimVersion(
        version_id=info.version_id.strip() if info.version_id else None,
        dim_id=info.dim_id.strip(),
        dim_name=info.dim_name.strip(),
        version=info.version,
        effective_time=info.effective_time,
        is_current=1 if info.is_current else 0
    )

    session.add(version)
    session.flush()
    session.refresh(version)
    session.commit()

    return version.version_id


def batch_create_dim_version(session: Session, info_list: List[DimVersionInfo]):
    """批量创建维度版本记录"""
    if not info_list:
        return {'success_count': 0, 'failed_records': [], 'original_count': 0}

    failed_records = []
    success_count = 0
    inserted_ids = []

    for info in info_list:
        try:
            version_id = create_dim_version(session, info)
            inserted_ids.append(version_id)
            success_count += 1
        except Exception as e:
            failed_records.append({'data': info, 'errors': [str(e)]})

    return {
        'success_count': success_count,
        'failed_records': failed_records,
        'original_count': len(info_list)
    }


def update_dim_version(session: Session, info: DimVersionInfo):
    """更新维度版本记录"""
    if not info.version_id:
        raise Exception("版本ID不能为空")

    count = session.query(DimVersion).filter(DimVersion.version_id == info.version_id).count()
    if count == 0:
        raise Exception("维度版本不存在")

    stmt = update(DimVersion).where(
        DimVersion.version_id == info.version_id
    ).values(
        dim_name=info.dim_name.strip() if info.dim_name else None,
        effective_time=info.effective_time if info.effective_time else None,
        is_current=1 if info.is_current else 0
    )

    session.execute(stmt)
    session.commit()

    return info.version_id


def delete_dim_version(session: Session, version_ids: List[str]):
    """删除维度版本记录"""
    stmt = delete(DimVersion).where(DimVersion.version_id.in_(version_ids))
    session.execute(stmt)
    session.commit()


def get_dim_version_by_id(session: Session, version_id: str) -> Optional[DimVersionInfo]:
    """根据ID查询维度版本"""
    dim_version = session.query(DimVersion).filter(DimVersion.version_id == version_id).first()

    if not dim_version:
        return None

    return DimVersionInfo(
        version_id=dim_version.version_id,
        dim_id=dim_version.dim_id,
        dim_name=dim_version.dim_name,
        version=dim_version.version,
        effective_time=dim_version.effective_time,
        is_current=bool(dim_version.is_current)
    )


def get_dim_versions_by_dim_id(session: Session, dim_id: str) -> List[DimVersionInfo]:
    """根据维度ID查询所有版本"""
    results = session.query(DimVersion).filter(
        DimVersion.dim_id == dim_id
    ).order_by(DimVersion.version.desc()).all()

    _list = []
    for version in results:
        _list.append(DimVersionInfo(
            version_id=version.version_id,
            dim_id=version.dim_id,
            dim_name=version.dim_name,
            version=version.version,
            effective_time=version.effective_time,
            is_current=bool(version.is_current)
        ))

    return _list


def get_current_version_by_dim_id(session: Session, dim_id: str) -> Optional[DimVersionInfo]:
    """获取维度的当前版本"""
    dim_version = session.query(DimVersion).filter(
        and_(
            DimVersion.dim_id == dim_id,
            DimVersion.is_current == 1
        )
    ).first()

    if not dim_version:
        return None

    return DimVersionInfo(
        version_id=dim_version.version_id,
        dim_id=dim_version.dim_id,
        dim_name=dim_version.dim_name,
        version=dim_version.version,
        effective_time=dim_version.effective_time,
        is_current=bool(dim_version.is_current)
    )


def page_dim_version(session: Session, current_page: int = 1, page_size: int = 10,
                     dim_id: Optional[str] = None):
    """分页查询维度版本"""
    conditions = []
    if dim_id and dim_id.strip():
        conditions.append(DimVersion.dim_id == dim_id.strip())

    if conditions:
        count_stmt = select(func.count()).select_from(DimVersion).where(and_(*conditions))
    else:
        count_stmt = select(func.count()).select_from(DimVersion)

    total_count = session.execute(count_stmt).scalar()

    page_size = max(10, page_size)
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
    current_page = max(1, min(current_page, total_pages)) if total_pages > 0 else 1

    stmt = select(DimVersion)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.order_by(DimVersion.version.desc())
    stmt = stmt.offset((current_page - 1) * page_size).limit(page_size)

    results = session.execute(stmt).scalars().all()

    _list = []
    for version in results:
        _list.append(DimVersionInfo(
            version_id=version.version_id,
            dim_id=version.dim_id,
            dim_name=version.dim_name,
            version=version.version,
            effective_time=version.effective_time,
            is_current=bool(version.is_current)
        ))

    return current_page, page_size, total_count, total_pages, _list


def get_all_dim_version(session: Session, dim_id: Optional[str] = None):
    """获取所有维度版本（不分页）"""
    conditions = []
    if dim_id and dim_id.strip():
        conditions.append(DimVersion.dim_id == dim_id.strip())

    stmt = select(DimVersion)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.order_by(DimVersion.version.desc())

    results = session.execute(stmt).scalars().all()

    _list = []
    for version in results:
        _list.append(DimVersionInfo(
            version_id=version.version_id,
            dim_id=version.dim_id,
            dim_name=version.dim_name,
            version=version.version,
            effective_time=version.effective_time,
            is_current=bool(version.is_current)
        ))

    return _list
