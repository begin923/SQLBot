from datetime import datetime
from typing import List, Optional
from sqlalchemy import and_, or_, select, insert, update, delete, text, func
from sqlmodel import Session

from apps.extend.metrics2.models.field_lineage_model import FieldLineage, FieldLineageInfo


def create_field_lineage(session: Session, info: FieldLineageInfo):
    """创建字段级血缘记录"""
    if not info.lineage_id or not info.lineage_id.strip():
        raise Exception("字段血缘ID不能为空")

    if not info.table_lineage_id or not info.table_lineage_id.strip():
        raise Exception("表血缘ID不能为空")

    if not info.source_table or not info.source_table.strip():
        raise Exception("上游表不能为空")

    if not info.source_field or not info.source_field.strip():
        raise Exception("上游字段不能为空")

    if not info.target_table or not info.target_table.strip():
        raise Exception("下游表不能为空")

    if not info.target_field or not info.target_field.strip():
        raise Exception("下游字段不能为空")

    # 检查是否已存在
    exists_query = session.query(FieldLineage).filter(
        FieldLineage.lineage_id == info.lineage_id.strip()
    ).first()

    if exists_query:
        raise Exception(f"字段级血缘关系已存在: {info.lineage_id}")

    lineage = FieldLineage(
        lineage_id=info.lineage_id.strip(),
        table_lineage_id=info.table_lineage_id.strip(),
        source_table=info.source_table.strip(),
        source_field=info.source_field.strip(),
        target_table=info.target_table.strip(),
        target_field=info.target_field.strip(),
        target_field_mark=info.target_field_mark.strip() if info.target_field_mark else 'normal',
        dim_id=info.dim_id.strip() if info.dim_id else None
    )

    session.add(lineage)
    session.flush()
    session.refresh(lineage)
    session.commit()

    return lineage.lineage_id


def batch_create_field_lineage(session: Session, info_list: List[FieldLineageInfo]):
    """批量创建字段级血缘记录"""
    if not info_list:
        return {'success_count': 0, 'failed_records': [], 'duplicate_count': 0, 'original_count': 0}

    failed_records = []
    success_count = 0
    inserted_ids = []
    duplicate_count = 0

    unique_key_set = set()
    deduplicated_list = []

    for info in info_list:
        unique_key = (
            info.lineage_id.strip().lower() if info.lineage_id else '',
            info.source_field.strip().lower() if info.source_field else '',
            info.target_field.strip().lower() if info.target_field else ''
        )

        if unique_key in unique_key_set:
            duplicate_count += 1
            continue

        unique_key_set.add(unique_key)
        deduplicated_list.append(info)

    for info in deduplicated_list:
        try:
            lineage_id = create_field_lineage(session, info)
            inserted_ids.append(lineage_id)
            success_count += 1
        except Exception as e:
            failed_records.append({'data': info, 'errors': [str(e)]})

    return {
        'success_count': success_count,
        'failed_records': failed_records,
        'duplicate_count': duplicate_count,
        'original_count': len(info_list),
        'deduplicated_count': len(deduplicated_list)
    }


def delete_field_lineage(session: Session, lineage_ids: List[str]):
    """删除字段级血缘记录"""
    stmt = delete(FieldLineage).where(FieldLineage.lineage_id.in_(lineage_ids))
    session.execute(stmt)
    session.commit()


def get_field_lineage_by_id(session: Session, lineage_id: str) -> Optional[FieldLineageInfo]:
    """根据ID查询字段级血缘"""
    field_lineage = session.query(FieldLineage).filter(
        FieldLineage.lineage_id == lineage_id
    ).first()

    if not field_lineage:
        return None

    return FieldLineageInfo(
        lineage_id=field_lineage.lineage_id,
        table_lineage_id=field_lineage.table_lineage_id,
        source_table=field_lineage.source_table,
        source_field=field_lineage.source_field,
        target_table=field_lineage.target_table,
        target_field=field_lineage.target_field,
        target_field_mark=field_lineage.target_field_mark,
        dim_id=field_lineage.dim_id
    )


def get_field_lineage_by_lineage_id(session: Session, lineage_id: str) -> List[FieldLineageInfo]:
    """根据表血缘ID查询所有字段血缘"""
    results = session.query(FieldLineage).filter(
        FieldLineage.table_lineage_id == lineage_id
    ).all()

    _list = []
    for lineage in results:
        _list.append(FieldLineageInfo(
            lineage_id=lineage.lineage_id,
            table_lineage_id=lineage.table_lineage_id,
            source_table=lineage.source_table,
            source_field=lineage.source_field,
            target_table=lineage.target_table,
            target_field=lineage.target_field,
            target_field_mark=lineage.target_field_mark,
            dim_id=lineage.dim_id
        ))

    return _list


def validate_field_exists(session: Session, db_table: str, metric_column: str) -> bool:
    """
    校验字段是否存在于血缘映射中（防SQL报错核心算法）
    
    Args:
        session: 数据库会话
        db_table: 物理表名
        metric_column: 指标字段名
    
    Returns:
        字段是否合法
    """
    count = session.query(FieldLineage).filter(
        and_(
            FieldLineage.target_table == db_table,
            FieldLineage.target_field == metric_column
        )
    ).count()

    return count >= 1


def get_field_lineage_by_target_field(session: Session, target_table: str, target_field: str) -> List[FieldLineageInfo]:
    """根据目标字段查询上游血缘"""
    results = session.query(FieldLineage).filter(
        and_(
            FieldLineage.target_table == target_table,
            FieldLineage.target_field == target_field
        )
    ).all()

    _list = []
    for lineage in results:
        _list.append(FieldLineageInfo(
            lineage_id=lineage.lineage_id,
            table_lineage_id=lineage.table_lineage_id,
            source_table=lineage.source_table,
            source_field=lineage.source_field,
            target_table=lineage.target_table,
            target_field=lineage.target_field,
            target_field_mark=lineage.target_field_mark,
            dim_id=lineage.dim_id
        ))

    return _list


def get_all_field_lineage(session: Session, table_lineage_id: Optional[str] = None):
    """获取所有字段级血缘"""
    conditions = []
    if table_lineage_id and table_lineage_id.strip():
        conditions.append(FieldLineage.table_lineage_id == table_lineage_id.strip())

    stmt = select(FieldLineage)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    results = session.execute(stmt).scalars().all()

    _list = []
    for lineage in results:
        _list.append(FieldLineageInfo(
            lineage_id=lineage.lineage_id,
            table_lineage_id=lineage.table_lineage_id,
            source_table=lineage.source_table,
            source_field=lineage.source_field,
            target_table=lineage.target_table,
            target_field=lineage.target_field,
            target_field_mark=lineage.target_field_mark,
            dim_id=lineage.dim_id
        ))

    return _list
