from datetime import datetime
from typing import List, Optional
from sqlalchemy import and_, or_, select, insert, update, delete, text, func
from sqlmodel import Session

from apps.extend.metrics2.models.table_lineage_model import TableLineage, TableLineageInfo


def create_table_lineage(session: Session, info: TableLineageInfo):
    """创建表级血缘记录"""
    if not info.source_table or not info.source_table.strip():
        raise Exception("上游表不能为空")

    if not info.target_table or not info.target_table.strip():
        raise Exception("下游表不能为空")

    # 检查是否已存在
    exists_query = session.query(TableLineage).filter(
        and_(
            TableLineage.source_table == info.source_table.strip(),
            TableLineage.target_table == info.target_table.strip()
        )
    ).first()

    if exists_query:
        raise Exception(f"表级血缘关系已存在")

    lineage = TableLineage(
        lineage_id=info.lineage_id.strip() if info.lineage_id else None,
        source_table=info.source_table.strip(),
        target_table=info.target_table.strip()
    )

    session.add(lineage)
    session.flush()
    session.refresh(lineage)
    session.commit()

    return lineage.lineage_id


def batch_create_table_lineage(session: Session, info_list: List[TableLineageInfo]):
    """批量创建表级血缘记录"""
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
            info.source_table.strip().lower() if info.source_table else '',
            info.target_table.strip().lower() if info.target_table else ''
        )

        if unique_key in unique_key_set:
            duplicate_count += 1
            continue

        unique_key_set.add(unique_key)
        deduplicated_list.append(info)

    for info in deduplicated_list:
        try:
            lineage_id = create_table_lineage(session, info)
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


def delete_table_lineage(session: Session, lineage_ids: List[str]):
    """删除表级血缘记录"""
    stmt = delete(TableLineage).where(TableLineage.lineage_id.in_(lineage_ids))
    session.execute(stmt)
    session.commit()


def get_table_lineage_by_id(session: Session, lineage_id: str) -> Optional[TableLineageInfo]:
    """根据ID查询表级血缘"""
    table_lineage = session.query(TableLineage).filter(TableLineage.lineage_id == lineage_id).first()

    if not table_lineage:
        return None

    return TableLineageInfo(
        lineage_id=table_lineage.lineage_id,
        source_table=table_lineage.source_table,
        target_table=table_lineage.target_table
    )


def get_table_lineage_by_target(session: Session, target_table: str) -> List[TableLineageInfo]:
    """根据下游表查询所有上游血缘"""
    results = session.query(TableLineage).filter(
        TableLineage.target_table == target_table
    ).all()

    _list = []
    for lineage in results:
        _list.append(TableLineageInfo(
            lineage_id=lineage.lineage_id,
            source_table=lineage.source_table,
            target_table=lineage.target_table
        ))

    return _list


def get_table_lineage_by_source(session: Session, source_table: str) -> List[TableLineageInfo]:
    """根据上游表查询所有下游血缘"""
    results = session.query(TableLineage).filter(
        TableLineage.source_table == source_table
    ).all()

    _list = []
    for lineage in results:
        _list.append(TableLineageInfo(
            lineage_id=lineage.lineage_id,
            source_table=lineage.source_table,
            target_table=lineage.target_table
        ))

    return _list


def get_all_table_lineage(session: Session):
    """获取所有表级血缘"""
    results = session.query(TableLineage).all()

    _list = []
    for lineage in results:
        _list.append(TableLineageInfo(
            lineage_id=lineage.lineage_id,
            source_table=lineage.source_table,
            target_table=lineage.target_table
        ))

    return _list
