from datetime import datetime
from typing import List, Optional
from sqlalchemy import and_, or_, select, insert, update, delete, text, func
from sqlmodel import Session

from apps.extend.metrics2.models.metric_lineage_model import MetricLineage, MetricLineageInfo


def create_metric_lineage(session: Session, info: MetricLineageInfo):
    """创建指标血缘记录"""
    if not info.metric_id or not info.metric_id.strip():
        raise Exception("指标ID不能为空")

    if not info.map_id or not info.map_id.strip():
        raise Exception("映射ID不能为空")

    # 检查是否已存在
    exists_query = session.query(MetricLineage).filter(
        and_(
            MetricLineage.metric_id == info.metric_id.strip(),
            MetricLineage.map_id == info.map_id.strip()
        )
    ).first()

    if exists_query:
        raise Exception(f"指标血缘关系已存在")

    lineage = MetricLineage(
        metric_id=info.metric_id.strip(),
        map_id=info.map_id.strip()
    )

    session.add(lineage)
    session.flush()
    session.refresh(lineage)
    # ⚠️ 事务提交/回滚由调用方统一管理

    return lineage.id


def batch_create_metric_lineage(session: Session, info_list: List[MetricLineageInfo]):
    """批量创建指标血缘记录"""
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
            info.metric_id.strip().lower() if info.metric_id else '',
            info.map_id.strip().lower() if info.map_id else ''
        )

        if unique_key in unique_key_set:
            duplicate_count += 1
            continue

        unique_key_set.add(unique_key)
        deduplicated_list.append(info)

    for info in deduplicated_list:
        try:
            lineage_id = create_metric_lineage(session, info)
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


def delete_metric_lineage(session: Session, ids: List[int]):
    """删除指标血缘记录"""
    stmt = delete(MetricLineage).where(MetricLineage.id.in_(ids))
    session.execute(stmt)
    # ⚠️ 事务提交/回滚由调用方统一管理


def get_metric_lineage_by_id(session: Session, id: int) -> Optional[MetricLineageInfo]:
    """根据ID查询指标血缘"""
    metric_lineage = session.query(MetricLineage).filter(MetricLineage.id == id).first()

    if not metric_lineage:
        return None

    return MetricLineageInfo(
        id=metric_lineage.id,
        metric_id=metric_lineage.metric_id,
        map_id=metric_lineage.map_id
    )


def get_metric_lineage_by_metric_id(session: Session, metric_id: str) -> List[MetricLineageInfo]:
    """根据指标ID查询所有血缘映射"""
    results = session.query(MetricLineage).filter(
        MetricLineage.metric_id == metric_id
    ).all()

    _list = []
    for lineage in results:
        _list.append(MetricLineageInfo(
            id=lineage.id,
            metric_id=lineage.metric_id,
            map_id=lineage.map_id
        ))

    return _list


def get_all_metric_lineage(session: Session, metric_id: Optional[str] = None):
    """获取所有指标血缘"""
    conditions = []
    if metric_id and metric_id.strip():
        conditions.append(MetricLineage.metric_id == metric_id.strip())

    stmt = select(MetricLineage)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    results = session.execute(stmt).scalars().all()

    _list = []
    for lineage in results:
        _list.append(MetricLineageInfo(
            id=lineage.id,
            metric_id=lineage.metric_id,
            map_id=lineage.map_id
        ))

    return _list
