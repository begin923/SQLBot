from datetime import datetime
from typing import List, Optional
from sqlalchemy import and_, or_, select, insert, update, delete, text, func
from sqlmodel import Session

from apps.extend.metrics2.models.dim_field_mapping_model import DimFieldMapping, DimFieldMappingInfo


def create_dim_field_mapping(session: Session, info: DimFieldMappingInfo):
    """创建维度字段映射记录（UPSERT模式）"""
    if not info.db_table or not info.db_table.strip():
        raise Exception("物理表不能为空")

    if not info.dim_field or not info.dim_field.strip():
        raise Exception("维度字段不能为空")
    
    if not info.dim_id or not info.dim_id.strip():
        raise Exception("维度ID不能为空")

    # 检查是否已存在（根据联合主键 db_table + dim_field）
    exists_query = session.query(DimFieldMapping).filter(
        and_(
            DimFieldMapping.db_table == info.db_table.strip(),
            DimFieldMapping.dim_field == info.dim_field.strip()
        )
    ).first()

    if exists_query:
        # 更新现有记录的 dim_id
        exists_query.dim_id = info.dim_id.strip()
        session.flush()
        session.commit()
        return True  # 返回更新成功
    else:
        # 插入新记录
        mapping = DimFieldMapping(
            dim_id=info.dim_id.strip(),
            db_table=info.db_table.strip(),
            dim_field=info.dim_field.strip()
        )

        session.add(mapping)
        session.flush()
        session.commit()
        return True  # 返回插入成功


def batch_create_dim_field_mapping(session: Session, info_list: List[DimFieldMappingInfo]):
    """批量创建维度字段映射记录（UPSERT模式）"""
    if not info_list:
        return {'success_count': 0, 'failed_records': [], 'duplicate_count': 0, 'original_count': 0}

    failed_records = []
    success_count = 0
    duplicate_count = 0

    unique_key_set = set()
    deduplicated_list = []

    for info in info_list:
        # 根据联合主键 (db_table, dim_field) 去重
        unique_key = (
            info.db_table.strip().lower() if info.db_table else '',
            info.dim_field.strip().lower() if info.dim_field else ''
        )

        if unique_key in unique_key_set:
            duplicate_count += 1
            continue

        unique_key_set.add(unique_key)
        deduplicated_list.append(info)

    for info in deduplicated_list:
        try:
            create_dim_field_mapping(session, info)
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


def delete_dim_field_mapping(session: Session, db_tables_and_fields: List[tuple]):
    """
    删除维度字段映射记录
    
    Args:
        session: 数据库会话
        db_tables_and_fields: 联合主键列表 [(db_table, dim_field), ...]
    """
    if not db_tables_and_fields:
        return
    
    for db_table, dim_field in db_tables_and_fields:
        stmt = delete(DimFieldMapping).where(
            and_(
                DimFieldMapping.db_table == db_table,
                DimFieldMapping.dim_field == dim_field
            )
        )
        session.execute(stmt)
    
    session.commit()


def get_dim_field_mapping_by_primary_key(session: Session, db_table: str, dim_field: str) -> Optional[DimFieldMappingInfo]:
    """根据联合主键查询维度字段映射"""
    mapping = session.query(DimFieldMapping).filter(
        and_(
            DimFieldMapping.db_table == db_table,
            DimFieldMapping.dim_field == dim_field
        )
    ).first()

    if not mapping:
        return None

    return DimFieldMappingInfo(
        dim_id=mapping.dim_id,
        db_table=mapping.db_table,
        dim_field=mapping.dim_field
    )


def get_dim_field_mapping_by_dim_id(session: Session, dim_id: str) -> List[DimFieldMappingInfo]:
    """根据维度ID查询所有字段映射"""
    results = session.query(DimFieldMapping).filter(
        DimFieldMapping.dim_id == dim_id
    ).all()

    _list = []
    for mapping in results:
        _list.append(DimFieldMappingInfo(
            dim_id=mapping.dim_id,
            db_table=mapping.db_table,
            dim_field=mapping.dim_field
        ))

    return _list


def get_dim_field_mapping_by_table(session: Session, db_table: str) -> List[DimFieldMappingInfo]:
    """根据物理表查询所有维度字段映射"""
    results = session.query(DimFieldMapping).filter(
        DimFieldMapping.db_table == db_table
    ).all()

    _list = []
    for mapping in results:
        _list.append(DimFieldMappingInfo(
            dim_id=mapping.dim_id,
            db_table=mapping.db_table,
            dim_field=mapping.dim_field
        ))

    return _list


def get_all_dim_field_mapping(session: Session, dim_id: Optional[str] = None):
    """获取所有维度字段映射"""
    conditions = []
    if dim_id and dim_id.strip():
        conditions.append(DimFieldMapping.dim_id == dim_id.strip())

    stmt = select(DimFieldMapping)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    results = session.execute(stmt).scalars().all()

    _list = []
    for mapping in results:
        _list.append(DimFieldMappingInfo(
            dim_id=mapping.dim_id,
            db_table=mapping.db_table,
            dim_field=mapping.dim_field
        ))

    return _list
