from datetime import datetime
from typing import List, Optional
from sqlalchemy import and_, or_, select, insert, update, delete, text, func
from sqlmodel import Session

from apps.extend.metrics2.models.dim_field_lineage_model import DimFieldLineage, DimFieldLineageInfo


def create_dim_field_lineage(session: Session, info: DimFieldLineageInfo):
    """
    创建维度字段血缘记录（UPSERT模式）
    
    Args:
        session: 数据库会话
        info: 维度字段血缘信息对象
    
    Returns:
        创建的记录 ID
    """
    if not info.db_table or not info.db_table.strip():
        raise Exception("物理表不能为空")
    
    if not info.field or not info.field.strip():
        raise Exception("维度字段不能为空")
    
    if not info.dim_id or not info.dim_id.strip():
        raise Exception("维度ID不能为空")
    
    # 检查是否已存在（根据唯一索引 db_table + field）
    exists_query = session.query(DimFieldLineage).filter(
        and_(
            DimFieldLineage.db_table == info.db_table.strip(),
            DimFieldLineage.field == info.field.strip()
        )
    ).first()
    
    if exists_query:
        # 更新现有记录的 dim_id 和 field_name
        exists_query.dim_id = info.dim_id.strip()
        if info.field_name:
            exists_query.field_name = info.field_name.strip()
        session.flush()
        session.commit()
        return exists_query.id  # 返回已有ID
    else:
        # 插入新记录（需要生成ID）
        from apps.extend.metrics2.utils.id_generator import IdGenerator
        id_gen = IdGenerator(session, 'dim_field_lineage', 'D')
        new_id = id_gen.get_next_id()
        
        lineage = DimFieldLineage(
            id=new_id,
            db_table=info.db_table.strip(),
            field=info.field.strip(),
            field_name=info.field_name.strip() if info.field_name else '',
            dim_id=info.dim_id.strip()
        )
        
        session.add(lineage)
        session.flush()
        session.commit()
        return new_id


def batch_create_dim_field_lineage(session: Session, info_list: List[DimFieldLineageInfo]):
    """
    批量创建维度字段血缘记录（UPSERT模式）
    
    Args:
        session: 数据库会话
        info_list: 维度字段血缘信息列表
    
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
    duplicate_count = 0
    
    # 去重处理（基于 db_table + field）
    unique_key_set = set()
    deduplicated_list = []
    
    for info in info_list:
        unique_key = (
            info.db_table.strip().lower() if info.db_table else '',
            info.field.strip().lower() if info.field else ''
        )
        
        if unique_key in unique_key_set:
            duplicate_count += 1
            continue
        
        unique_key_set.add(unique_key)
        deduplicated_list.append(info)
    
    # 批量插入/更新
    for info in deduplicated_list:
        try:
            create_dim_field_lineage(session, info)
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


def delete_dim_field_lineage(session: Session, ids: List[str]):
    """
    根据ID删除维度字段血缘记录
    
    Args:
        session: 数据库会话
        ids: 要删除的ID列表
    """
    stmt = delete(DimFieldLineage).where(DimFieldLineage.id.in_(ids))
    session.execute(stmt)
    session.commit()


def get_dim_field_lineage_by_id(session: Session, id: str) -> Optional[DimFieldLineageInfo]:
    """
    根据ID查询维度字段血缘
    
    Args:
        session: 数据库会话
        id: 记录ID
    
    Returns:
        维度字段血缘信息对象
    """
    lineage = session.query(DimFieldLineage).filter(DimFieldLineage.id == id).first()
    
    if not lineage:
        return None
    
    return DimFieldLineageInfo(
        id=lineage.id,
        db_table=lineage.db_table,
        field=lineage.field,
        field_name=lineage.field_name,
        dim_id=lineage.dim_id
    )


def get_dim_field_lineage_by_db_table_and_field(session: Session, db_table: str, field: str) -> Optional[DimFieldLineageInfo]:
    """
    根据物理表和字段查询维度字段血缘
    
    Args:
        session: 数据库会话
        db_table: 物理表名
        field: 字段名
    
    Returns:
        维度字段血缘信息对象
    """
    lineage = session.query(DimFieldLineage).filter(
        and_(
            DimFieldLineage.db_table == db_table,
            DimFieldLineage.field == field
        )
    ).first()
    
    if not lineage:
        return None
    
    return DimFieldLineageInfo(
        id=lineage.id,
        db_table=lineage.db_table,
        field=lineage.field,
        field_name=lineage.field_name,
        dim_id=lineage.dim_id
    )


def get_dim_field_lineage_by_dim_id(session: Session, dim_id: str) -> List[DimFieldLineageInfo]:
    """
    根据维度ID查询所有字段血缘
    
    Args:
        session: 数据库会话
        dim_id: 维度ID
    
    Returns:
        维度字段血缘信息列表
    """
    results = session.query(DimFieldLineage).filter(
        DimFieldLineage.dim_id == dim_id
    ).all()
    
    _list = []
    for lineage in results:
        _list.append(DimFieldLineageInfo(
            id=lineage.id,
            db_table=lineage.db_table,
            field=lineage.field,
            field_name=lineage.field_name,
            dim_id=lineage.dim_id
        ))
    
    return _list


def get_dim_field_lineage_by_table(session: Session, db_table: str) -> List[DimFieldLineageInfo]:
    """
    根据物理表查询所有维度字段血缘
    
    Args:
        session: 数据库会话
        db_table: 物理表名
    
    Returns:
        维度字段血缘信息列表
    """
    results = session.query(DimFieldLineage).filter(
        DimFieldLineage.db_table == db_table
    ).all()
    
    _list = []
    for lineage in results:
        _list.append(DimFieldLineageInfo(
            id=lineage.id,
            db_table=lineage.db_table,
            field=lineage.field,
            field_name=lineage.field_name,
            dim_id=lineage.dim_id
        ))
    
    return _list


def get_all_dim_field_lineage(session: Session, dim_id: Optional[str] = None):
    """
    获取所有维度字段血缘
    
    Args:
        session: 数据库会话
        dim_id: 维度ID（可选过滤条件）
    
    Returns:
        维度字段血缘信息列表
    """
    conditions = []
    if dim_id and dim_id.strip():
        conditions.append(DimFieldLineage.dim_id == dim_id.strip())
    
    stmt = select(DimFieldLineage)
    if conditions:
        stmt = stmt.where(and_(*conditions))
    
    results = session.execute(stmt).scalars().all()
    
    _list = []
    for lineage in results:
        _list.append(DimFieldLineageInfo(
            id=lineage.id,
            db_table=lineage.db_table,
            field=lineage.field,
            field_name=lineage.field_name,
            dim_id=lineage.dim_id
        ))
    
    return _list
