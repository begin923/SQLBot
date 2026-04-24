"""
表元数据 CURD 操作
"""

from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from apps.extend.metrics2.models.table_metadata_model import TableMetadata
from apps.extend.metrics2.utils.id_generator import IdGenerator
from apps.extend.metrics2.utils.timezone_helper import get_now_utc8


def get_or_create_table_metadata(
    session: Session,
    table_name: str,
    source_level: Optional[str] = None,
    biz_domain: Optional[str] = None,
    table_comment: Optional[str] = None
) -> str:
    """
    获取或创建表元数据（如果已存在则更新）
    
    Args:
        session: 数据库会话
        table_name: 物理表名
        source_level: 分层（ADS/DWS/DWD）
        biz_domain: 业务域
        table_comment: 表注释
        
    Returns:
        table_metadata_id: 表元数据ID
    """
    # 查询是否已存在
    existing = session.query(TableMetadata).filter(
        TableMetadata.table_name == table_name
    ).first()
    
    now = get_now_utc8()
    
    if existing:
        # 更新已有记录
        if source_level is not None:
            existing.source_level = source_level
        if biz_domain is not None:
            existing.biz_domain = biz_domain
        if table_comment is not None:
            existing.table_comment = table_comment
        existing.modify_time = now
        
        return existing.id
    else:
        # 创建新记录
        id_gen = IdGenerator(session, 'table_metadata', 'TM')
        new_id = id_gen.get_next_id()
        
        new_metadata = TableMetadata(
            id=new_id,
            table_name=table_name,
            source_level=source_level,
            biz_domain=biz_domain,
            table_comment=table_comment,
            create_time=now,
            modify_time=now
        )
        session.add(new_metadata)
        
        return new_id


def get_table_metadata_by_name(session: Session, table_name: str) -> Optional[TableMetadata]:
    """
    根据表名获取表元数据
    
    Args:
        session: 数据库会话
        table_name: 物理表名
        
    Returns:
        TableMetadata 对象或 None
    """
    return session.query(TableMetadata).filter(
        TableMetadata.table_name == table_name
    ).first()


def get_table_metadata_by_id(session: Session, table_metadata_id: str) -> Optional[TableMetadata]:
    """
    根据 ID 获取表元数据
    
    Args:
        session: 数据库会话
        table_metadata_id: 表元数据ID
        
    Returns:
        TableMetadata 对象或 None
    """
    return session.query(TableMetadata).filter(
        TableMetadata.id == table_metadata_id
    ).first()
