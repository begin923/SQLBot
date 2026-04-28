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
    table_comment: Optional[str] = None,
    file_path: Optional[str] = None  # ⚠️ 新增参数
) -> str:
    """
    获取或创建表元数据（如果已存在则更新）
    ⚠️ 使用 SQL 执行而不是 ORM，确保与批次回滚机制兼容
    
    Args:
        session: 数据库会话
        table_name: 物理表名
        source_level: 分层（ADS/DWS/DWD）
        biz_domain: 业务域
        table_comment: 表注释
        file_path: SQL文件路径  # ⚠️ 新增参数
        
    Returns:
        table_metadata_id: 表元数据ID
    """
    from sqlalchemy import text
    
    # 查询是否已存在
    existing = session.query(TableMetadata).filter(
        TableMetadata.table_name == table_name
    ).first()
    
    now = get_now_utc8()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    
    if existing:
        # 更新已有记录（使用 SQL）
        updates = []
        if source_level is not None:
            updates.append(f"source_level = '{source_level}'")
        if biz_domain is not None:
            updates.append(f"biz_domain = '{biz_domain}'")
        if table_comment is not None:
            # 转义单引号
            escaped_comment = table_comment.replace("'", "''")
            updates.append(f"table_comment = '{escaped_comment}'")
        if file_path is not None:  # ⚠️ 更新 file_path
            escaped_path = file_path.replace("'", "''")
            updates.append(f"file_path = '{escaped_path}'")
        updates.append(f"modify_time = '{now_str}'")
        
        if updates:
            update_sql = f"UPDATE table_metadata SET {', '.join(updates)} WHERE table_name = '{table_name}'"
            session.execute(text(update_sql))
        
        return existing.id
    else:
        # 创建新记录（使用 SQL）
        id_gen = IdGenerator(session, 'table_metadata', 'TM')
        new_id = id_gen.get_next_id()
        
        # 转义特殊字符
        escaped_source_level = f"'{source_level}'" if source_level else "NULL"
        escaped_biz_domain = f"'{biz_domain}'" if biz_domain else "NULL"
        escaped_table_comment = f"'{table_comment.replace(chr(39), chr(39)+chr(39))}'" if table_comment else "NULL"
        escaped_file_path = f"'{file_path.replace(chr(39), chr(39)+chr(39))}'" if file_path else "NULL"
        
        insert_sql = f"""
            INSERT INTO table_metadata (id, table_name, source_level, biz_domain, table_comment, file_path, create_time, modify_time)
            VALUES ('{new_id}', '{table_name}', {escaped_source_level}, {escaped_biz_domain}, {escaped_table_comment}, {escaped_file_path}, '{now_str}', '{now_str}')
        """
        session.execute(text(insert_sql))
        
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
