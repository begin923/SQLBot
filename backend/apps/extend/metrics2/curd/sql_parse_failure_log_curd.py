"""
SQL 解析失败日志 CURD 操作
"""
from datetime import datetime
from typing import List, Optional
from sqlmodel import Session, select, update

from apps.extend.metrics2.models.sql_parse_failure_log_model import SqlParseFailureLog


def create_failure_log(
    session: Session,
    file_path: str,
    file_name: str,
    failure_reason: str,
    layer_type: Optional[str] = None,
    error_type: Optional[str] = None,
    sql_content: Optional[str] = None,
    matched_pattern: Optional[str] = None
) -> SqlParseFailureLog:
    """
    创建或更新解析失败日志（UPSERT）
    
    如果 file_path + file_name 已存在，则更新记录；否则插入新记录。
    
    Args:
        session: 数据库会话
        file_path: SQL 文件路径
        file_name: 文件名
        failure_reason: 失败原因
        layer_type: 层级类型
        error_type: 错误类型
        sql_content: SQL 内容
        matched_pattern: 匹配到的模式
        
    Returns:
        创建或更新的日志记录
    """
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    
    # 准备数据
    now = datetime.utcnow()
    data = {
        'file_path': file_path,
        'file_name': file_name,
        'layer_type': layer_type,
        'failure_reason': failure_reason,
        'error_type': error_type,
        'sql_content': sql_content,
        'matched_pattern': matched_pattern,
        'parse_time': now,
        'is_resolved': False,
        'retry_count': 0,
        'create_time': now,
        'modify_time': now
    }
    
    # 使用 PostgreSQL 的 UPSERT 语法
    stmt = pg_insert(SqlParseFailureLog).values(**data)
    
    # 如果冲突（file_path + file_name 已存在），则更新相关字段
    update_dict = {
        'layer_type': stmt.excluded.layer_type,
        'failure_reason': stmt.excluded.failure_reason,
        'error_type': stmt.excluded.error_type,
        'sql_content': stmt.excluded.sql_content,
        'matched_pattern': stmt.excluded.matched_pattern,
        'parse_time': stmt.excluded.parse_time,
        'is_resolved': False,  # 重新失败时重置为未解决
        'resolve_time': None,  # 清空解决时间
        'retry_count': SqlParseFailureLog.retry_count + 1,  # 重试次数+1
        'modify_time': now
    }
    
    stmt = stmt.on_conflict_do_update(
        index_elements=['file_path', 'file_name'],
        set_=update_dict
    )
    
    session.execute(stmt)
    session.commit()
    
    # 查询并返回最新记录
    statement = select(SqlParseFailureLog).where(
        SqlParseFailureLog.file_path == file_path,
        SqlParseFailureLog.file_name == file_name
    )
    result = session.execute(statement)
    log = result.scalars().one()
    
    return log


def get_failure_logs(
    session: Session,
    is_resolved: Optional[bool] = None,
    error_type: Optional[str] = None,
    layer_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[SqlParseFailureLog]:
    """
    查询失败日志列表
    
    Args:
        session: 数据库会话
        is_resolved: 是否已解决
        error_type: 错误类型
        layer_type: 层级类型
        limit: 返回数量限制
        offset: 偏移量
        
    Returns:
        日志列表
    """
    statement = select(SqlParseFailureLog)
    
    if is_resolved is not None:
        statement = statement.where(SqlParseFailureLog.is_resolved == is_resolved)
    
    if error_type:
        statement = statement.where(SqlParseFailureLog.error_type == error_type)
    
    if layer_type:
        statement = statement.where(SqlParseFailureLog.layer_type == layer_type)
    
    statement = statement.order_by(SqlParseFailureLog.parse_time.desc())
    statement = statement.offset(offset).limit(limit)
    
    return session.exec(statement).all()


def mark_as_resolved(session: Session, log_id: int) -> bool:
    """
    标记为已解决
    
    Args:
        session: 数据库会话
        log_id: 日志ID
        
    Returns:
        是否成功
    """
    statement = (
        update(SqlParseFailureLog)
        .where(SqlParseFailureLog.id == log_id)
        .values(
            is_resolved=True,
            resolve_time=datetime.utcnow(),
            modify_time=datetime.utcnow()
        )
    )
    
    result = session.exec(statement)
    session.commit()
    
    return result.rowcount > 0


def increment_retry_count(session: Session, log_id: int) -> bool:
    """
    增加重试次数
    
    Args:
        session: 数据库会话
        log_id: 日志ID
        
    Returns:
        是否成功
    """
    statement = (
        update(SqlParseFailureLog)
        .where(SqlParseFailureLog.id == log_id)
        .values(
            retry_count=SqlParseFailureLog.retry_count + 1,
            modify_time=datetime.utcnow()
        )
    )
    
    result = session.exec(statement)
    session.commit()
    
    return result.rowcount > 0


def get_failure_statistics(session: Session) -> dict:
    """
    获取失败统计信息
    
    Args:
        session: 数据库会话
        
    Returns:
        统计信息字典
    """
    # 总失败数
    total_statement = select(SqlParseFailureLog)
    total_count = len(session.exec(total_statement).all())
    
    # 未解决数
    unresolved_statement = select(SqlParseFailureLog).where(
        SqlParseFailureLog.is_resolved == False
    )
    unresolved_count = len(session.exec(unresolved_statement).all())
    
    # 已解决数
    resolved_count = total_count - unresolved_count
    
    # 按错误类型统计
    error_type_stats = {}
    all_logs = session.exec(select(SqlParseFailureLog)).all()
    for log in all_logs:
        error_type = log.error_type or "UNKNOWN"
        if error_type not in error_type_stats:
            error_type_stats[error_type] = 0
        error_type_stats[error_type] += 1
    
    return {
        "total_failures": total_count,
        "unresolved_count": unresolved_count,
        "resolved_count": resolved_count,
        "resolution_rate": f"{(resolved_count / total_count * 100) if total_count > 0 else 0:.2f}%",
        "error_type_distribution": error_type_stats
    }
