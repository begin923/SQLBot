"""
SQL 解析失败日志 CURD 操作
"""
from datetime import datetime
from typing import List, Optional, Dict, Any
import logging
from sqlmodel import Session, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.dialects.postgresql import insert as pg_insert

from apps.extend.metrics2.models.sql_parse_failure_log_model import SqlParseFailureLog

logger = logging.getLogger(__name__)


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
    创建或更新解析失败日志（UPSERT）- 单条记录
    
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
    # 准备数据
    now = datetime.utcnow()
    data = {
        'file_path': str(file_path),  # ⚠️ 确保转换为字符串
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
    # ⚠️ 不调用 session.commit()，由调用方在事务中统一管理
    
    # 查询并返回最新记录
    statement = select(SqlParseFailureLog).where(
        SqlParseFailureLog.file_path == file_path,
        SqlParseFailureLog.file_name == file_name
    )
    result = session.execute(statement)
    log = result.scalars().one()
    
    return log


def batch_create_failure_logs(
    session: Session,
    failure_logs: List[Dict[str, Any]]
) -> int:
    """
    批量创建或更新解析失败日志（UPSERT）
    
    Args:
        session: 数据库会话
        failure_logs: 失败日志列表，每个元素包含：
            - file_path: 文件路径
            - file_name: 文件名
            - failure_reason: 失败原因
            - layer_type: 层级类型（可选）
            - error_type: 错误类型（可选）
            - sql_content: SQL 内容（可选）
            - matched_pattern: 匹配模式（可选）
    
    Returns:
        成功插入/更新的记录数量
    """
    if not failure_logs:
        return 0
    
    try:
        now = datetime.utcnow()
        
        # 构建批量数据
        values_list = []
        for log_data in failure_logs:
            file_path = log_data.get('file_path', '')
            file_name = log_data.get('file_name', '')
            
            if not file_path or not file_name:
                continue
            
            values_list.append({
                'file_path': str(file_path),
                'file_name': file_name,
                'layer_type': log_data.get('layer_type'),
                'failure_reason': log_data.get('failure_reason', '未知错误'),
                'error_type': log_data.get('error_type'),
                'sql_content': log_data.get('sql_content'),
                'matched_pattern': log_data.get('matched_pattern'),
                'parse_time': now,
                'is_resolved': False,
                'retry_count': 0,
                'create_time': now,
                'modify_time': now
            })
        
        if not values_list:
            return 0
        
        # 使用 PostgreSQL 的批量 UPSERT 语法
        stmt = pg_insert(SqlParseFailureLog).values(values_list)
        
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
        
        result = session.execute(stmt)
        # ⚠️ 不调用 session.commit()，由调用方在事务中统一管理
        
        inserted_count = len(values_list)
        logger.info(f"[失败日志] 批量插入 {inserted_count} 条失败记录")
        
        return inserted_count
    
    except Exception as e:
        logger.error(f"[失败日志] 批量插入失败: {str(e)}")
        raise


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
    
    return session.execute(statement).scalars().all()


def mark_as_resolved(session: Session, log_id: int) -> bool:
    """
    标记为已解决（单个）
    
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
    
    result: CursorResult = session.execute(statement)
    # ⚠️ 不调用 session.commit()，由调用方在事务中统一管理
    
    return result.rowcount > 0


def mark_batch_as_resolved_by_file_paths(
    session: Session,
    file_paths: List[str]
) -> int:
    """
    批量标记为已解决（根据文件路径列表）
    
    Args:
        session: 数据库会话
        file_paths: 文件路径列表
        
    Returns:
        更新的记录数量
    """
    if not file_paths:
        return 0
    
    statement = (
        update(SqlParseFailureLog)
        .where(SqlParseFailureLog.file_path.in_(file_paths))
        .where(SqlParseFailureLog.is_resolved == False)  # 只更新未解决的
        .values(
            is_resolved=True,
            resolve_time=datetime.utcnow(),
            modify_time=datetime.utcnow()
        )
    )
    
    result: CursorResult = session.execute(statement)
    # ⚠️ 不调用 session.commit()，由调用方在事务中统一管理
    
    updated_count = result.rowcount
    return updated_count


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
    
    result: CursorResult = session.execute(statement)
    # ⚠️ 事务提交/回滚由调用方统一管理
    
    return result.rowcount > 0


def get_failure_statistics(session: Session) -> dict:
    """
    获取失败统计信息
    
    Args:
        session: 数据库会话
        
    Returns:
        统计信息字典
    """
    from sqlalchemy import func
    
    # 总失败数
    total_count = session.execute(
        select(func.count()).select_from(SqlParseFailureLog)
    ).scalar()
    
    # 未解决数
    unresolved_count = session.execute(
        select(func.count()).select_from(SqlParseFailureLog).where(
            SqlParseFailureLog.is_resolved == False
        )
    ).scalar()
    
    # 已解决数
    resolved_count = total_count - unresolved_count
    
    # 按错误类型统计
    error_type_stats = {}
    all_logs = session.execute(select(SqlParseFailureLog)).scalars().all()
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
