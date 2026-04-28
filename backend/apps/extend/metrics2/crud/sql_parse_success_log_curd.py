"""
SQL 解析成功记录 CURD 操作
"""
from datetime import datetime
from typing import List, Optional, Set, Dict, Any
import json
import logging

from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, select

from apps.extend.metrics2.models.sql_parse_success_log_model import SqlParseSuccessLog

logger = logging.getLogger(__name__)


def create_or_update_success_log(
    session: Session,
    file_path: str,
    file_name: str,
    layer_type: str,
    target_table: Optional[str] = None,
    table_stats: Optional[dict] = None,
    processing_duration: Optional[float] = None
) -> SqlParseSuccessLog:
    """
    创建或更新成功记录（UPSERT）- 单条记录
    
    Args:
        session: 数据库会话
        file_path: SQL 文件路径
        file_name: 文件名
        layer_type: 层级类型
        target_table: 目标表名
        table_stats: 表统计信息
        processing_duration: 处理耗时（秒）
        
    Returns:
        创建或更新的记录
    """
    # 准备数据
    now = datetime.utcnow()
    data = {
        'file_path': str(file_path),  # ⚠️ 确保转换为字符串
        'file_name': file_name,
        'layer_type': layer_type,
        'target_table': target_table,
        'table_stats': json.dumps(table_stats) if table_stats else None,
        'parse_time': now,
        'processing_duration': processing_duration,
        'create_time': now,
        'modify_time': now
    }
    
    # 使用 PostgreSQL 的 UPSERT 语法
    stmt = insert(SqlParseSuccessLog).values(**data)
    
    # 如果冲突（file_path 已存在），则更新相关字段
    update_dict = {
        'layer_type': stmt.excluded.layer_type,
        'target_table': stmt.excluded.target_table,
        'table_stats': stmt.excluded.table_stats,
        'parse_time': stmt.excluded.parse_time,
        'processing_duration': stmt.excluded.processing_duration,
        'modify_time': now
    }
    
    stmt = stmt.on_conflict_do_update(
        index_elements=['file_path'],
        set_=update_dict
    )
    
    session.execute(stmt)
    # ⚠️ 不调用 session.commit()，由调用方在事务中统一管理
    
    # 查询并返回最新记录
    statement = select(SqlParseSuccessLog).where(
        SqlParseSuccessLog.file_path == file_path
    )
    result = session.execute(statement)
    log = result.scalars().one()
    
    return log


def batch_create_or_update_success_logs(
    session: Session,
    success_logs: List[Dict[str, Any]]
) -> int:
    """
    批量创建或更新成功记录（UPSERT）
    
    Args:
        session: 数据库会话
        success_logs: 成功日志列表，每个元素包含：
            - file_path: 文件路径
            - file_name: 文件名
            - layer_type: 层级类型
            - target_table: 目标表名（可选）
            - table_stats: 表统计信息（可选）
            - processing_duration: 处理耗时（可选）
    
    Returns:
        成功插入/更新的记录数量
    """
    if not success_logs:
        return 0
    
    try:
        now = datetime.utcnow()
        
        # 构建批量数据
        values_list = []
        for log_data in success_logs:
            file_path = log_data.get('file_path', '')
            if not file_path:
                continue
            
            values_list.append({
                'file_path': str(file_path),
                'file_name': log_data.get('file_name', ''),
                'layer_type': log_data.get('layer_type', 'UNKNOWN'),
                'target_table': log_data.get('target_table'),
                'table_stats': json.dumps(log_data.get('table_stats')) if log_data.get('table_stats') else None,
                'parse_time': now,
                'processing_duration': log_data.get('processing_duration'),
                'create_time': now,
                'modify_time': now
            })
        
        if not values_list:
            return 0
        
        # 使用 PostgreSQL 的批量 UPSERT 语法
        stmt = insert(SqlParseSuccessLog).values(values_list)
        
        # 如果冲突（file_path 已存在），则更新相关字段
        update_dict = {
            'layer_type': stmt.excluded.layer_type,
            'target_table': stmt.excluded.target_table,
            'table_stats': stmt.excluded.table_stats,
            'parse_time': stmt.excluded.parse_time,
            'processing_duration': stmt.excluded.processing_duration,
            'modify_time': now
        }
        
        stmt = stmt.on_conflict_do_update(
            index_elements=['file_path'],
            set_=update_dict
        )
        
        result = session.execute(stmt)
        # ⚠️ 不调用 session.commit()，由调用方在事务中统一管理
        
        inserted_count = len(values_list)
        logger.info(f"[成功日志] 批量插入 {inserted_count} 条成功记录")
        
        return inserted_count
    
    except Exception as e:
        logger.error(f"[成功日志] 批量插入失败: {str(e)}")
        raise


def get_success_log(session: Session, file_path: str) -> Optional[SqlParseSuccessLog]:
    """
    查询单个文件的成功记录
    
    Args:
        session: 数据库会话
        file_path: 文件路径
        
    Returns:
        成功记录，不存在则返回 None
    """
    statement = select(SqlParseSuccessLog).where(
        SqlParseSuccessLog.file_path == file_path
    )
    result = session.execute(statement)
    return result.scalars().first()


def get_success_file_paths(session: Session) -> Set[str]:
    """
    获取所有已成功处理的文件路径集合
    
    Args:
        session: 数据库会话
        
    Returns:
        文件路径集合
    """
    statement = select(SqlParseSuccessLog.file_path)
    result = session.execute(statement)
    return {row[0] for row in result.fetchall()}


def get_success_logs(
    session: Session,
    layer_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[SqlParseSuccessLog]:
    """
    查询成功记录列表
    
    Args:
        session: 数据库会话
        layer_type: 层级类型
        limit: 返回数量限制
        offset: 偏移量
        
    Returns:
        记录列表
    """
    statement = select(SqlParseSuccessLog)
    
    if layer_type:
        statement = statement.where(SqlParseSuccessLog.layer_type == layer_type)
    
    statement = statement.order_by(SqlParseSuccessLog.parse_time.desc())
    statement = statement.offset(offset).limit(limit)
    
    return session.execute(statement).scalars().all()


def delete_success_log(session: Session, file_path: str) -> bool:
    """
    删除成功记录（用于重新处理）
    
    Args:
        session: 数据库会话
        file_path: 文件路径
        
    Returns:
        是否成功
    """
    statement = select(SqlParseSuccessLog).where(
        SqlParseSuccessLog.file_path == file_path
    )
    result = session.execute(statement)
    log = result.scalars().first()
    
    if log:
        session.delete(log)
    # ⚠️ 事务提交/回滚由调用方统一管理
        return True
    
    return False


def get_success_statistics(session: Session) -> dict:
    """
    获取成功统计信息
    
    Args:
        session: 数据库会话
        
    Returns:
        统计信息字典
    """
    from sqlalchemy import func
    
    # 总成功数
    total_count = session.execute(
        select(func.count()).select_from(SqlParseSuccessLog)
    ).scalar()
    
    # 按层级类型统计
    layer_type_stats = {}
    all_logs = session.execute(select(SqlParseSuccessLog)).scalars().all()
    for log in all_logs:
        layer_type = log.layer_type or "UNKNOWN"
        if layer_type not in layer_type_stats:
            layer_type_stats[layer_type] = 0
        layer_type_stats[layer_type] += 1
    
    # 平均处理时长
    durations = [log.processing_duration for log in all_logs if log.processing_duration is not None]
    avg_duration = sum(durations) / len(durations) if durations else 0
    
    return {
        "total_success": total_count,
        "layer_type_distribution": layer_type_stats,
        "avg_processing_duration": f"{avg_duration:.2f}s"
    }
