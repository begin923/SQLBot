"""
异常服务 - 统一管理 SQL 解析和处理过程中的异常

职责：
1. 记录 SQL 解析失败记录
2. 记录 Service 层处理失败记录
3. 标记失败记录为已解决
4. 异常分类和格式化
5. 统一错误处理
"""

import logging
import re
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from apps.extend.metrics2.crud.sql_parse_failure_log_curd import mark_batch_as_resolved_by_file_paths, \
    create_failure_log

logger = logging.getLogger("ExceptionService")


class ExceptionService:
    """异常服务 - 统一管理异常的记录、分类和处理"""
    
    def __init__(self, session: Session):
        """
        初始化异常服务
        
        Args:
            session: 数据库会话
        """
        self.session = session
    
    def save_failure_log(
        self,
        failed_files: List[Dict[str, Any]]
    ) -> int:
        """
        批量记录失败记录（核心方法）
        
        职责：
        1. 遍历失败文件列表
        2. 为每个文件插入一条失败记录记录
        3. 返回成功插入的日志数量
        
        数据格式要求（统一格式）：
        [
            {
                'file_path': '文件路径',           # 必填
                'failure_reason': '失败原因',       # 必填
                'layer_type': '层级类型',           # 可选
                'error_type': '错误类型',           # 可选
                'sql_content': 'SQL内容',           # 可选
                'matched_pattern': '匹配模式'       # 可选
            }
        ]
        
        Args:
            failed_files: 失败文件列表（统一格式）
            
        Returns:
            成功插入的日志数量
        """
        try:
            from apps.extend.metrics2.crud.sql_parse_failure_log_curd import batch_create_failure_logs
            
            if not failed_files:
                logger.debug("[失败记录] 没有需要记录的失败文件")
                return 0
            
            # ⚠️ 使用批量方法
            recorded_count = batch_create_failure_logs(self.session, failed_files)
            
            if recorded_count > 0:
                logger.info(f"[失败记录] 共记录 {recorded_count} 条")
            
            return recorded_count
        
        except Exception as e:
            logger.error(f"[失败记录] 批量记录失败记录时出错: {str(e)}")
            return 0
    
    def mark_failures_as_resolved(self, file_results: List[Dict[str, Any]]) -> int:
        """
        标记处理成功的文件的失败记录为已解决
        
        Args:
            file_results: 文件处理结果列表（只包含成功的文件）
            
        Returns:
            标记为已解决的日志数量
        """
        try:
            # 提取文件路径列表
            file_paths = []
            for file_result in file_results:
                if file_result.get('success', False):
                    file_path = file_result.get('file_path', '')
                    if file_path:
                        file_paths.append(str(file_path))
            
            if not file_paths:
                logger.debug("[失败记录] 没有需要标记的文件")
                return 0
            
            # 批量更新
            resolved_count = mark_batch_as_resolved_by_file_paths(self.session, file_paths)
            
            if resolved_count > 0:
                logger.info(f"[失败记录] 共标记 {resolved_count} 条记录为已解决")
            
            return resolved_count
        
        except Exception as e:
            # 标记失败记录不应影响主流程
            logger.error(f"[失败记录] 标记已解决时出错: {str(e)}")
            return 0

    # ==================== 私有辅助方法 ====================
    
    def _format_exception_details(self, exception: Exception, base_message: str) -> str:
        """
        格式化异常详细信息
        
        Args:
            exception: 异常对象
            base_message: 基础错误消息
            
        Returns:
            格式化后的错误消息
        """
        exc_type = type(exception).__name__
        exc_msg = str(exception)
        
        detailed_error = base_message
        
        # 提取关键错误信息（去除完整 SQL）
        if 'StringDataRightTruncation' in exc_msg:
            # 字符串截断错误
            detailed_error += f"\n异常类型: {exc_type}"
            detailed_error += f"\n原因: 字段值超过数据库定义长度"
            # 尝试提取表名和字段信息
            if 'INSERT INTO' in exc_msg:
                table_match = re.search(r'INSERT INTO (\w+)', exc_msg)
                if table_match:
                    detailed_error += f"\n异常表: {table_match.group(1)}"
        elif 'CardinalityViolation' in exc_msg:
            # ON CONFLICT 冲突
            detailed_error += f"\n异常类型: {exc_type}"
            detailed_error += f"\n原因: 批量插入数据中存在重复的冲突键值"
        else:
            # 其他异常，只显示前200字符
            detailed_error += f"\n异常类型: {exc_type}"
            detailed_error += f"\n异常详情: {exc_msg[:200]}..."
        
        return detailed_error
    
    def _classify_error(self, exception: Optional[Exception], error_message: str) -> str:
        """
        分类错误类型
        
        Args:
            exception: 异常对象
            error_message: 错误消息
            
        Returns:
            错误类型字符串
        """
        if exception:
            exc_type = type(exception).__name__
            exc_msg = str(exception)
            
            # 1. 数据库唯一约束冲突
            if 'UniqueViolation' in exc_msg or 'CardinalityViolation' in exc_msg:
                return "DB_UNIQUE_VIOLATION"
            # 2. 数据库字段长度超限
            elif 'StringDataRightTruncation' in exc_msg:
                return "DB_FIELD_LENGTH_EXCEEDED"
            # 3. SQLAlchemy 参数绑定错误
            elif 'StatementError' in exc_msg or 'InvalidRequestError' in exc_msg:
                return "DB_SQL_PARAM_ERROR"
            # 4. 事务状态异常
            elif 'PendingRollback' in exc_msg:
                return "DB_TRANSACTION_ERROR"
            # 5. 其他数据库异常
            elif 'psycopg2' in exc_msg or 'sqlalchemy' in exc_msg.lower():
                return "DB_ERROR"
            # 6. 其他异常
            else:
                return f"SERVICE_{exc_type.upper()}"
        else:
            # 没有异常对象，根据错误消息判断
            if '未生成任何 dim_definition' in error_message:
                return "DIM_AI_PARSE_FAILED"
            elif '未生成任何 table_lineage' in error_message:
                return "LINEAGE_TABLE_NOT_FOUND"
            elif '未生成任何 field_lineage' in error_message:
                return "LINEAGE_FIELD_NOT_FOUND"
            elif 'metric_definition' in error_message and '未解析到' in error_message:
                return "METRIC_DEFINITION_NOT_FOUND"
            elif 'metric_source_mapping' in error_message and '未解析到' in error_message:
                return "METRIC_SOURCE_MAPPING_NOT_FOUND"
            elif '数据完整性校验失败' in error_message:
                return "DATA_VALIDATION_FAILED"
            elif '批次回滚' in error_message or 'BATCH_ROLLBACK' in error_message:
                return "BATCH_ROLLBACK"  # ⚠️ 新增：批次回滚导致的失败
            else:
                return "SERVICE_ERROR"
    
    def save_success_logs(
        self,
        file_results: List[Dict[str, Any]]
    ) -> int:
        """
        批量记录成功日志到 sql_parse_success_log 表
        Args:
            file_results: 成功文件列表（统一格式）
            
        Returns:
            成功插入的日志数量
        """
        try:
            from apps.extend.metrics2.crud.sql_parse_success_log_curd import batch_create_or_update_success_logs
            
            if not file_results:
                logger.debug("[成功日志] 没有需要记录的成功文件")
                return 0
            
            # ⚠️ 使用批量方法
            success_count = batch_create_or_update_success_logs(self.session, file_results)
            
            if success_count > 0:
                logger.info(f"[成功记录] 共插入 {success_count} 条")
            
            return success_count
        
        except Exception as e:
            logger.error(f"[成功日志] 记录成功日志时出错: {str(e)}")
            return 0
