"""
异常服务 - 统一管理 SQL 解析和处理过程中的异常

职责：
1. 记录 SQL 解析失败日志
2. 记录 Service 层处理失败日志
3. 标记失败日志为已解决
4. 异常分类和格式化
5. 统一错误处理
"""

import logging
import os
import re
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

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
    
    def log_parse_failure(
        self,
        file_path: str,
        failure_reason: str,
        layer_type: Optional[str] = None,
        error_type: Optional[str] = None,
        sql_content: Optional[str] = None,
        matched_pattern: Optional[str] = None
    ) -> bool:
        """
        记录 SQL 解析失败日志
        
        Args:
            file_path: SQL 文件路径
            failure_reason: 失败原因
            layer_type: 层级类型
            error_type: 错误类型
            sql_content: SQL 内容
            matched_pattern: 匹配到的模式
            
        Returns:
            是否记录成功
        """
        try:
            from apps.extend.metrics2.curd.sql_parse_failure_log_curd import create_failure_log
            
            file_name = os.path.basename(file_path) if file_path else "unknown.sql"
            
            # 截断 SQL 内容（避免过大）
            if sql_content and len(sql_content) > 5000:
                sql_content = sql_content[:5000] + "... [truncated]"
            
            create_failure_log(
                session=self.session,
                file_path=file_path,
                file_name=file_name,
                failure_reason=failure_reason,
                layer_type=layer_type,
                error_type=error_type,
                sql_content=sql_content,
                matched_pattern=matched_pattern
            )
            
            logger.info(f"[失败日志] 已记录解析失败: {file_name} - {error_type}")
            return True
            
        except Exception as e:
            # 日志记录失败不应影响主流程
            logger.error(f"[失败日志] 记录解析失败日志时出错: {str(e)}")
            return False
    
    def log_service_failure(
        self,
        parsed_results: List[Dict[str, Any]],
        service_name: str,
        error_message: str,
        layer_type: str,
        exception: Optional[Exception] = None
    ) -> int:
        """
        记录 Service 层处理失败日志
        
        Args:
            parsed_results: 解析结果列表
            service_name: Service 名称
            error_message: 错误消息
            layer_type: 层级类型
            exception: 异常对象（可选）
            
        Returns:
            记录的日志数量
        """
        try:
            from apps.extend.metrics2.curd.sql_parse_failure_log_curd import create_failure_log
            
            recorded_count = 0
            
            # 从 parsed_results 中提取文件信息
            for result in parsed_results:
                if not result.get('success', False):
                    continue
                
                file_path = result.get('file_path', '')
                file_name = os.path.basename(file_path) if file_path else "unknown.sql"
                
                # ⚠️ 不获取 SQL 内容，避免占用资源
                sql_content = None
                
                # ⚠️ 构建简洁的错误信息
                detailed_error = f"[{service_name}] {error_message}"
                
                # 如果是数据库异常，提取关键信息
                if exception:
                    detailed_error = self._format_exception_details(exception, detailed_error)
                
                # ⚠️ 确定错误类型（细化分类）
                error_type = self._classify_error(exception, error_message)
                
                create_failure_log(
                    session=self.session,
                    file_path=file_path,
                    file_name=file_name,
                    failure_reason=detailed_error,
                    layer_type=layer_type,
                    error_type=error_type,
                    sql_content=sql_content  # ⚠️ 不存储 SQL 内容
                )
                
                recorded_count += 1
                logger.info(f"[失败日志] 已记录 Service 失败: {file_name} - {error_type}")
            
            return recorded_count
                
        except Exception as e:
            # 日志记录失败不应影响主流程
            logger.error(f"[失败日志] 记录 Service 失败日志时出错: {str(e)}")
            return 0
    
    def mark_failures_as_resolved(self, file_results: List[Dict[str, Any]], batch_size: int = 1000) -> int:
        """
        标记处理成功的文件的失败日志为已解决（支持分批处理）
        
        Args:
            file_results: 文件处理结果列表
            batch_size: 分批处理的大小，默认 1000 个文件
            
        Returns:
            标记为已解决的日志数量
        """
        try:
            from apps.extend.metrics2.curd.sql_parse_failure_log_curd import get_failure_logs, mark_as_resolved
            
            resolved_count = 0
            total_files = len(file_results)
            
            # ⚠️ 如果文件数量超过阈值，采用分批处理策略
            if total_files > batch_size:
                logger.info(f"[失败日志] 检测到大批量文件 ({total_files} 个)，启用分批处理模式 (batch_size={batch_size})")
                return self._mark_failures_batch_mode(file_results, batch_size)
            
            # ⚠️ 小批量场景：一次性查询所有未解决日志，避免 N 次查询
            unresolved_logs = get_failure_logs(
                session=self.session,
                is_resolved=False
            )
            
            # 构建文件名到日志的映射，提高查找效率
            log_map = {}
            for log in unresolved_logs:
                if log.file_name not in log_map:
                    log_map[log.file_name] = []
                log_map[log.file_name].append(log)
            
            for file_result in file_results:
                if not file_result.get('success', False):
                    continue
                
                file_path = file_result.get('file_path', '')
                file_name = os.path.basename(file_path) if file_path else ""
                
                if not file_name or file_name not in log_map:
                    continue
                
                # 标记该文件的所有未解决日志为已解决
                for log in log_map[file_name]:
                    if mark_as_resolved(self.session, log.id):
                        resolved_count += 1
                        logger.debug(f"[失败日志] ✅ 已标记为已解决: {file_name} (ID: {log.id})")
            
            if resolved_count > 0:
                logger.info(f"[失败日志] 共标记 {resolved_count} 条记录为已解决")
            
            return resolved_count
        
        except Exception as e:
            # 标记失败日志不应影响主流程
            logger.error(f"[失败日志] 标记已解决时出错: {str(e)}")
            return 0
    
    def _mark_failures_batch_mode(self, file_results: List[Dict[str, Any]], batch_size: int) -> int:
        """
        分批模式标记失败日志为已解决（适用于大批量文件场景）
        
        Args:
            file_results: 文件处理结果列表
            batch_size: 每批处理的文件数量
            
        Returns:
            标记为已解决的日志数量
        """
        try:
            from apps.extend.metrics2.curd.sql_parse_failure_log_curd import get_failure_logs, mark_as_resolved
            
            total_resolved = 0
            total_batches = (len(file_results) + batch_size - 1) // batch_size
            
            logger.info(f"[失败日志] 开始分批处理: 总计 {len(file_results)} 个文件，分 {total_batches} 批")
            
            # ⚠️ 优化：一次性查询所有未解决日志，避免每批都查询
            all_unresolved_logs = get_failure_logs(
                session=self.session,
                is_resolved=False
            )
            
            # 构建文件名到日志的映射
            log_map = {}
            for log in all_unresolved_logs:
                if log.file_name not in log_map:
                    log_map[log.file_name] = []
                log_map[log.file_name].append(log)
            
            logger.debug(f"[失败日志] 已加载 {len(all_unresolved_logs)} 条未解决日志到内存")
            
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(file_results))
                batch_files = file_results[start_idx:end_idx]
                
                logger.debug(f"[失败日志] 处理第 {batch_idx + 1}/{total_batches} 批 ({len(batch_files)} 个文件)")
                
                # 提取当前批次的成功文件并标记
                batch_resolved = 0
                for file_result in batch_files:
                    if not file_result.get('success', False):
                        continue
                    
                    file_path = file_result.get('file_path', '')
                    file_name = os.path.basename(file_path) if file_path else ""
                    
                    if not file_name or file_name not in log_map:
                        continue
                    
                    # 标记该文件的所有未解决日志为已解决
                    for log in log_map[file_name]:
                        if mark_as_resolved(self.session, log.id):
                            batch_resolved += 1
                            logger.debug(f"[失败日志] ✅ 已标记为已解决: {file_name} (ID: {log.id})")
                
                total_resolved += batch_resolved
                logger.info(f"[失败日志] 第 {batch_idx + 1}/{total_batches} 批完成，标记 {batch_resolved} 条记录")
                
                # ⚠️ 每批处理后短暂休眠，避免数据库压力过大
                if batch_idx < total_batches - 1:  # 最后一批不需要休眠
                    import time
                    time.sleep(0.1)  # 休眠 100ms
            
            logger.info(f"[失败日志] 分批处理完成，共标记 {total_resolved} 条记录为已解决")
            return total_resolved
        
        except Exception as e:
            logger.error(f"[失败日志] 分批标记已解决时出错: {str(e)}")
            return 0
    
    def handle_parse_error(
        self,
        sql_file: str,
        error_msg: str,
        layer_type: str,
        error_type: str,
        sql_content: Optional[str] = None,
        matched_pattern: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        统一处理解析错误（记录日志并返回错误结果）
        
        Args:
            sql_file: SQL 文件路径
            error_msg: 错误消息
            layer_type: 层级类型
            error_type: 错误类型
            sql_content: SQL 内容
            matched_pattern: 匹配到的模式
            
        Returns:
            错误结果字典
        """
        logger.error(f"[AI解析] {error_msg}")
        
        # 记录失败日志
        self.log_parse_failure(
            file_path=sql_file,
            failure_reason=error_msg,
            layer_type=layer_type,
            error_type=error_type,
            sql_content=sql_content,
            matched_pattern=matched_pattern
        )
        
        return {"success": False, "message": error_msg}
    
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
            else:
                return "SERVICE_ERROR"
