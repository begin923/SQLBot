"""
校验服务 - 负责所有数据校验和质量检查
"""

import logging
from typing import List, Dict, Any, Union
from pathlib import Path
import re

from apps.extend.metrics2.services.exception_service import ExceptionService

logger = logging.getLogger("CheckService")


class CheckService:
    """
    校验服务
    
    职责：
    1. SQL 质量检查（SELECT * 检测）
    2. 批次数据完整性校验
    3. 关键表数据校验
    """
    
    def __init__(self, session, exception_service: ExceptionService, data_processor=None):
        """
        初始化校验服务
        
        Args:
            session: 数据库会话
            exception_service: 异常服务
            data_processor: 数据处理器实例（用于层级识别和流程判断）
        """
        self.session = session
        self.exception_service = exception_service
        self.data_processor = data_processor
        
        logger.info("[CheckService] 实例已创建")
    
    # ==================== 输入路径校验 ====================

    def validate_and_detect_mode(self, input_path: Union[str, Path]) -> Dict[str, Any]:
        """
        验证输入路径并检测模式（文件/目录）、识别层级类型和分流决策

        Args:
            input_path: 输入路径

        Returns:
            {
                'success': bool,
                'is_directory': bool,
                'layer_type': str,
                'processing_flow': Optional[str]
            }
        """
        if not self.data_processor:
            raise RuntimeError("CheckService 需要 data_processor 实例才能执行路径校验")

        path = Path(input_path)
        if not path.exists():
            return {
                'success': False,
                'message': f'路径不存在：{input_path}',
                'file_result': {'total_files': 0, 'processed_files': 0, 'failed_files': 0, 'results': []}
            }

        is_directory = path.is_dir()
        mode_str = "目录" if is_directory else "文件"
        logger.info(f"[流程开始] 检测到输入为{mode_str}模式: {input_path}")

        # 自动识别层级类型
        layer_type = self.data_processor.auto_detect_layer_type(input_path)
        logger.info(f"[流程开始] 自动识别层级类型: {layer_type}")

        # DWS/ADS 层自动分流逻辑
        processing_flow = None
        if layer_type in ["DWS", "ADS"]:
            processing_flow = self.data_processor.determine_processing_flow(input_path, is_directory)
            logger.info(f"[{layer_type} 分流] 检测到 {'有' if processing_flow == 'METRIC' else '无'} GROUP BY，使用 {processing_flow} 流程")

        return {
            'success': True,
            'is_directory': is_directory,
            'layer_type': layer_type,
            'processing_flow': processing_flow
        }

    # ==================== SQL 质量检查 ====================
    
    def pre_check_sql_quality(self, file_data: Dict[str, Any], layer_type: str) -> Dict[str, Any]:
        """
        预检查 SQL 质量（SELECT * 检查）
        
        Args:
            file_data: 文件数据（包含 sql_content）
            layer_type: 层级类型
            
        Returns:
            {'success': bool, 'message': str}
        """
        # 只有 DIM 和 DWD 层需要检查 SELECT *
        if layer_type not in ["DIM", "DWD"]:
            return {'success': True}
        
        sql_content = file_data.get('sql_content', '')
        file_path = file_data.get('file_path', '')
        
        if not sql_content:
            return {'success': True}
        
        select_star_check = self._check_select_star(sql_content)
        
        if select_star_check['has_select_star']:
            error_msg = (
                f"❌ SQL 中存在 SELECT * 或通配符（{select_star_check['matched_pattern']}），无法准确解析字段。\n"
                f"   文件：{file_path}\n"
                f"   原因：{'DIM' if layer_type == 'DIM' else 'DWD'} 层需要精确的字段信息，必须将所有通配符展开为明确的字段列表\n"
                f"   建议：修改 SQL，将 {select_star_check['matched_pattern']} 替换为具体的字段名"
            )
            logger.error(error_msg)
            
            # 记录失败日志
            failure_log = {
                'file_path': file_path,
                'file_name': Path(file_path).name,
                'failure_reason': error_msg,
                'layer_type': layer_type,
                'error_type': 'SELECT_STAR'
            }
            return {
                "success": False,
                'failure_logs': failure_log,
                'error_msg':error_msg,
                'sql_content': sql_content,
                'matched_pattern': select_star_check['matched_pattern']
            }
        
        check_target = "DIM 层" if layer_type == "DIM" else "DWD 层"
        logger.info(f"[预检查] ✅ {check_target} SELECT * 校验通过")
        return {'success': True}
    
    def _check_select_star(self, sql_content: str) -> Dict[str, Any]:
        """
        【极简版】只拦截最极端的 SELECT * 情况
        
        Args:
            sql_content: SQL 内容
            
        Returns:
            {'has_select_star': bool, 'matched_pattern': str}
        """
        # 移除注释
        sql_cleaned = re.sub(r'--.*?$', '', sql_content, flags=re.MULTILINE)
        sql_cleaned = re.sub(r'/\*.*?\*/', '', sql_cleaned, flags=re.DOTALL)
        
        # 只拦截最明显的危险模式：INSERT INTO table SELECT * FROM
        dangerous_patterns = [
            r'INSERT\s+INTO\s+\w+\s+SELECT\s+\*\s+FROM',
            r'INSERT\s+INTO\s+\w+\s+SELECT\s+\w+\.\*\s+FROM',
        ]
        
        for pattern in dangerous_patterns:
            match = re.search(pattern, sql_cleaned, re.IGNORECASE)
            if match:
                matched_text = match.group(0)
                if '.*' in matched_text:
                    alias_match = re.search(r'(\w+)\.\*', matched_text)
                    if alias_match:
                        return {'has_select_star': True, 'matched_pattern': f'{alias_match.group(1)}.*'}
                return {'has_select_star': True, 'matched_pattern': 'SELECT *'}
        
        return {'has_select_star': False, 'matched_pattern': ''}
    
    # ==================== 单个文件数据校验 ====================
    
    def check_data_integrity(
        self, 
        table_data: Dict[str, List], 
        critical_tables: List[str]
    ) -> Dict[str, Any]:
        """
        校验单个文件的表数据完整性（Service 层调用）
        
        校验规则：
        1. table_data 不能为空
        2. 关键表必须有数据（由调用方指定）
        
        Args:
            table_data: Service 返回的表数据
            critical_tables: 需要校验的关键表列表（由调用方决定）
            
        Returns:
            {'success': bool, 'message': str}
        """
        # 1. 检查是否有 table_data
        if not table_data:
            return {
                'success': False,
                'message': '❌ 未生成任何表数据'
            }
        
        # 2. 检查关键表是否都有数据
        missing_critical_tables = []
        for table_name in critical_tables:
            if table_name not in table_data or not table_data[table_name]:
                missing_critical_tables.append(table_name)
        
        if missing_critical_tables:
            return {
                'success': False,
                'message': f"❌ 数据校验失败，缺少关键表数据: {', '.join(missing_critical_tables)}"
            }
        
        return {'success': True, 'message': '数据校验通过'}
    
    def _check_has_group_by(self, sql_content: str) -> bool:
        """
        检查 SQL 是否包含 GROUP BY 子句
        
        Args:
            sql_content: SQL 内容
            
        Returns:
            是否包含 GROUP BY
        """
        # 移除注释
        sql_clean = re.sub(r'--.*?$', '', sql_content, flags=re.MULTILINE)
        sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)
        
        # 检查是否包含 GROUP BY（不区分大小写）
        return bool(re.search(r'\bGROUP\s+BY\b', sql_clean, re.IGNORECASE))
