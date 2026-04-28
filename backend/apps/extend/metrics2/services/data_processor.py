"""
数据处理器 - 负责 SQL 文件的读取、解析和预处理
"""

import logging
from typing import Dict, Any, Union
from pathlib import Path
import json

from apps.extend.metrics2.models.sql_parse_failure_log_model import SqlParseFailureLog
from apps.extend.metrics2.models.sql_parse_success_log_model import SqlParseSuccessLog
from apps.extend.utils.utils import ModelClient
from apps.extend.metrics2.services.exception_service import ExceptionService

logger = logging.getLogger("DataProcessor")


class DataProcessor:
    """
    数据处理器
    
    职责：
    1. 读取 SQL 文件
    2. 调用 AI 解析 SQL
    3. 修正 warehouse_layer 字段
    4. 自动识别层级类型（基于路径）
    5. 检查 GROUP BY
    6. 确定处理流程
    """
    
    def __init__(self, session, exception_service: ExceptionService):
        """
        初始化脚本处理器
        
        Args:
            session: 数据库会话
            exception_service: 异常服务
        """
        self.session = session
        self.model_client = ModelClient()
        self.exception_service = exception_service
        
        logger.info("[DataProcessor] 实例已创建")
    
    def read_sql_file(self, file_path: Path) -> Dict[str, Any]:
        """
        读取单个 SQL 文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            {
                'file_path': str,
                'sql_content': str,
                'success': bool,
                'message': str
            }
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            return {
                'file_path': str(file_path),
                'sql_content': sql_content,
                'success': True,
                'message': '文件读取成功'
            }
            
        except Exception as e:
            logger.error(f"[脚本处理器] ❌ 读取文件失败: {file_path.name}, 错误: {str(e)}")
            return {
                'file_path': str(file_path),
                'sql_content': '',
                'success': False,
                'message': f'文件读取失败：{str(e)}'
            }
    
    def parse_sql_with_ai(self, sql_content: str, file_path: str, layer_type: str) -> Dict[str, Any]:
        """
        使用大模型解析 SQL 内容（对外服务接口）
        
        Args:
            sql_content: SQL 内容
            file_path: 文件路径
            layer_type: 层级类型

        Returns:
            {
                'file_path': str,
                'success': bool,
                'message': str,
                'parsed_data': dict or None
            }
        """
        try:
            logger.info(f"[AI解析] 调用提示词模板: sql_analysis, layer_type={layer_type}")
            
            # 调用大模型解析 SQL
            result = self.model_client.call_ai(
                template_name="sql_analysis",
                sql_content=sql_content,
                layer_type=layer_type
            )
            
            # 检查返回内容
            if not result or (isinstance(result, str) and not result.strip()):
                logger.error(f"[AI解析] ❌ AI 返回空内容, 文件: {file_path}")
                error_msg = "大模型返回空内容"

                # 记录失败日志
                failure_log = {
                    'file_path': file_path,
                    'file_name': Path(file_path).name,
                    'failure_reason': error_msg,
                    'layer_type': layer_type,
                    'error_type': 'AI_EMPTY_RESPONSE'
                }

                return {
                    'success': False,
                    'file_path': file_path,
                    'message': error_msg,
                    'failure_log': failure_log
                }
            
            logger.debug(f"[AI解析] ✅ AI 返回内容长度: {len(result)} 字符")
            
            # 解析 JSON
            try:
                parsed_json = json.loads(result)
                logger.debug(f"[AI解析] ✅ JSON 解析成功")
            except json.JSONDecodeError as e:
                error_msg = f"JSON解析失败：{str(e)}"
                logger.error(f"[AI解析] ❌ {error_msg}, 文件: {file_path}")

                # 记录失败日志
                failure_log = {
                    'file_path': file_path,
                    'file_name': Path(file_path).name,
                    'failure_reason': error_msg,
                    'layer_type': layer_type,
                    'error_type': 'JSON_PARSE_ERROR'
                }

                return {
                    'success': False,
                    'file_path': file_path,
                    'message': error_msg,
                    'failure_log': failure_log
                }
            
            # 强制覆盖 basic_info.file_name 为实际文件路径
            if 'basic_info' not in parsed_json:
                parsed_json['basic_info'] = {}
            parsed_json['basic_info']['file_name'] = file_path
            
            # 强制修正 warehouse_layer
            self._fix_warehouse_layer(parsed_json, layer_type, file_path)
            
            # 保存原始 SQL 内容
            if 'basic_info' in parsed_json:
                parsed_json['basic_info']['sql_content'] = sql_content
            
            return {
                'file_path': file_path,
                'success': True,
                'message': 'SQL解析成功',
                'parsed_data': parsed_json
            }
            
        except json.JSONDecodeError as e:
            # 记录失败日志
            failure_log = {
                'file_path': file_path,
                'file_name': Path(file_path).name,
                'failure_reason': f"JSON解析失败：{str(e)}",
                'layer_type': layer_type,
                'error_type': 'JSON_DECODE_EXCEPTION'
            }

            return {
                'success': False,
                'file_path': file_path,
                'message': error_msg,
                'failure_log': failure_log
            }
        except Exception as e:
            logger.error(f"[脚本处理器] ❌ 解析文件失败: {file_path}, 错误: {str(e)}", exc_info=True)
            # 记录失败日志
            failure_log = {
                'file_path': file_path,
                'file_name': Path(file_path).name,
                'failure_reason': f"大模型解析失败：{str(e)}",
                'layer_type': layer_type,
                'error_type': 'JSON_DECODE_EXCEPTION'
            }

            return {
                'success': False,
                'file_path': file_path,
                'message': error_msg,
                'failure_log': failure_log
            }
    
    def _fix_warehouse_layer(self, parsed_json: Dict, layer_type: str, sql_file: str):
        """
        修正 warehouse_layer 字段
        
        Args:
            parsed_json: 解析后的 JSON
            layer_type: 当前流程类型
            sql_file: SQL 文件路径
        """
        ai_layer = parsed_json.get('basic_info', {}).get('warehouse_layer', '').lower()
        
        if layer_type == "WIDE":
            # WIDE 流程，保持原始数仓层级（dwd/dws/ads）
            if ai_layer not in ['dwd', 'dws', 'ads']:
                # 从文件路径推断
                file_path_lower = sql_file.lower()
                if '/dwd/' in file_path_lower or '\\dwd\\' in file_path_lower:
                    parsed_json['basic_info']['warehouse_layer'] = 'dwd'
                elif '/dws/' in file_path_lower or '\\dws\\' in file_path_lower:
                    parsed_json['basic_info']['warehouse_layer'] = 'dws'
                elif '/ads/' in file_path_lower or '\\ads\\' in file_path_lower:
                    parsed_json['basic_info']['warehouse_layer'] = 'ads'
                else:
                    parsed_json['basic_info']['warehouse_layer'] = 'dwd'
        elif layer_type == "METRIC":
            # METRIC 流程，保持 AI 的判断（dws 或 ads）
            if ai_layer not in ['dws', 'ads']:
                parsed_json['basic_info']['warehouse_layer'] = 'dws'
        elif layer_type == "DIM":
            # DIM 流程，设置为 dim
            parsed_json['basic_info']['warehouse_layer'] = 'dim'
    
    def auto_detect_layer_type(self, input_path: Union[str, Path]) -> str:
        """
        根据文件路径自动识别数仓层级类型
        
        Args:
            input_path: 文件路径或目录路径
            
        Returns:
            层级类型："DIM" / "DWD" / "DWS" / "ADS"
        """
        path_str = str(input_path).lower()
        
        # 匹配路径中的关键词（支持中间和末尾）
        if '/dim/' in path_str or '\\dim\\' in path_str or path_str.endswith('/dim') or path_str.endswith('\\dim'):
            return "DIM"
        elif '/dwd/' in path_str or '\\dwd\\' in path_str or path_str.endswith('/dwd') or path_str.endswith('\\dwd'):
            return "DWD"
        elif '/dws/' in path_str or '\\dws\\' in path_str or path_str.endswith('/dws') or path_str.endswith('\\dws'):
            return "DWS"
        elif '/ads/' in path_str or '\\ads\\' in path_str or path_str.endswith('/ads') or path_str.endswith('\\ads'):
            return "ADS"
        
        # 其他情况默认为 DWD 层
        logger.warning(f"[自动识别] 无法从路径识别层级: {input_path}，默认为 DWD")
        return "DWD"
    
    def check_has_group_by(self, sql_content: str) -> bool:
        """
        检查 SQL 是否包含 GROUP BY 子句
        
        Args:
            sql_content: SQL 内容
            
        Returns:
            是否包含 GROUP BY
        """
        import re
        # 移除注释
        sql_clean = re.sub(r'--.*?$', '', sql_content, flags=re.MULTILINE)
        sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)
        
        # 检查是否包含 GROUP BY（不区分大小写）
        return bool(re.search(r'\bGROUP\s+BY\b', sql_clean, re.IGNORECASE))
    
    def determine_processing_flow(self, input_path: Union[str, Path], is_directory: bool) -> str:
        """
        根据 SQL 内容确定处理流程（仅用于 DWS/ADS 层）
        
        规则：
        - 有 GROUP BY → METRIC 流程（指标层，走 MetricsService）
        - 无 GROUP BY → WIDE 流程（宽表/明细层血缘提取，走 LineageService）
        
        ⚠️ 注意：此方法只被 DWS/ADS 层调用，DWD 层直接走 LineageService
        
        Args:
            input_path: 输入路径（文件路径或目录路径）
            is_directory: 是否为目录模式
            
        Returns:
            处理流程类型："METRIC" 或 "WIDE"
        """
        try:
            # 获取SQL文件列表
            path = Path(input_path)
            sql_files = []
            
            if is_directory:
                sql_files = list(path.glob("*.sql"))
            else:
                if path.suffix.lower() == '.sql':
                    sql_files = [path]
            
            if not sql_files:
                logger.warning(f"[流程判断] 未找到 SQL 文件，默认使用 METRIC 流程")
                return "METRIC"
            
            # 检查每个SQL文件是否有 GROUP BY
            has_group_by = False
            for sql_file in sql_files:
                try:
                    with open(sql_file, 'r', encoding='utf-8') as f:
                        sql_content = f.read()
                    
                    if self.check_has_group_by(sql_content):
                        has_group_by = True
                        logger.info(f"[流程判断] 文件 {sql_file.name} 包含 GROUP BY，使用 METRIC 流程")
                        break
                except Exception as e:
                    logger.warning(f"[流程判断] 读取文件 {sql_file} 失败: {e}")
                    continue
            
            # 根据是否有 GROUP BY 决定流程
            if has_group_by:
                return "METRIC"
            else:
                logger.info(f"[流程判断] 所有文件均无 GROUP BY，使用 WIDE 流程（宽表/明细层）")
                return "WIDE"
            
        except Exception as e:
            logger.warning(f"[流程判断] 检查失败: {e}，默认使用 METRIC 流程")
            return "METRIC"

    def get_processed_file_paths(self, session, file_paths: list[str], layer_type: str) -> set:
        """
        ⚠️ 断点续传：获取所有已处理的文件路径（成功 + 失败）
        
        合并 sql_parse_success_log 和 sql_parse_failure_log 两个表的 file_path，并去重
        根据 layer_type 过滤已处理文件
        
        Args:
            session: 数据库会话
            file_paths: 当前待处理的文件路径列表
            layer_type: 层级类型（DIM/DWD/DWS/ADS）
        
        Returns:
            已处理文件路径集合（仅包含当前 layer_type 的文件）
        """
        try:
            from sqlmodel import select
            
            # 查询成功记录的文件路径（按 layer_type 过滤）
            success_statement = select(SqlParseSuccessLog.file_path).where(
                SqlParseSuccessLog.layer_type == layer_type
            )
            success_result = self.session.execute(success_statement)
            success_paths = {row[0] for row in success_result.fetchall()}
            
            # 查询失败记录的文件路径（按 layer_type 过滤）
            failure_statement = select(SqlParseFailureLog.file_path).where(
                SqlParseFailureLog.layer_type == layer_type
            )
            failure_result = self.session.execute(failure_statement)
            failure_paths = {row[0] for row in failure_result.fetchall()}
            
            # 合并并去重
            all_processed_paths = success_paths | failure_paths
            
            logger.debug(f"[断点续传] 查询到 {len(all_processed_paths)} 个已处理文件（{layer_type} 层，成功: {len(success_paths)}, 失败: {len(failure_paths)}）")
            
            return all_processed_paths
        
        except Exception as e:
            logger.warning(f"[断点续传] 查询已处理文件失败: {str(e)}，返回空集合")
            return set()
