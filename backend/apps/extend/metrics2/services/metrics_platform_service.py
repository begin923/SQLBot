from typing import List, Dict, Any, Optional, Union
from pathlib import Path
import logging
import json
from sqlalchemy.orm import Session
from apps.extend.metrics2.services.dim_service import DimService
from apps.extend.metrics2.services.lineage_service import LineageService
from apps.extend.metrics2.services.metrics_service import MetricsService
from apps.extend.utils.utils import ModelClient

# 配置日志
logger = logging.getLogger("MetricsPlatformService")
# ⚠️ 不在这里添加 handler，由调用方（如测试脚本）统一配置日志


class MetricsPlatformService:
    """养殖业务指标平台总服务类 - 负责整个自动化建设链路的编排"""

    def __init__(self, session: Session):
        """
        初始化指标平台服务

        Args:
            session: 数据库会话
        """
        self.session = session
        self.model_client = ModelClient()
        self.dim_service = DimService(session)  # 新增领域服务
        self.lineage_service = LineageService(session)  # 新增领域服务
        self.metrics_service = MetricsService(session)  # 新增领域服务
        

    def process_metrics_from_sql(self, input_path: Union[str, Path], is_directory: bool = False, layer_type: str = "AUTO") -> Dict[str, Any]:
        """
        从SQL文件处理指标（完整流程：读取文件 -> 解析 -> 规则处理 -> 生成SQL -> 写入表）

        Args:
            input_path: 输入路径（文件路径或目录路径）
            is_directory: 是否为目录模式
            layer_type: 数仓层级类型
                - "DIM": 维度定义层（只提取维度，写入 dim_dict）
                - "METRIC": 指标层（dwd/dws/ads，提取指标并引用已有维度）
                - "AUTO": 自动识别（默认，根据文件路径判断）

        Returns:
            处理结果字典
        """
        try:
            # 0. 确保会话处于干净状态（回滚任何未完成的事务）
            if self.session.in_transaction():
                try:
                    self.session.rollback()
                    logger.info("[流程开始] 已回滚之前的未完成事务，确保会话状态干净")
                except Exception as e:
                    logger.warning(f"[流程开始] 回滚事务失败: {str(e)}")
            
            # 0.5 自动识别层级类型（如果未指定）
            if layer_type == "AUTO":
                layer_type = self._auto_detect_layer_type(input_path)
                logger.info(f"[流程开始] 自动识别层级类型: {layer_type}")
            
            # 1. 读取并处理SQL文件
            file_result = self._read_and_process_sql_files(input_path, is_directory)
            if not file_result.get('success', False):
                return file_result

            # ⚠️ 1.5 SELECT * 预检查（在调用 AI 之前）
            if layer_type in ["DIM", "DWD"]:
                for file_data in file_result.get('results', []):
                    if file_data.get('success', False):
                        sql_content = file_data.get('sql_content', '')
                        file_path = file_data.get('file_path', '')
                        
                        if sql_content:
                            select_star_check = self._check_select_star(sql_content)
                            if select_star_check['has_select_star']:
                                error_msg = (
                                    f"❌ SQL 中存在 SELECT * 或通配符（{select_star_check['matched_pattern']}），无法准确解析字段。\n"
                                    f"   文件：{file_path}\n"
                                    f"   原因：DIM/DWD 层需要精确的字段信息，必须将所有通配符展开为明确的字段列表\n"
                                    f"   建议：修改 SQL，将 {select_star_check['matched_pattern']} 替换为具体的字段名"
                                )
                                logger.error(error_msg)
                                
                                # 记录失败日志
                                self._log_parse_failure(
                                    file_path=file_path,
                                    failure_reason=error_msg,
                                    layer_type=layer_type,
                                    error_type="SELECT_STAR",
                                    sql_content=sql_content,
                                    matched_pattern=select_star_check['matched_pattern']
                                )
                                
                                return {
                                    "success": False,
                                    "message": error_msg,
                                    "needs_sql_improvement": True,
                                    "input_path": str(input_path)
                                }
                
                logger.info(f"[预检查] ✅ {layer_type} 层 SELECT * 校验通过")

            # ⚠️ 1.6 DWS/ADS 层严格校验：检查是否有 GROUP BY（在调用 AI 之前）
            if layer_type == "METRIC":
                for file_data in file_result.get('results', []):
                    if file_data.get('success', False):
                        sql_content = file_data.get('sql_content', '')
                        file_path = file_data.get('file_path', '')
                        
                        if sql_content:
                            has_group_by = self._check_has_group_by(sql_content)
                            if not has_group_by:
                                error_msg = (
                                    f"❌ DWS/ADS 层 ETL 脚本必须包含 GROUP BY 子句！\n"
                                    f"   文件：{file_path}\n"
                                    f"   原因：DWS/ADS 层是聚合层，必须通过 GROUP BY 进行数据聚合\n"
                                    f"   建议：检查 SQL 是否缺少 GROUP BY，或者该脚本应该属于 DWD 明细层"
                                )
                                logger.error(error_msg)
                                
                                # 记录失败日志
                                self._log_parse_failure(
                                    file_path=file_path,
                                    failure_reason=error_msg,
                                    layer_type=layer_type,
                                    error_type="MISSING_GROUP_BY",
                                    sql_content=sql_content
                                )
                                
                                return {
                                    "success": False,
                                    "message": error_msg,
                                    "needs_sql_improvement": True,
                                    "input_path": str(input_path)
                                }
                
                logger.info(f"[预检查] ✅ METRIC 层 GROUP BY 校验通过")

            # 2. 解析SQL内容（传递层级类型）
            parsed_results = self._parse_sql_files(file_result.get('results', []), layer_type)
            if not parsed_results or not any(r.get('success', False) for r in parsed_results):
                return {"success": False, "message": "SQL解析失败或未解析到有效数据"}

            logger.info(f"[流程] 第2步完成 - SQL解析成功，共 {len([r for r in parsed_results if r.get('success')])} 个文件")

            # 3. 根据层级类型委派给对应的 Service 处理（Service 内部负责数据准备和校验）
            # 初始化变量（避免作用域问题）
            execution_result = None
            insert_sqls = []
            validation_result = None
            table_stats = {}
            
            if layer_type == "DIM":
                logger.info("[流程] DIM 层：使用 DimService 处理")
                dim_result = self.dim_service.process(parsed_results)
                if not dim_result.get('success', False):
                    return dim_result
                
                execution_result, insert_sqls, validation_result = self._build_execution_result(
                    dim_result, 'DIM层处理成功', 'DIM层跳过SQL校验'
                )
                table_stats = dim_result.get('table_stats', {})
                
            elif layer_type == "DWD":
                logger.info("[流程] DWD 层：使用 LineageService 处理")
                lineage_result = self.lineage_service.process(parsed_results)
                if not lineage_result.get('success', False):
                    return lineage_result
                
                execution_result, insert_sqls, validation_result = self._build_execution_result(
                    lineage_result, 'DWD层处理成功', 'DWD层跳过SQL校验'
                )
                table_stats = lineage_result.get('table_stats', {})
                
            elif layer_type == "METRIC":
                logger.info("[流程] METRIC 层：使用 MetricsService 处理")
                metrics_result = self.metrics_service.process(parsed_results)
                if not metrics_result.get('success', False):
                    return metrics_result
                
                execution_result, insert_sqls, validation_result = self._build_execution_result(
                    metrics_result, 'METRIC层处理成功', 'METRIC层跳过SQL校验'
                )
                table_stats = metrics_result.get('table_stats', {})
            else:
                # 未知的层级类型
                error_msg = f"不支持的层级类型: {layer_type}"
                logger.error(error_msg)
                return {
                    "success": False,
                    "message": error_msg,
                    "input_path": str(input_path)
                }
            
            # 7. 确保事务已提交
            if self.session.in_transaction():
                try:
                    self.session.commit()
                    
                    # 输出简洁的流程完成日志（详细统计由各 Service 输出）
                    file_paths_count = len(file_result.get('results', [])) if file_result.get('success') else 0
                    file_name = Path(str(input_path)).name if not is_directory else f"{file_paths_count}个文件"
                    logger.info(f"✅ 流程完成 | {file_name}")
                    
                except Exception as commit_error:
                    logger.error(f"[流程结束] 提交事务失败: {str(commit_error)}")
                    self.session.rollback()
                    return {
                        "success": False,
                        "message": f"事务提交失败：{str(commit_error)}",
                        "input_path": str(input_path)
                    }

            # 8. 返回总体结果
            return {
                "success": True,
                "file_result": file_result,
                "parsed_results": parsed_results,
                "insert_sqls": insert_sqls,
                "validation_result": validation_result,
                "execution_result": execution_result,
                "message": "指标平台自动化建设流程完成"
            }

        except Exception as e:
            logger.error(f"指标平台处理流程失败：{str(e)}")
            # 发生异常时回滚事务
            try:
                if self.session.in_transaction():
                    self.session.rollback()
                    logger.info("[流程异常] 已回滚事务")
            except Exception as rollback_error:
                logger.error(f"[流程异常] 回滚事务失败: {str(rollback_error)}")
            
            return {
                "success": False,
                "message": f"处理流程失败：{str(e)}",
                "input_path": str(input_path)
            }

    def _build_execution_result(self, service_result: Dict[str, Any], default_message: str, validation_message: str) -> tuple:
        """
        构造统一的执行结果
        
        Args:
            service_result: Service 返回的结果
            default_message: 默认成功消息
            validation_message: 校验消息
            
        Returns:
            (execution_result, insert_sqls, validation_result)
        """
        table_stats = service_result.get('table_stats', {})
        
        execution_result = {
            'success': True,
            'message': service_result.get('message', default_message),
            'table_stats': table_stats
        }
        insert_sqls = []  # 各 Service 直接插入，不生成 SQL
        validation_result = {"success": True, "message": validation_message}
        
        return execution_result, insert_sqls, validation_result

    def _auto_detect_layer_type(self, input_path: Union[str, Path]) -> str:
        """
        根据文件路径自动识别数仓层级类型
        
        Args:
            input_path: 文件路径或目录路径
            
        Returns:
            层级类型："DIM"、"DWD" 或 "METRIC"
            - DIM: 维度定义层
            - DWD: 明细层（只提取血缘，不提取指标/维度）
            - METRIC: 指标层（dws/ads，需要提取指标和维度）
        """
        path_str = str(input_path).lower()
        
        # 检查路径中是否包含 dim 关键词
        if '/dim/' in path_str or '\\dim\\' in path_str or path_str.endswith('/dim') or path_str.endswith('\\dim'):
            return "DIM"
        
        # 检查路径中是否包含 dwd 关键词（明细层）
        if '/dwd/' in path_str or '\\dwd\\' in path_str or path_str.endswith('/dwd') or path_str.endswith('\\dwd'):
            return "DWD"
        
        # 其他情况默认为指标层（dws/ads）
        return "METRIC"

    def _check_has_group_by(self, sql_content: str) -> bool:
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
    


    def _read_and_process_sql_files(self, input_path: Union[str, Path], is_directory: bool) -> Dict[str, Any]:
        """
        读取并处理SQL文件

        Args:
            input_path: 输入路径
            is_directory: 是否为目录模式

        Returns:
            文件处理结果
        """
        try:
            # 验证输入路径
            path = Path(input_path)
            if not path.exists():
                return {"success": False, "message": f"路径不存在：{input_path}"}

            # 获取文件列表
            file_paths = []
            if is_directory:
                # 目录模式：获取目录下所有.sql文件
                file_paths = list(path.glob("*.sql"))
                if not file_paths:
                    return {"success": False, "message": f"目录中没有找到SQL文件：{input_path}"}
            else:
                # 文件模式：直接使用指定文件
                if path.suffix.lower() != '.sql':
                    return {"success": False, "message": f"文件必须是.sql格式：{input_path}"}
                file_paths = [path]

            # 处理每个文件
            total_files = len(file_paths)
            processed_files = 0
            failed_files = 0
            results = []

            for file_path in file_paths:
                try:
                    # 读取文件内容
                    with open(file_path, 'r', encoding='utf-8') as f:
                        sql_content = f.read()

                    # 处理单个文件
                    file_result = {
                        "file_path": str(file_path),
                        "sql_content": sql_content,
                        "success": True,
                        "message": "文件读取成功"
                    }
                    results.append(file_result)
                    processed_files += 1

                except Exception as e:
                    failed_files += 1
                    logger.error(f"读取文件 {file_path} 失败：{str(e)}")
                    results.append({
                        "file_path": str(file_path),
                        "success": False,
                        "message": f"文件读取失败：{str(e)}"
                    })

            return {
                "success": True,
                "total_files": total_files,
                "processed_files": processed_files,
                "failed_files": failed_files,
                "results": results,
                "message": f"成功读取 {processed_files}/{total_files} 个文件"
            }

        except Exception as e:
            logger.error(f"读取SQL文件失败：{str(e)}")
            return {
                "success": False,
                "message": f"读取文件失败：{str(e)}",
                "input_path": str(input_path)
            }

    def _parse_sql_files(self, file_results: List[Dict[str, Any]], layer_type: str = "METRIC") -> List[Dict[str, Any]]:
        """
        解析SQL文件内容

        Args:
            file_results: 文件处理结果列表
            layer_type: 数仓层级类型（DIM/DWD/METRIC）

        Returns:
            解析结果列表
        """
        parsed_results = []

        for file_result in file_results:
            if not file_result.get('success', False):
                parsed_results.append({
                    "file_path": file_result.get('file_path'),
                    "success": False,
                    "message": file_result.get('message'),
                    "parsed_data": None
                })
                continue

            try:
                sql_content = file_result.get('sql_content', '')
                file_path = file_result.get('file_path', '')

                # 调用大模型解析SQL（传递层级类型）
                parsed_data = self._parse_sql_with_ai(sql_content, file_path, layer_type)
                logger.info(f"[大模型SQL生成-解析结果] {parsed_data}")
                if not parsed_data or not parsed_data.get('success', False):
                    parsed_results.append({
                        "file_path": file_path,
                        "success": False,
                        "message": "大模型解析失败",
                        "parsed_data": None
                    })
                    continue

                parsed_results.append({
                    "file_path": file_path,
                    "success": True,
                    "message": "SQL解析成功",
                    "parsed_data": parsed_data.get('parsed_data', {})
                })

            except Exception as e:
                logger.error(f"解析文件 {file_result.get('file_path')} 失败：{str(e)}")
                parsed_results.append({
                    "file_path": file_result.get('file_path'),
                    "success": False,
                    "message": f"解析失败：{str(e)}",
                    "parsed_data": None
                })

        return parsed_results

    def _check_select_star(self, sql_content: str) -> Dict[str, Any]:
        """
        使用正则表达式检查 SQL 中是否存在 SELECT * 或 表别名.*
        
        Args:
            sql_content: SQL 内容
            
        Returns:
            {
                'has_select_star': bool,  # 是否包含通配符
                'matched_pattern': str     # 匹配到的模式（如 'a.*' 或 'SELECT *'）
            }
        """
        import re
        
        # 移除注释（单行注释 -- 和 /* */）
        sql_cleaned = re.sub(r'--.*?$', '', sql_content, flags=re.MULTILINE)
        sql_cleaned = re.sub(r'/\*.*?\*/', '', sql_cleaned, flags=re.DOTALL)
        
        # 提取 SELECT 子句（从 SELECT 到 FROM）
        select_pattern = r'SELECT\s+(.*?)\s+FROM'
        match = re.search(select_pattern, sql_cleaned, re.IGNORECASE | re.DOTALL)
        
        if not match:
            return {'has_select_star': False, 'matched_pattern': ''}
        
        select_clause = match.group(1)
        
        # 检测模式1：单独的 *
        if re.search(r'\bSELECT\s+\*', sql_cleaned, re.IGNORECASE):
            return {'has_select_star': True, 'matched_pattern': 'SELECT *'}
        
        # 检测模式2：表别名.* （如 a.*, b.*, t1.*）
        alias_star_match = re.search(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\.\*', select_clause, re.IGNORECASE)
        if alias_star_match:
            alias = alias_star_match.group(1)
            return {'has_select_star': True, 'matched_pattern': f'{alias}.*'}
        
        # 检测模式3：表名.* （完整表名，如 schema.table.*）
        table_star_match = re.search(r'\b([a-zA-Z_][a-zA-Z0-9_.]*)\.\*', select_clause, re.IGNORECASE)
        if table_star_match:
            table_name = table_star_match.group(1)
            return {'has_select_star': True, 'matched_pattern': f'{table_name}.*'}
        
        return {'has_select_star': False, 'matched_pattern': ''}

    def _log_parse_failure(
        self,
        file_path: str,
        failure_reason: str,
        layer_type: Optional[str] = None,
        error_type: Optional[str] = None,
        sql_content: Optional[str] = None,
        matched_pattern: Optional[str] = None
    ):
        """
        记录 SQL 解析失败日志
        
        Args:
            file_path: SQL 文件路径
            failure_reason: 失败原因
            layer_type: 层级类型
            error_type: 错误类型
            sql_content: SQL 内容
            matched_pattern: 匹配到的模式
        """
        try:
            from apps.extend.metrics2.curd.sql_parse_failure_log_curd import create_failure_log
            import os
            
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
            
            logger.info(f"[失败日志] 已记录: {file_name} - {error_type}")
            
        except Exception as e:
            # 日志记录失败不应影响主流程
            logger.error(f"[失败日志] 记录失败: {str(e)}")

    def _handle_parse_error(
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
        self._log_parse_failure(
            file_path=sql_file,
            failure_reason=error_msg,
            layer_type=layer_type,
            error_type=error_type,
            sql_content=sql_content,
            matched_pattern=matched_pattern
        )
        
        return {"success": False, "message": error_msg}

    def _parse_sql_with_ai(self, sql_content: str, sql_file: str = "", layer_type: str = "METRIC") -> Dict[str, Any]:
        """
        使用大模型解析SQL内容

        Args:
            sql_content: SQL内容
            sql_file: SQL文件名（可选）
            layer_type: 数仓层级类型（DIM/DWD/METRIC）

        Returns:
            解析结果字典
        """
        try:
            # 调用大模型解析SQL（传递层级类型）
            result = self.model_client.call_ai(
                template_name="sql_analysis",
                sql_content=sql_content,
                sql_file=sql_file,
                layer_type=layer_type
            )

            if not result:
                error_msg = "大模型返回空内容"
                logger.error(f"[AI解析] {error_msg}")
                logger.error(f"[AI解析] 文件: {sql_file}")
                logger.error(f"[AI解析] SQL 内容前200字符: {sql_content[:200]}")
                return self._handle_parse_error(
                    sql_file=sql_file,
                    error_msg=error_msg,
                    layer_type=layer_type,
                    error_type="AI_EMPTY_RESPONSE",
                    sql_content=sql_content
                )

            # 检查返回内容是否为有效的 JSON（先打印日志）
            logger.debug(f"[AI解析] AI 返回内容长度: {len(result)} 字符")
            logger.debug(f"[AI解析] AI 返回内容前300字符: {result[:300]}")

            # 解析大模型返回的JSON
            try:
                parsed_json = json.loads(result)
            except json.JSONDecodeError as e:
                error_msg = f"JSON解析失败：{str(e)}"
                logger.error(error_msg)
                logger.error(f"[AI解析] 文件: {sql_file}")
                logger.error(f"[AI解析] AI 返回完整内容:\n{result}")
                logger.error(f"[AI解析] SQL 内容前200字符: {sql_content[:200]}")
                return self._handle_parse_error(
                    sql_file=sql_file,
                    error_msg=error_msg,
                    layer_type=layer_type,
                    error_type="JSON_PARSE_ERROR",
                    sql_content=sql_content
                )

            # ⚠️ 强制覆盖 basic_info.file_name 为实际文件路径（不让 AI 解析）
            if 'basic_info' not in parsed_json:
                parsed_json['basic_info'] = {}
            parsed_json['basic_info']['file_name'] = sql_file
            logger.debug(f"[AI解析] 设置 file_name: {sql_file}")

            # ⚠️ 保存原始 SQL 内容用于后续校验
            if 'basic_info' in parsed_json:
                parsed_json['basic_info']['sql_content'] = sql_content

            # 返回统一的解析结果（不区分层级，由各 Service 自行处理）
            return {
                "success": True,
                "parsed_data": parsed_json
            }

        except json.JSONDecodeError as e:
            return self._handle_parse_error(
                sql_file=sql_file,
                error_msg=f"JSON解析失败：{str(e)}",
                layer_type=layer_type,
                error_type="JSON_DECODE_EXCEPTION",
                sql_content=sql_content
            )
        except Exception as e:
            return self._handle_parse_error(
                sql_file=sql_file,
                error_msg=f"大模型解析失败：{str(e)}",
                layer_type=layer_type,
                error_type="EXCEPTION",
                sql_content=sql_content
            )


