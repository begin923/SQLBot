from typing import List, Dict, Any, Optional, Union
from pathlib import Path
import logging
import json
import re
from sqlalchemy.orm import Session
from apps.extend.metrics2.services.dim_service import DimService
from apps.extend.metrics2.services.lineage_service import LineageService
from apps.extend.metrics2.services.metrics_service import MetricsService
from apps.extend.metrics2.services.exception_service import ExceptionService  # ⚠️ 重命名
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
        
        # ⚠️ 先创建 LineageService（共享实例）
        self.lineage_service = LineageService(session)
        
        # ⚠️ 将 LineageService 实例传递给 MetricsService，实现缓存共享
        self.dim_service = DimService(session)
        self.metrics_service = MetricsService(session, lineage_service=self.lineage_service)
        
        # ⚠️ 创建异常服务（统一管理异常处理）
        self.exception_service = ExceptionService(session)
        
        logger.info("[MetricsPlatformService] 初始化完成 - LineageService 已共享给 MetricsService")
        

    def process_metrics_from_sql(self, input_path: Union[str, Path], is_directory: bool = False, layer_type: str = "AUTO") -> Dict[str, Any]:
        """
        从SQL文件处理指标（完整流程：读取文件 -> 解析 -> 规则处理 -> 生成SQL -> 写入表）

        Args:
            input_path: 输入路径（文件路径或目录路径）
            is_directory: 是否为目录模式
            layer_type: 数仓层级类型
                - "DIM": 维度定义层（只提取维度，写入 dim_definition/dim_field_mapping）
                - "DWD": 明细数据层（直接走 LineageService，提取表血缘和字段血缘）
                - "DWS": 汇总数据层（根据是否有 GROUP BY 决定走 MetricsService 还是 LineageService）
                - "ADS": 应用数据层（根据是否有 GROUP BY 决定走 MetricsService 还是 LineageService）
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
            
            # ⚠️ 0.5 自动识别层级类型（如果未指定）
            if layer_type == "AUTO":
                layer_type = self._auto_detect_layer_type(input_path)
                logger.info(f"[流程开始] 自动识别层级类型: {layer_type}")
            
            # ⚠️ 0.6 DWS/ADS 层自动分流逻辑：根据是否有 GROUP BY 决定走 METRIC 还是 WIDE 流程
            processing_flow = None
            if layer_type in ["DWS", "ADS"]:
                processing_flow = self._determine_processing_flow(input_path, is_directory)
                logger.info(f"[{layer_type} 分流] 检测到 {'有' if processing_flow == 'METRIC' else '无'} GROUP BY，使用 {processing_flow} 流程")
            
            # 1. 读取并处理SQL文件
            file_result = self._read_and_process_sql_files(input_path, is_directory)
            if not file_result.get('success', False):
                return file_result

            # ⚠️ 1.5 SELECT * 预检查（在调用 AI 之前，针对 DIM 层和 DWD 层）
            should_check_select_star = (layer_type == "DIM") or (
                layer_type == "DWD"
            )
            
            if should_check_select_star:
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
                                    f"   原因：{'DIM' if layer_type == 'DIM' else 'DWD'} 层需要精确的字段信息，必须将所有通配符展开为明确的字段列表\n"
                                    f"   建议：修改 SQL，将 {select_star_check['matched_pattern']} 替换为具体的字段名"
                                )
                                logger.error(error_msg)
                                
                                # ⚠️ 记录失败日志（使用 ExceptionService）
                                self.exception_service.log_parse_failure(
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
                
                check_target = "DIM 层" if layer_type == "DIM" else "DWD 层"
                logger.info(f"[预检查] ✅ {check_target} SELECT * 校验通过")

            # ⚠️ 1.6 移除原有的 GROUP BY 校验逻辑（已在 0.6 步处理）

            # 2. 解析SQL内容（传递层级类型）
            # ⚠️ 对于 DWS/ADS 层，AI 解析时使用实际的 processing_flow（METRIC 或 WIDE）
            ai_layer_type = processing_flow if layer_type in ["DWS", "ADS"] else layer_type
            parsed_results = self._parse_sql_files(file_result.get('results', []), ai_layer_type)
            if not parsed_results or not any(r.get('success', False) for r in parsed_results):
                return {"success": False, "message": "SQL解析失败或未解析到有效数据"}

            logger.info(f"[流程] 第2步完成 - SQL解析成功，共 {len([r for r in parsed_results if r.get('success')])} 个文件")

            # 3. 根据层级类型委派给对应的 Service 处理（Service 内部负责数据准备和校验）
            # 初始化变量（避免作用域问题）
            execution_result = None
            insert_sqls = []
            validation_result = None
            table_stats = {}
            
            try:
                if layer_type == "DIM":
                    logger.info("[流程] DIM 层：使用 DimService 处理")
                    dim_result = self.dim_service.process(parsed_results)
                    
                    execution_result, insert_sqls, validation_result = self._build_execution_result(
                        dim_result, 'DIM层处理成功', 'DIM层跳过SQL校验'
                    )
                    table_stats = dim_result.get('table_stats', {})
                    
                    # ⚠️ 验证DIM层解析完整性
                    validation_msg = self._validate_parse_completeness(
                        parsed_results=parsed_results,
                        execution_result=dim_result,
                        layer_type="DIM"
                    )
                    if validation_msg:
                        logger.warning(f"[完整性校验] DIM层: {validation_msg}")
                        # 注意：这里只记录警告，不中断流程
                    
                elif layer_type == "DWD":
                    logger.info("[流程] DWD 层：使用 LineageService 处理")
                    lineage_result = self.lineage_service.process(parsed_results)
                    
                    execution_result, insert_sqls, validation_result = self._build_execution_result(
                        lineage_result, 'DWD层处理成功', 'DWD层跳过SQL校验'
                    )
                    table_stats = lineage_result.get('table_stats', {})
                    
                    # ⚠️ 验证DWD层解析完整性
                    validation_msg = self._validate_parse_completeness(
                        parsed_results=parsed_results,
                        execution_result=lineage_result,
                        layer_type="WIDE"
                    )
                    if validation_msg:
                        logger.warning(f"[完整性校验] DWD层: {validation_msg}")
                        # 注意：这里只记录警告，不中断流程
                    
                elif layer_type in ["DWS", "ADS"]:
                    # ⚠️ DWS/ADS 层根据 processing_flow 分流
                    if processing_flow == "WIDE":
                        logger.info(f"[{layer_type} 层] 无 GROUP BY，使用 LineageService 处理")
                        lineage_result = self.lineage_service.process(parsed_results)
                        
                        execution_result, insert_sqls, validation_result = self._build_execution_result(
                            lineage_result, f'{layer_type}层(WIDE流程)处理成功', 'WIDE流程跳过SQL校验'
                        )
                        table_stats = lineage_result.get('table_stats', {})
                        
                        # ⚠️ 验证WIDE流程解析完整性
                        validation_msg = self._validate_parse_completeness(
                            parsed_results=parsed_results,
                            execution_result=lineage_result,
                            layer_type="WIDE"
                        )
                        if validation_msg:
                            logger.warning(f"[完整性校验] {layer_type}(WIDE): {validation_msg}")
                            # 注意：这里只记录警告，不中断流程（因为血缘已成功写入）
                    else:  # processing_flow == "METRIC"
                        logger.info(f"[{layer_type} 层] 有 GROUP BY，使用 MetricsService 处理")
                        metrics_result = self.metrics_service.process(parsed_results)
                        
                        execution_result, insert_sqls, validation_result = self._build_execution_result(
                            metrics_result, f'{layer_type}层(METRIC流程)处理成功', 'METRIC流程跳过SQL校验'
                        )
                        table_stats = metrics_result.get('table_stats', {})
                        
                        # ⚠️ 验证METRIC流程解析完整性
                        validation_msg = self._validate_parse_completeness(
                            parsed_results=parsed_results,
                            execution_result=metrics_result,
                            layer_type="METRIC"
                        )
                        if validation_msg:
                            logger.warning(f"[完整性校验] {layer_type}(METRIC): {validation_msg}")
                            # 注意：这里只记录警告，不中断流程
                else:
                    # 未知的层级类型
                    error_msg = f"不支持的层级类型: {layer_type}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)  # ⚠️ 抛出异常
                    
            except Exception as service_error:
                # ⚠️ 捕获 Service 层的未预期异常（如数据库约束冲突）
                # ⚠️ 对于 DWS/ADS 层，使用 processing_flow；其他层直接使用 layer_type
                actual_layer = processing_flow if (layer_type in ["DWS", "ADS"] and processing_flow) else layer_type
                error_msg = f"{actual_layer} 层处理异常: {str(service_error)}"
                logger.error(f"[流程异常] {error_msg}", exc_info=True)
                
                # ⚠️ 记录失败日志（使用 ExceptionService）
                recorded_count = self.exception_service.log_service_failure(
                    parsed_results=parsed_results,
                    service_name=f"{actual_layer}Service",
                    error_message=error_msg,
                    layer_type=actual_layer,
                    exception=service_error
                )
                
                # 回滚事务
                if self.session.in_transaction():
                    self.session.rollback()
                    logger.info("[流程异常] 已回滚事务")
                
                return {
                    "success": False,
                    "message": error_msg,
                    "input_path": str(input_path)
                }
            
            # 7. 统一提交事务（由主流程管理）
            try:
                self.session.commit()
                logger.info("[流程结束] ✅ 主流程已提交事务")
                
                # ⚠️ 8. 只有处理成功时，才标记相关的失败日志为已解决
                resolved_count = self.exception_service.mark_failures_as_resolved(file_result.get('results', []))
                if resolved_count > 0:
                    logger.info(f"[流程结束] ✅ 已标记 {resolved_count} 条失败记录为已解决")
                
                # 输出简洁的流程完成日志（详细统计由各 Service 输出）
                file_paths_count = len(file_result.get('results', [])) if file_result.get('success') else 0
                file_name = Path(str(input_path)).name if not is_directory else f"{file_paths_count}个文件"
                logger.info(f"✅ 流程完成 | {file_name}")
                    
            except Exception as commit_error:
                logger.error(f"[流程结束] ❌ 提交事务失败: {str(commit_error)}")
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
            层级类型："DIM" / "DWD" / "DWS" / "ADS"
        """
        path_str = str(input_path).lower()
        
        # 直接匹配路径中的关键词
        if '/dim/' in path_str or '\\dim\\' in path_str:
            return "DIM"
        elif '/dwd/' in path_str or '\\dwd\\' in path_str:
            return "DWD"
        elif '/dws/' in path_str or '\\dws\\' in path_str:
            return "DWS"
        elif '/ads/' in path_str or '\\ads\\' in path_str:
            return "ADS"
        
        # 其他情况默认为 DWD 层
        logger.warning(f"[自动识别] 无法从路径识别层级: {input_path}，默认为 DWD")
        return "DWD"

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

    def _parse_sql_files(self, file_results: List[Dict[str, Any]], layer_type: str = "METRIC", original_detected_layer: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        解析SQL文件内容

        Args:
            file_results: 文件处理结果列表
            layer_type: 数仓层级类型（DIM/WIDE/METRIC）
            original_detected_layer: 原始检测到的层级（用于修正AI判断错误）

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
                parsed_data = self._parse_sql_with_ai(sql_content, file_path, layer_type, original_detected_layer)
                logger.debug(f"[大模型SQL生成-解析结果] {parsed_data}")
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
        【极简版】只拦截最极端的 SELECT * 情况
        
        策略：
        1. 只拦截 INSERT INTO ... SELECT * FROM （最危险且明确的情况）
        2. 其他情况交给 AI 解析，AI 解析失败会记录到 sql_parse_failure_log
        3. 人工干预处理异常情况
        
        Args:
            sql_content: SQL 内容
            
        Returns:
            {
                'has_select_star': bool,  # 是否包含极端通配符
                'matched_pattern': str     # 匹配到的模式
            }
        """
        import re
        
        # 移除注释
        sql_cleaned = re.sub(r'--.*?$', '', sql_content, flags=re.MULTILINE)
        sql_cleaned = re.sub(r'/\*.*?\*/', '', sql_cleaned, flags=re.DOTALL)
        
        # ⚠️ 只拦截最明显的危险模式：INSERT INTO table SELECT * FROM
        # 这是最危险且无争议的情况，必须拦截
        dangerous_patterns = [
            r'INSERT\s+INTO\s+\w+\s+SELECT\s+\*\s+FROM',  # INSERT INTO t SELECT * FROM
            r'INSERT\s+INTO\s+\w+\s+SELECT\s+\w+\.\*\s+FROM',  # INSERT INTO t SELECT a.* FROM
        ]
        
        for pattern in dangerous_patterns:
            match = re.search(pattern, sql_cleaned, re.IGNORECASE)
            if match:
                matched_text = match.group(0)
                # 提取通配符模式
                if '.*' in matched_text:
                    alias_match = re.search(r'(\w+)\.\*', matched_text)
                    if alias_match:
                        return {'has_select_star': True, 'matched_pattern': f'{alias_match.group(1)}.*'}
                return {'has_select_star': True, 'matched_pattern': 'SELECT *'}
        
        # 其他情况放行，交给 AI 解析
        return {'has_select_star': False, 'matched_pattern': ''}

    def _parse_sql_with_ai(self, sql_content: str, sql_file: str = "", layer_type: str = "METRIC", original_detected_layer: Optional[str] = None) -> Dict[str, Any]:
        """
        使用大模型解析SQL内容

        Args:
            sql_content: SQL内容
            sql_file: SQL文件名（可选）
            layer_type: 数仓层级类型（DIM/DWD/METRIC）
            original_detected_layer: 原始检测到的层级（用于修正AI判断错误）

        Returns:
            解析结果字典
        """
        try:
            # ⚠️ 记录调用的提示词模板和层级类型
            logger.info(f"[AI解析] 调用提示词模板: sql_analysis, layer_type={layer_type}, original_detected_layer={original_detected_layer}")
            
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
                # ⚠️ 使用 ExceptionService 处理错误
                return self.exception_service.handle_parse_error(
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
                print(f"[AI解析] 解析结果: {parsed_json}")
            except json.JSONDecodeError as e:
                error_msg = f"JSON解析失败：{str(e)}"
                logger.error(error_msg)
                logger.error(f"[AI解析] 文件: {sql_file}")
                # ⚠️ 使用 ExceptionService 处理错误
                return self.exception_service.handle_parse_error(
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

            # ⚠️ 强制修正 warehouse_layer（不再使用 original_detected_layer）
            # 根据当前流程设置合理的默认值
            ai_layer = parsed_json.get('basic_info', {}).get('warehouse_layer', '').lower()
            if layer_type == "WIDE":
                # WIDE 流程（宽表/明细层），保持原始数仓层级（dwd/dws/ads）
                # 如果 AI 没有正确识别，则从文件路径推断
                if ai_layer not in ['dwd', 'dws', 'ads']:
                    # 尝试从文件路径推断
                    file_path_lower = sql_file.lower()
                    if '/dwd/' in file_path_lower or '\\dwd\\' in file_path_lower:
                        parsed_json['basic_info']['warehouse_layer'] = 'dwd'
                        logger.info(f"[AI解析] 从路径推断 warehouse_layer 为 dwd")
                    elif '/dws/' in file_path_lower or '\\dws\\' in file_path_lower:
                        parsed_json['basic_info']['warehouse_layer'] = 'dws'
                        logger.info(f"[AI解析] 从路径推断 warehouse_layer 为 dws")
                    elif '/ads/' in file_path_lower or '\\ads\\' in file_path_lower:
                        parsed_json['basic_info']['warehouse_layer'] = 'ads'
                        logger.info(f"[AI解析] 从路径推断 warehouse_layer 为 ads")
                    else:
                        parsed_json['basic_info']['warehouse_layer'] = 'dwd'  # 默认
                        logger.info(f"[AI解析] 默认设置 warehouse_layer 为 dwd")
                else:
                    logger.info(f"[AI解析] 保持AI判断的 warehouse_layer: {ai_layer}")
            elif layer_type == "METRIC":
                # METRIC 流程，保持 AI 的判断（dws 或 ads）
                if ai_layer not in ['dws', 'ads']:
                    parsed_json['basic_info']['warehouse_layer'] = 'dws'
                    logger.info(f"[AI解析] 默认设置 warehouse_layer 为 dws (METRIC流程)")
                else:
                    logger.info(f"[AI解析] 保持AI判断的 warehouse_layer: {ai_layer}")
            elif layer_type == "DIM":
                # DIM 流程，设置为 dim
                parsed_json['basic_info']['warehouse_layer'] = 'dim'
                logger.info(f"[AI解析] 设置 warehouse_layer 为 dim (DIM流程)")

            # ⚠️ 保存原始 SQL 内容用于后续校验
            if 'basic_info' in parsed_json:
                parsed_json['basic_info']['sql_content'] = sql_content

            # 返回统一的解析结果（不区分层级，由各 Service 自行处理）
            return {
                "success": True,
                "parsed_data": parsed_json
            }

        except json.JSONDecodeError as e:
            # ⚠️ 使用 ExceptionService 处理错误
            return self.exception_service.handle_parse_error(
                sql_file=sql_file,
                error_msg=f"JSON解析失败：{str(e)}",
                layer_type=layer_type,
                error_type="JSON_DECODE_EXCEPTION",
                sql_content=sql_content
            )
        except Exception as e:
            # ⚠️ 使用 ExceptionService 处理错误
            return self.exception_service.handle_parse_error(
                sql_file=sql_file,
                error_msg=f"大模型解析失败：{str(e)}",
                layer_type=layer_type,
                error_type="EXCEPTION",
                sql_content=sql_content
            )

    def _determine_processing_flow(self, input_path: Union[str, Path], is_directory: bool) -> str:
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
                    
                    if self._check_has_group_by(sql_content):
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
    
    def _validate_parse_completeness(
        self,
        parsed_results: List[Dict[str, Any]],
        execution_result: Dict[str, Any],
        layer_type: str
    ) -> Optional[str]:
        """
        验证解析结果的完整性
        
        Args:
            parsed_results: 解析结果列表
            execution_result: 执行结果（包含table_stats）
            layer_type: 层级类型
            
        Returns:
            如果有问题返回警告消息，否则返回None
        """
        issues = []
        
        if not parsed_results:
            return "没有解析结果"
        
        first_result = parsed_results[0]
        if not first_result.get('success', False):
            return "解析本身失败"
        
        parsed_data = first_result.get('parsed_data', {})
        
        # 根据不同层级设置不同的期望
        if layer_type == "WIDE":
            # WIDE层（宽表/明细层）应该有表血缘和字段血缘
            table_stats = execution_result.get('table_stats', {})
            
            # ⚠️ 优先检查表血缘
            table_lineage_count = table_stats.get('table_lineage', 0)
            if table_lineage_count == 0:
                issues.append("WIDE层未提取到任何表血缘")
            
            field_lineage_count = table_stats.get('field_lineage', 0)
            if field_lineage_count == 0:
                issues.append("WIDE层未提取到任何字段血缘")
            
            # 检查是否有基本的表信息
            basic_info = parsed_data.get('basic_info', {})
            if not basic_info.get('target_table'):
                issues.append("未识别到目标表")
        
        elif layer_type == "METRIC":
            # METRIC层应该有指标定义
            metric_definitions = parsed_data.get('metric_definition', [])  # ⚠️ 修正：使用单数形式
            if len(metric_definitions) == 0:
                issues.append("指标层未生成任何指标定义")
        
        elif layer_type == "DIM":
            # DIM层应该有维度定义
            dim_definitions = parsed_data.get('dim_definition', [])  # ⚠️ 修正：使用单数形式
            if len(dim_definitions) == 0:
                issues.append("DIM层未生成任何维度定义")
        
        if issues:
            return "; ".join(issues)
        
        return None
