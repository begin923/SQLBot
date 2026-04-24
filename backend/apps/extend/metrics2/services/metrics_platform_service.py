from typing import List, Dict, Any, Optional, Union
from pathlib import Path
import logging
import json
from sqlalchemy.orm import Session
from apps.extend.metrics2.services.dim_service import DimService
from apps.extend.metrics2.services.lineage_service import LineageService
from apps.extend.metrics2.services.metrics_service import MetricsService
from apps.extend.metrics2.services.exception_service import ExceptionService  # ⚠️ 重命名
from apps.extend.utils.utils import ModelClient

# ⚠️ 统一配置日志（模块加载时执行）
logging.basicConfig(
    level=logging.INFO,  # 基础级别设为 INFO，会输出 INFO、WARNING、ERROR
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True  # 强制覆盖已有的配置
)

# 配置日志
logger = logging.getLogger("MetricsPlatformService")

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
    
    def _initialize_session(self):
        """
        初始化会话状态（回滚任何未完成的事务）
        """
        if self.session.in_transaction():
            try:
                self.session.rollback()
                logger.info("[流程开始] 已回滚之前的未完成事务，确保会话状态干净")
            except Exception as e:
                logger.warning(f"[流程开始] 回滚事务失败: {str(e)}")
    
    def _cleanup_session(self):
        """
        清理会话状态（最终清理）
        """
        try:
            if self.session.is_active:
                # 如果还有未提交的事务，回滚
                if self.session.in_transaction():
                    self.session.rollback()
                    logger.debug("[流程清理] 回滚未提交的事务")
                logger.debug("[流程清理] 会话保持活跃状态（由调用方管理）")
            else:
                logger.debug("[流程清理] 会话已关闭")
        except Exception as cleanup_error:
            logger.warning(f"[流程清理] 清理会话时出错: {str(cleanup_error)}")
    
    def _validate_and_detect_mode(self, input_path: Union[str, Path]) -> Dict[str, Any]:
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
        layer_type = self._auto_detect_layer_type(input_path)
        logger.info(f"[流程开始] 自动识别层级类型: {layer_type}")
        
        # DWS/ADS 层自动分流逻辑
        processing_flow = None
        if layer_type in ["DWS", "ADS"]:
            processing_flow = self._determine_processing_flow(input_path, is_directory)
            logger.info(f"[{layer_type} 分流] 检测到 {'有' if processing_flow == 'METRIC' else '无'} GROUP BY，使用 {processing_flow} 流程")
        
        return {
            'success': True,
            'is_directory': is_directory,
            'layer_type': layer_type,
            'processing_flow': processing_flow
        }
    
    def _pre_check_sql_quality(self, file_result: Dict[str, Any], layer_type: str, input_path: Union[str, Path]) -> Dict[str, Any]:
        """
        预检查 SQL 质量（SELECT * 检查）
        
        Args:
            file_result: 文件读取结果
            layer_type: 层级类型
            input_path: 输入路径
            
        Returns:
            检查结果
        """
        should_check_select_star = (layer_type == "DIM") or (layer_type == "DWD")
        
        if not should_check_select_star:
            return {'success': True}
        
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
                        
                        # 记录失败日志
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
        return {'success': True}
    
    def _dispatch_to_service(self, parsed_results: List[Dict[str, Any]], layer_type: str, processing_flow: Optional[str]) -> Dict[str, Any]:
        """
        根据层级类型委派给对应的 Service 处理
        
        Args:
            parsed_results: 解析结果列表
            layer_type: 层级类型
            processing_flow: 处理流程（仅 DWS/ADS 层）
            
        Returns:
            {
                'success': bool,
                'execution_result': dict,
                'insert_sqls': list,
                'validation_result': dict,
                'table_stats': dict
            }
        """
        try:
            if layer_type == "DIM":
                logger.info("[流程] DIM 层：使用 DimService 处理")
                service_result = self.dim_service.process(parsed_results, layer_type=layer_type)
                execution_result, insert_sqls, validation_result = self._build_execution_result(
                    service_result, 'DIM层处理成功', 'DIM层跳过SQL校验'
                )
                table_stats = service_result.get('table_stats', {})
                
            elif layer_type == "DWD":
                logger.info("[流程] DWD 层：使用 LineageService 处理")
                service_result = self.lineage_service.process(parsed_results, layer_type=layer_type)
                execution_result, insert_sqls, validation_result = self._build_execution_result(
                    service_result, 'DWD层处理成功', 'DWD层跳过SQL校验'
                )
                table_stats = service_result.get('table_stats', {})
                
            elif layer_type in ["DWS", "ADS"]:
                if processing_flow == "WIDE":
                    logger.info(f"[{layer_type} 层] 无 GROUP BY，使用 LineageService 处理")
                    service_result = self.lineage_service.process(parsed_results, layer_type=layer_type)
                    execution_result, insert_sqls, validation_result = self._build_execution_result(
                        service_result, f'{layer_type}层(WIDE流程)处理成功', 'WIDE流程跳过SQL校验'
                    )
                    table_stats = service_result.get('table_stats', {})
                else:  # processing_flow == "METRIC"
                    logger.info(f"[{layer_type} 层] 有 GROUP BY，使用 MetricsService 处理")
                    service_result = self.metrics_service.process(parsed_results, layer_type=layer_type)
                    execution_result, insert_sqls, validation_result = self._build_execution_result(
                        service_result, f'{layer_type}层(METRIC流程)处理成功', 'METRIC流程跳过SQL校验'
                    )
                    table_stats = service_result.get('table_stats', {})
            else:
                error_msg = f"不支持的层级类型: {layer_type}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            return {
                'success': True,
                'execution_result': execution_result,
                'insert_sqls': insert_sqls,
                'validation_result': validation_result,
                'table_stats': table_stats
            }
            
        except Exception as service_error:
            # 捕获 Service 层的未预期异常
            actual_layer = processing_flow if (layer_type in ["DWS", "ADS"] and processing_flow) else layer_type
            error_msg = f"{actual_layer} 层处理异常: {str(service_error)}"
            logger.error(f"[流程异常] {error_msg}", exc_info=True)
            
            # 记录失败日志
            self.exception_service.log_service_failure(
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
                "input_path": None  # 由调用方填充
            }
    
    def _commit_and_report(self, file_result: Dict[str, Any], table_stats: Dict[str, int], 
                          input_path: Union[str, Path], is_directory: bool) -> Dict[str, Any]:
        """
        提交事务并输出统计报告
        
        Args:
            file_result: 文件处理结果
            table_stats: 表写入统计
            input_path: 输入路径
            is_directory: 是否为目录
            
        Returns:
            提交结果
        """
        try:
            self.session.commit()
            logger.info("[流程结束] ✅ 主流程已提交事务")
            
            # 标记失败日志为已解决
            resolved_count = self.exception_service.mark_failures_as_resolved(file_result.get('results', []))
            if resolved_count > 0:
                logger.info(f"[流程结束] ✅ 已标记 {resolved_count} 条失败记录为已解决")
            
            # 输出详细的表写入统计
            file_paths_count = len(file_result.get('results', [])) if file_result.get('success') else 0
            file_name = Path(str(input_path)).name if not is_directory else f"{file_paths_count}个文件"
            
            logger.info(f"\n{'='*80}")
            logger.info(f"✅ 流程完成 | {file_name}")
            logger.info(f"{'='*80}")
            
            if table_stats:
                logger.info(f"💾 表写入统计（共 {len(table_stats)} 张表）:")
                for table_name, count in sorted(table_stats.items()):
                    status = "✅" if count > 0 else "⚠️ 无数据"
                    logger.info(f"   {status} {table_name:<35s}: {count:>4d} 条")
            else:
                logger.warning("⚠️ 未获取到表写入统计信息")
            
            logger.info(f"{'='*80}\n")
            return {'success': True}
                
        except Exception as commit_error:
            logger.error(f"[流程结束] ❌ 提交事务失败: {str(commit_error)}")
            self.session.rollback()
            return {
                "success": False,
                "message": f"事务提交失败：{str(commit_error)}",
                "input_path": str(input_path)
            }
        

    def process_sql_files(self, input_path: Union[str, Path]) -> Dict[str, Any]:
        """
        处理 SQL 文件（完整流程：读取文件 -> 解析 -> 规则处理 -> 生成SQL -> 写入表）
        
        自动判断输入是文件还是目录，逐个文件处理，返回统一格式的结果

        Args:
            input_path: 输入路径（文件路径或目录路径）

        Returns:
            统一格式的处理结果字典
        """
        try:
            # 0. 初始化会话
            self._initialize_session()
            
            # 1. 验证输入并检测模式
            validation_result = self._validate_and_detect_mode(input_path)
            if not validation_result['success']:
                return validation_result
            
            is_directory = validation_result['is_directory']
            layer_type = validation_result['layer_type']
            processing_flow = validation_result.get('processing_flow')
            
            # 2. 获取文件列表
            path = Path(input_path)
            if is_directory:
                file_paths = list(path.glob("*.sql"))
                if not file_paths:
                    return {"success": False, "message": f"目录中没有找到SQL文件：{input_path}"}
            else:
                if path.suffix.lower() != '.sql':
                    return {"success": False, "message": f"文件必须是.sql格式：{input_path}"}
                file_paths = [path]
            
            total_files = len(file_paths)
            logger.info(f"[流程开始] 共找到 {total_files} 个 SQL 文件，将逐个处理")
            
            # 3. 逐个文件处理
            all_results = []
            processed_count = 0
            failed_count = 0
            
            for idx, file_path in enumerate(file_paths, 1):
                logger.info(f"\n{'='*80}")
                logger.info(f"📄 处理文件 [{idx}/{total_files}]: {file_path.name}")
                logger.info(f"{'='*80}")
                
                try:
                    # 3.1 读取单个文件
                    with open(file_path, 'r', encoding='utf-8') as f:
                        sql_content = f.read()
                    
                    file_data = {
                        'file_path': str(file_path),
                        'sql_content': sql_content,
                        'success': True,
                        'message': '文件读取成功'
                    }
                    
                    # 3.2 预检查 SQL 质量
                    pre_check_result = self._pre_check_sql_quality(
                        {'results': [file_data]}, 
                        layer_type, 
                        file_path
                    )
                    if not pre_check_result['success']:
                        # 预检查失败的详细日志已在 _pre_check_sql_quality() 中打印
                        file_data['success'] = False
                        file_data['message'] = pre_check_result['message']
                        all_results.append(file_data)
                        failed_count += 1
                        continue
                    
                    # 3.3 解析 SQL
                    ai_layer_type = processing_flow if layer_type in ["DWS", "ADS"] else layer_type
                    parsed_result = self._parse_single_sql_file(sql_content, str(file_path), ai_layer_type)
                    
                    if not parsed_result['success']:
                        file_data['success'] = False
                        file_data['message'] = parsed_result['message']
                        all_results.append(file_data)
                        failed_count += 1
                        continue
                    
                    parsed_results = [parsed_result]
                    # 解析成功的日志已在 _parse_sql_with_ai() 中打印
                    
                    # 3.4 委派给 Service 处理
                    service_result = self._dispatch_to_service(parsed_results, layer_type, processing_flow)
                    
                    if not service_result['success']:
                        # Service 处理失败的详细日志已在 _dispatch_to_service() 中打印
                        file_data['success'] = False
                        file_data['message'] = service_result['message']
                        all_results.append(file_data)
                        failed_count += 1
                        
                        # 回滚当前文件的事务
                        if self.session.in_transaction():
                            self.session.rollback()
                            logger.debug(f"[文件处理] 已回滚 {file_path.name} 的事务")
                        continue
                    
                    # 3.5 提交当前文件的事务
                    table_stats = service_result['table_stats']
                    commit_result = self._commit_single_file(file_data, table_stats, file_path)
                    
                    if not commit_result['success']:
                        # 提交失败的详细日志已在 _commit_single_file() 中打印
                        file_data['success'] = False
                        file_data['message'] = commit_result['message']
                        all_results.append(file_data)
                        failed_count += 1
                        continue
                    
                    # 3.6 记录成功
                    file_data['success'] = True
                    file_data['message'] = '处理成功'
                    all_results.append(file_data)
                    processed_count += 1
                    # 处理成功的统计信息已在 _commit_single_file() 中打印
                    
                except Exception as e:
                    # 未预期异常，打印详细堆栈信息
                    logger.error(f"[文件处理] ❌ {file_path.name} 发生未预期异常", exc_info=True)
                    all_results.append({
                        'file_path': str(file_path),
                        'success': False,
                        'message': f'处理异常：{str(e)}'
                    })
                    failed_count += 1
                    
                    # 确保回滚
                    if self.session.in_transaction():
                        self.session.rollback()
                        logger.debug(f"[文件处理] 异常回滚 {file_path.name} 的事务")
            
            # 4. 汇总结果
            file_result = {
                'success': True,
                'total_files': total_files,
                'processed_files': processed_count,
                'failed_files': failed_count,
                'results': all_results,
                'message': f'成功处理 {processed_count}/{total_files} 个文件'
            }
            
            logger.info(f"\n{'='*80}")
            logger.info(f"📊 处理总结")
            logger.info(f"{'='*80}")
            logger.info(f"   - 总文件数: {total_files}")
            logger.info(f"   - 成功: {processed_count}")
            logger.info(f"   - 失败: {failed_count}")
            logger.info(f"{'='*80}\n")
            
            return {
                "success": True,
                "file_result": file_result,
                "parsed_results": [r for r in all_results if r.get('success')],
                "message": f"指标平台自动化建设流程完成，成功 {processed_count}/{total_files} 个文件"
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
        
        finally:
            # 最终清理
            self._cleanup_session()

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
    
    def _parse_single_sql_file(self, sql_content: str, file_path: str, layer_type: str) -> Dict[str, Any]:
        """
        解析单个 SQL 文件
        
        Args:
            sql_content: SQL 内容
            file_path: 文件路径
            layer_type: 层级类型
            
        Returns:
            解析结果
        """
        try:
            parsed_data = self._parse_sql_with_ai(sql_content, file_path, layer_type)
            
            if not parsed_data or not parsed_data.get('success', False):
                return {
                    'file_path': file_path,
                    'success': False,
                    'message': parsed_data.get('message', '大模型解析失败') if parsed_data else '大模型解析失败',
                    'parsed_data': None
                }
            
            return {
                'file_path': file_path,
                'success': True,
                'message': 'SQL解析成功',
                'parsed_data': parsed_data.get('parsed_data', {})
            }
            
        except Exception as e:
            logger.error(f"解析文件 {file_path} 失败：{str(e)}")
            return {
                'file_path': file_path,
                'success': False,
                'message': f'解析失败：{str(e)}',
                'parsed_data': None
            }
    
    def _commit_single_file(self, file_data: Dict[str, Any], table_stats: Dict[str, int], file_path: Path) -> Dict[str, Any]:
        """
        提交单个文件的事务并输出统计
        
        Args:
            file_data: 文件数据
            table_stats: 表写入统计
            file_path: 文件路径
            
        Returns:
            提交结果
        """
        try:
            self.session.commit()
            logger.debug(f"[文件提交] ✅ {file_path.name} 已提交事务")
            
            # 标记失败日志为已解决
            resolved_count = self.exception_service.mark_failures_as_resolved([file_data])
            if resolved_count > 0:
                logger.debug(f"[文件提交] ✅ 已标记 {resolved_count} 条失败记录为已解决")
            
            # 输出表写入统计
            if table_stats:
                logger.info(f"💾 {file_path.name} 表写入统计（共 {len(table_stats)} 张表）:")
                for table_name, count in sorted(table_stats.items()):
                    status = "✅" if count > 0 else "⚠️ 无数据"
                    logger.info(f"   {status} {table_name:<35s}: {count:>4d} 条")
            
            return {'success': True}
                
        except Exception as commit_error:
            logger.error(f"[文件提交] ❌ {file_path.name} 提交事务失败: {str(commit_error)}")
            self.session.rollback()
            return {
                "success": False,
                "message": f"事务提交失败：{str(commit_error)}",
                "input_path": str(file_path)
            }

    def _auto_detect_layer_type(self, input_path: Union[str, Path]) -> str:
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
            
            # ⚠️ 详细记录 AI 返回结果
            if not result or (isinstance(result, str) and not result.strip()):
                logger.error(f"[AI解析] ❌ AI 返回空内容")
                logger.error(f"[AI解析] 文件: {sql_file}")
                logger.error(f"[AI解析] SQL 长度: {len(sql_content)} 字符")
                logger.error(f"[AI解析] SQL 前100字符: {sql_content[:100]}")
                error_msg = "大模型返回空内容"
                # ⚠️ 使用 ExceptionService 处理错误
                return self.exception_service.handle_parse_error(
                    sql_file=sql_file,
                    error_msg=error_msg,
                    layer_type=layer_type,
                    error_type="AI_EMPTY_RESPONSE",
                    sql_content=sql_content
                )
            else:
                logger.debug(f"[AI解析] ✅ AI 返回内容长度: {len(result)} 字符")
                logger.debug(f"[AI解析] AI 返回前200字符: {result[:200]}")

            # 检查返回内容是否为有效的 JSON（先打印日志）
            logger.debug(f"[AI解析] AI 返回内容长度: {len(result)} 字符")
            logger.debug(f"[AI解析] AI 返回内容前300字符: {result[:300]}")

            # 解析大模型返回的JSON
            try:
                parsed_json = json.loads(result)
                logger.debug(f"[AI解析] ✅ JSON 解析成功")
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
    

