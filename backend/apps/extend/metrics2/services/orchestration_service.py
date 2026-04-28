from typing import List, Dict, Any, Optional, Union
from pathlib import Path
import logging
from sqlalchemy.orm import Session
from sqlalchemy import text
from apps.extend.metrics2.services.dim_service import DimService
from apps.extend.metrics2.services.lineage_service import LineageService
from apps.extend.metrics2.services.metrics_service import MetricsService
from apps.extend.metrics2.services.exception_service import ExceptionService
from apps.extend.metrics2.services.data_processor import DataProcessor  # ⚠️ 新增
from apps.extend.metrics2.services.check_service import CheckService  # ⚠️ 校验服务
from apps.extend.metrics2.services.metadata_service import metadataService  # ⚠️ 用于 table_metadata 插入
from apps.extend.metrics2.utils.sql_generator import SqlGenerator  # ⚠️ 用于批量插入

# ⚠️ 统一配置日志（模块加载时执行）
logging.basicConfig(
    level=logging.INFO,  # 基础级别设为 INFO，会输出 INFO、WARNING、ERROR
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True  # 强制覆盖已有的配置
)

# 配置日志
logger = logging.getLogger("orchestrationService")

class orchestrationService:
    """养殖业务指标平台总服务类 - 负责整个自动化建设链路的编排"""

    def __init__(self, session: Session):
        """
        初始化指标平台服务

        Args:
            session: 数据库会话
        """
        self.session = session
        
        # ⚠️ 先创建异常服务和校验服务（其他 Service 需要依赖）
        self.exception_service = ExceptionService(session)
        
        # ⚠️ 新增：创建脚本处理器
        self.data_processor = DataProcessor(session, self.exception_service)
        
        # ⚠️ 新增：创建校验服务（需要传入 data_processor）
        self.check_service = CheckService(session, self.exception_service, data_processor=self.data_processor)
        
        # ⚠️ 先创建 LineageService（共享实例）
        self.lineage_service = LineageService(session, check_service=self.check_service)  # ⚠️ 注入 CheckService
        
        # ⚠️ 将 LineageService 实例传递给 MetricsService，实现缓存共享
        self.dim_service = DimService(session, check_service=self.check_service)  # ⚠️ 注入 CheckService
        self.metrics_service = MetricsService(session, lineage_service=self.lineage_service, check_service=self.check_service)  # ⚠️ 注入 CheckService
        
        # ⚠️ 新增：创建元数据服务（用于 table_metadata 插入）
        self.table_metadata_service = metadataService(session)
        
        logger.info("[MetricsPlatformService] 初始化完成 - ScriptProcessor & CheckService & MetadataService 已就绪")

    def _dispatch_to_service(
        self, 
        parsed_results_list: List[Dict[str, Any]], 
        layer_type: str, 
        processing_flow: Optional[str],
        dim_cache: Dict[str, str] = None,
        metric_cache: Dict[str, str] = None
    ) -> Dict[str, Any]:
        """
        根据层级类型委派给对应的 Service 处理
        
        Args:
            parsed_results_list: 单个文件的解析结果（字典）
            layer_type: 层级类型
            processing_flow: 处理流程（仅 DWS/ADS 层）
            dim_cache: 维度缓存 {code: dim_id}
            metric_cache: 指标缓存 {code: metric_id}
            
        Returns:
            {
                'success': bool,
                'table_data': dict,
                'processed_results': list
            }
            
        Raises:
            Exception: Service 层的异常会直接传播到主流程，由主流程统一处理
        """
        if layer_type == "DIM":
            logger.info("[流程] DIM 层：使用 DimService 处理")
            service_result = self.dim_service.process(
                parsed_results_list, 
                layer_type=layer_type,
                dim_cache=dim_cache  # ⚠️ 传递缓存
            )
            
        elif layer_type == "DWD":
            logger.info("[流程] DWD 层：使用 LineageService 处理")
            service_result = self.lineage_service.process(parsed_results_list, layer_type=layer_type)
            
        elif layer_type in ["DWS", "ADS"]:
            if processing_flow == "WIDE":
                logger.info(f"[{layer_type} 层] 无 GROUP BY，使用 LineageService 处理")
                service_result = self.lineage_service.process(parsed_results_list, layer_type=layer_type)
            else:  # processing_flow == "METRIC"
                logger.info(f"[{layer_type} 层] 有 GROUP BY，使用 MetricsService 处理")
                service_result = self.metrics_service.process(
                    parsed_results_list, 
                    layer_type=layer_type,
                    metric_cache=metric_cache  # ⚠️ 传递缓存
                )
        else:
            error_msg = f"不支持的层级类型: {layer_type}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        return {
            'success': True,
            'table_data': service_result.get('table_data', {}),  # ⚠️ 保留 Service 返回的表数据
            'processed_results': service_result.get('processed_results', [])  # ⚠️ 保留原始结果
        }

    def process(self, input_path: Union[str, Path]) -> Dict[str, Any]:
        """
        处理 SQL 文件（完整流程：读取文件 -> 解析 -> 规则处理 -> 生成SQL -> 写入表）
        
        自动判断输入是文件还是目录，逐个文件处理，返回统一格式的结果

        Args:
            input_path: 输入路径（文件路径或目录路径）

        Returns:
            统一格式的处理结果字典
        """
        # 1. 验证输入并检测模式
        validation_result = self.check_service.validate_and_detect_mode(input_path)
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

        total_count = len(file_paths)
        logger.info(f"[流程开始] 共找到 {total_count} 个 SQL 文件，将分批处理（batch_size=10）")

        # 3. 分批处理文件
        all_results = []
        batch_size = 10  # ⚠️ 批次大小
        current_batch = []  # ⚠️ 当前批次的成功结果
        current_process_files = 0
        
        # ⚠️ 全局缓存：保证同一批次中相同 code 使用相同的 ID（按类型拆分）
        dim_cache = {}        # {code: dim_id}
        metric_cache = {}     # {code: metric_id}
        failure_logs = []

        # ⚠️ 断点续传：获取已处理的文件路径
        processed_file_paths = set()
        if is_directory:
            try:
                processed_file_paths = self.data_processor.get_processed_file_paths(
                    session=self.session,
                    file_paths=[str(fp) for fp in file_paths],
                    layer_type=layer_type
                )
                if processed_file_paths:
                    logger.info(f"[断点续传] ✅ 检测到 {len(processed_file_paths)} 个已处理文件（{layer_type} 层），将自动跳过")
            except Exception as e:
                logger.warning(f"[断点续传] ⚠️ 查询已处理文件失败: {str(e)}，将处理所有文件")

        for idx, file_path in enumerate(file_paths, 1):
            # ⚠️ 断点续传：跳过已处理的文件
            if str(file_path) in processed_file_paths:
                logger.debug(f"[断点续传] ⏭️  跳过已处理文件 [{idx}/{total_count}]: {file_path.name}")
                continue
            
            current_process_files += 1
            logger.info(f"\n{'='*80}")
            logger.info(f"📄 处理文件 [{idx}/{total_count}]: {file_path.name}")
            logger.info(f"{'='*80}")

            # 3.1 读取单个文件
            file_data = self.data_processor.read_sql_file(file_path)
            if not file_data['success']:
                all_results.append(file_data)
                continue

            # 3.2 预检查 SQL 质量
            pre_check_result = self.check_service.pre_check_sql_quality(file_data, layer_type)
            if not pre_check_result['success']:
                file_data['success'] = False
                file_data['message'] = pre_check_result['message']
                failure_logs.append(pre_check_result['failure_log']) if 'failure_log' in parsed_result else None
                all_results.append(file_data)
                continue

            # 3.3 解析 SQL
            ai_layer_type = processing_flow if layer_type in ["DWS", "ADS"] else layer_type
            parsed_result = self.data_processor.parse_sql_with_ai(
                file_data['sql_content'],
                str(file_path),
                ai_layer_type
            )

            if not parsed_result['success']:
                file_data['success'] = False
                file_data['message'] = parsed_result['message']
                failure_logs.append(parsed_result['failure_log']) if 'failure_log' in parsed_result else None
                all_results.append(file_data)
                continue

            # 3.4 委派给 Service 处理（如果 Service 失败会抛出异常，由下方的 except 捕获）
            try:
                parsed_results_list = [parsed_result]

                # ⚠️ 统一调用 _dispatch_to_service，传递缓存
                service_result = self._dispatch_to_service(
                    parsed_results_list,
                    layer_type,
                    processing_flow,
                    dim_cache=dim_cache,      # ⚠️ 传递 dim_cache
                    metric_cache=metric_cache  # ⚠️ 传递 metric_cache
                )

                # 3.5 加入批次缓存（不立即提交）
                current_batch.append({
                    'file_path': file_path,  # ⚠️ 直接存储文件路径字符串
                    'layer_type': layer_type,  # ⚠️ 保存层级类型
                    'service_result': service_result  # ⚠️ 保存 service_result，包含 table_data 等所有数据
                })

                logger.debug(f"[批次处理] 文件 {file_path.name} 已加入批次缓存，当前批次大小: {len(current_batch)}")
            except Exception as service_error:
                # ⚠️ Service 层异常，记录失败日志并继续处理下一个文件
                error_msg = str(service_error)

                # 记录失败日志
                failure_logs.append({
                    'file_path': str(file_path),
                    'failure_reason': error_msg,
                    'layer_type': layer_type,
                    'error_type': 'SERVICE_ERROR'
                })

                # 添加到结果列表
                all_results.append({
                    'file_path': str(file_path),
                    'success': False,
                    'message': error_msg
                })
                continue

            # 3.6 检查是否需要提交批次
            should_commit = (current_process_files >= batch_size) or (idx == total_count)

            if should_commit and current_batch:
                logger.info(f"[批次处理] 文件处理完毕，开始提交批次")

                # ⚠️ 显式开启事务，包裹所有数据库操作
                try:
                    # ⚠️ 确保 Session 状态干净（避免多批次事务冲突）
                    if self.session.in_transaction():
                        logger.warning("[批次提交] ⚠️ 检测到活跃事务，尝试回滚")
                        try:
                            self.session.rollback()
                        except Exception:
                            pass

                    with self.session.begin():
                        # 2. 执行批次 SQL
                        self.exe_sql(current_batch)

                        # 3. 成功 → 自动提交（包括失败日志 + 批次数据）
                        success_logs = []
                        for item in current_batch:
                            all_results.append(
                                {
                                    'file_path': item['file_path'],
                                    'success': True,
                                    'message': '批次写入成功'
                                }
                            )

                            success_logs.append({
                                'file_path': str(item['file_path']),  # ⚠️ 使用 item['file_path']
                                'file_name': Path(item['file_path']).name,  # ⚠️ 使用 item['file_path']
                                'layer_type': item.get('layer_type', 'METRIC'),
                                'target_table': None
                            })


                        self.exception_service.save_success_logs(success_logs)
                        logger.info(f"[批次提交] ✅ 成功处理 {len(current_batch)} 个文件")

                        self.exception_service.save_failure_log(failure_logs)

                        # ⚠️ 清空缓存（下一批次重新构建）
                        current_process_files = 0
                        failure_logs.clear()
                        current_batch.clear()
                        dim_cache.clear()
                        metric_cache.clear()

                except Exception as commit_error:
                    # ⚠️ 4. 事务已自动回滚，记录错误并启动二分法
                    error_msg = str(commit_error)
                    logger.error(f"[批次提交] ❌ 提交失败，事务已自动回滚: {error_msg}")

                    # ⚠️ 5. 直接记录失败
                    for item in current_batch:
                        file_path = item['file_path']  # ⚠️ 直接使用 item['file_path']，不要覆盖 item

                        failure_logs.append({
                            'file_path': str(file_path),
                            'file_name': Path(file_path).name,
                            'failure_reason': f'批次提交失败：{error_msg}',
                            'layer_type': item.get('layer_type', 'METRIC'),
                            'error_type': 'BATCH_COMMIT_FAILED'
                        })

                    # ⚠️ 批量保存失败日志（循环外）
                    self.exception_service.save_failure_log(failure_logs)

                    # ⚠️ 添加到结果列表
                    for item in current_batch:
                        file_path = item['file_path']
                        all_results.append({
                            'file_path': file_path,
                            'success': False,
                            'message': error_msg
                        })

                    # 二分法
                    # self.SplitProcess(all_results, current_batch)

                    # ⚠️ 清空缓存（下一批次重新构建）
                    current_process_files = 0
                    failure_logs.clear()
                    current_batch.clear()
                    dim_cache.clear()
                    metric_cache.clear()

        # 4. 汇总结果 - 从 all_results 中统计成功和失败数量
        success_count = 0
        failed_count = 0
        success_files = []
        failed_files = []
        for r in all_results:
            if r.get('success'):
                success_count += 1
                success_files.append(Path(r['file_path']).name)
            if not r.get('success'):
                failed_count += 1
                failed_files.append(Path(r['file_path']).name)

        file_result = {
            'success': True,
            'total_count': total_count,
            'success_count': success_count,
            'success_files': success_files,
            'failed_count': failed_count,
            'failed_files': failed_files,
            'results': all_results,
            'message': f'成功处理 {success_count}/{total_count} 个文件'
        }

        logger.info(f"📊 处理结果详情如下：\n{file_result}")

        return file_result

    def SplitProcess(self, all_results, current_batch):
        # ⚠️ 6. 多文件批次，使用二分法定位故障
        logger.info(f"尝试使用二分法处理批次文件（{len(current_batch)} 个）")
        success_files, failed_files = self.fallbackSplitProcess(current_batch)
        # ⚠️ 7. 开启事务，记录成功和失败日志
        try:
            # 7.1 标记失败日志为已解决
            if success_files:
                resolved_count = self.exception_service.mark_failures_as_resolved(success_files)
                if resolved_count > 0:
                    logger.info(f"[二分法] ✅ 已标记 {resolved_count} 条成功文件失败记录为已解决")

                # 7.2 在 sql_parse_success_log 表中插入成功记录（不 commit）
                success_log_count = self.exception_service.save_success_logs(success_files)
                if success_log_count > 0:
                    logger.info(f"[二分法] ✅ 已插入 {success_log_count} 条成功日志记录")

                # 更新结果列表
                for success_log in success_files:
                    all_results.append({
                        'file_path': success_log['file_path'],
                        'success': True,
                        'message': '二分法写入成功'
                    })

                logger.info(f"[二分法] ✅ 成功处理 {len(success_files)} 个文件")

            # 8. 记录失败文件的日志（不 commit）
            if failed_files:
                recorded_count = self.exception_service.save_failure_log(failed_files)
                if recorded_count > 0:
                    logger.info(f"[二分法] ✅ 已记录 {recorded_count} 条失败日志")

                # 将失败文件添加到结果列表
                for failure_log in failed_files:
                    all_results.append({
                        'file_path': failure_log['file_path'],
                        'success': False,
                        'message': failure_log['failure_reason']
                    })

                logger.warning(f"[二分法] ❌ 失败 {len(failed_files)} 个文件")
        except Exception as log_error:
            logger.error(f"[二分法] ❌ 记录日志失败: {str(log_error)}")

    def exe_sql(self, current_batch):
        logger.info(f"💾 提交批次（{len(current_batch)} 个文件）")

        # ⚠️ 1. 合并所有文件的表数据
        merged_data = self._merge_batch_table_data(current_batch)

        # ⚠️ 2. 直接执行批量插入（Service 层已经校验过数据完整性）
        insert_sqls = self._generate_batch_insert_sqls(merged_data)
        logger.info(f"[批次提交] 开始执行 {len(insert_sqls)} 条 SQL...")

        # ⚠️ 3. 执行所有 SQL（任何一条失败都会自动 rollback）
        for sql_idx, sql in enumerate(insert_sqls, 1):
            result = self.session.execute(text(sql))
            logger.debug(f"[批次提交] SQL {sql_idx}/{len(insert_sqls)} 执行成功")

        logger.info("[批次提交] ✅ 事务已提交（所有表写入成功且校验通过）")

    def fallbackSplitProcess(self, batch_items: List[Dict[str, Any]], is_recursive: bool = False) -> tuple:
        """
        使用二分法递归处理失败数据
        
        核心逻辑：
        1. 外部调用（is_recursive=False）：
           - len == 1：直接执行 exe_sql
           - len > 1：先拆分为两半，再分别递归处理
        2. 内部递归（is_recursive=True）：
           - len == 1：直接报错返回（外层已尝试过，无需重复执行）
           - len > 1：先整体执行 exe_sql，成功则返回，失败才继续拆分
        
        Args:
            batch_items: 当前批次
            is_recursive: 是否为内部递归调用（默认False=外部调用）
            
        Returns:
            (success_files, failed_files) 元组
            - success_files: [{'file_path': '...', ...}] 统一成功日志格式
            - failed_files: [{'file_path': '...', 'failure_reason': '...', ...}] 统一失败日志格式
        """
        if not batch_items:
            logger.warning("[二分法] ⚠️ 批次为空，跳过处理")
            return [], []
        
        # ⚠️ 内部递归 + 单文件：直接报错返回（外层已尝试过）
        if is_recursive and len(batch_items) == 1:
            item = batch_items[0]
            file_path = item['file_path']
            file_name = Path(file_path).name
            logger.warning(f"  ❌ 文件 {file_name} 在内部递归中仍为单文件，判定为最终失败")
            
            failed_log = {
                'file_path': str(file_path),
                'file_name': file_name,
                'failure_reason': f'二分法验证失败：文件在多次拆分后仍然失败',
                'layer_type': item.get('layer_type', 'METRIC'),
                'error_type': 'BINARY_SEARCH_FAILED'
            }
            return [], [failed_log]
        
        # ⚠️ 外部调用 + 单文件：直接执行
        if not is_recursive and len(batch_items) == 1:
            item = batch_items[0]
            file_path = item['file_path']
            file_name = Path(file_path).name
            logger.info(f"  🔍 验证单个文件: {file_name}")
            
            try:
                self.exe_sql(batch_items)
                logger.info(f"  ✅ 文件 {file_name} 验证成功")
                success_log = {
                    'file_path': str(file_path),
                    'file_name': file_name,
                    'layer_type': item.get('layer_type', 'METRIC'),
                    'target_table': None
                }
                return [success_log], []
            except Exception as e:
                logger.warning(f"  ❌ 文件 {file_name} 验证失败: {str(e)}")
                failed_log = {
                    'file_path': str(file_path),
                    'file_name': file_name,
                    'failure_reason': f'二分法验证失败：SQL执行异常 - {str(e)}',
                    'layer_type': item.get('layer_type', 'METRIC'),
                    'error_type': 'BINARY_SEARCH_FAILED'
                }
                return [], [failed_log]
        
        # ⚠️ 多文件情况：根据调用类型决定处理方式
        if is_recursive:
            # 内部递归 + 多文件：先尝试整体执行，成功则返回，失败才拆分
            first_names = [Path(item['file_path']).name for item in batch_items]
            logger.info(f"  📊 尝试验证 {len(batch_items)} 个文件: {', '.join(first_names[:2])}{'...' if len(first_names) > 2 else ''}")
            
            try:
                self.exe_sql(batch_items)
                logger.info(f"  ✅ {len(batch_items)} 个文件整体验证成功")
                # 整体成功，转换为成功日志格式
                success_logs = []
                for item in batch_items:
                    file_path = item['file_path']
                    success_logs.append({
                        'file_path': str(file_path),
                        'file_name': Path(file_path).name,
                        'layer_type': item.get('layer_type', 'METRIC'),
                        'target_table': None
                    })
                return success_logs, []
            except Exception as e:
                logger.warning(f"  ❌ {len(batch_items)} 个文件整体验证失败，开始拆分: {str(e)}")
        
        # ⚠️ 外部调用（多文件）或 内部递归但整体执行失败：拆分为两半
        mid = len(batch_items) // 2
        first_half = batch_items[:mid]
        second_half = batch_items[mid:]
        
        first_names = [Path(item['file_path']).name for item in first_half]
        second_names = [Path(item['file_path']).name for item in second_half]
        
        logger.info(f"  📊 分割批次: {len(batch_items)} → {len(first_half)} + {len(second_half)}")
        logger.info(f"     前半部分: {', '.join(first_names[:2])}{'...' if len(first_names) > 2 else ''}")
        logger.info(f"     后半部分: {', '.join(second_names[:2])}{'...' if len(second_names) > 2 else ''}")
        
        # ⚠️ 串行处理前半部分（传入 is_recursive=True，表示内部递归）
        first_success, first_failed = self.fallbackSplitProcess(first_half, is_recursive=True)
        
        # ⚠️ 串行处理后半部分（传入 is_recursive=True，表示内部递归）
        second_success, second_failed = self.fallbackSplitProcess(second_half, is_recursive=True)
        
        # ⚠️ 合并结果
        all_success = first_success + second_success
        all_failed = first_failed + second_failed
        
        return all_success, all_failed
        
    def _merge_batch_table_data(self, current_batch: List[Dict]) -> Dict[str, List]:
        """
        合并批次中所有文件的表数据（含去重）
        
        Args:
            current_batch: 批次中的所有文件项
            
        Returns:
            合并后的表数据 {table_name: [records]}，已根据唯一键去重
        """
        merged_data = {}
        
        # ⚠️ 定义各表的唯一键字段
        unique_keys = {
            'dim_definition': 'code',              # dim_definition.code 唯一
            'dim_field_lineage': ('db_table', 'field'),  # 复合唯一键
            'metric_definition': 'code',           # metric_definition.code 唯一
            'metric_source_mapping': ('metric_id', 'db_table', 'metric_column'),  # 复合唯一键
            'metric_dim_rel': ('metric_id', 'dim_id'),  # 复合唯一键
            'metric_compound_rel': ('metric_id', 'sub_metric_id'),  # 复合唯一键
            'table_lineage': ('source_table', 'target_table'),  # 复合唯一键
            'field_lineage': ('source_table', 'source_field', 'target_table', 'target_field'),  # 复合唯一键
            'metric_lineage': ('metric_id', 'field_lineage_id'),  # 复合唯一键
            'table_metadata': 'table_name',        # table_metadata.table_name 唯一
        }
        
        for item in current_batch:
            service_result = item.get('service_result', {})
            table_data = service_result.get('table_data', {})
            
            for table_name, records in table_data.items():
                if table_name not in merged_data:
                    merged_data[table_name] = []
                
                # ⚠️ 获取该表的唯一键
                key_field = unique_keys.get(table_name)
                if not key_field:
                    # 如果没有定义唯一键，直接追加
                    merged_data[table_name].extend(records)
                    continue
                
                # ⚠️ 根据唯一键去重
                if isinstance(key_field, str):
                    # 单字段唯一键
                    existing_keys = {record.get(key_field) for record in merged_data[table_name]}
                    for record in records:
                        key_value = record.get(key_field)
                        if key_value and key_value not in existing_keys:
                            merged_data[table_name].append(record)
                            existing_keys.add(key_value)
                else:
                    # 复合唯一键（元组）
                    existing_keys = {
                        tuple(record.get(field) for field in key_field)
                        for record in merged_data[table_name]
                    }
                    for record in records:
                        key_value = tuple(record.get(field) for field in key_field)
                        if all(v is not None for v in key_value) and key_value not in existing_keys:
                            merged_data[table_name].append(record)
                            existing_keys.add(key_value)
        
        logger.info(f"[批量合并] 合并完成 - {len(merged_data)} 张表, 总记录数: {sum(len(v) for v in merged_data.values())}")
        return merged_data

    def _generate_batch_insert_sqls(self, merged_data: Dict[str, List]) -> List[str]:
        """
        生成批量插入sql
        
        Args:
            merged_data: 合并后的表数据
        """
        # 定义各表的配置（基于模型定义）
        table_configs = {
            # DIM 层
            'dim_definition': {
                'columns': ['id', 'name', 'code', 'type', 'is_valid', 'create_time', 'modify_time'],
                'conflict_target': '(code)'
            },
            'dim_field_lineage': {
                'columns': ['id', 'db_table', 'field', 'field_name', 'dim_id', 'create_time', 'modify_time'],
                'conflict_target': '(db_table, field)'
            },
            'table_metadata': {
                'columns': ['id', 'table_name', 'source_level', 'biz_domain', 'table_comment', 'file_path', 'create_time', 'modify_time'],
                'conflict_target': '(table_name)'
            },
            # METRIC 层
            'metric_definition': {
                'columns': ['id', 'name', 'code', 'metric_type', 'biz_domain', 'status', 'create_time', 'modify_time'],
                'conflict_target': '(code)'
            },
            'metric_dim_rel': {
                'columns': ['metric_id', 'dim_id', 'is_required', 'create_time', 'modify_time'],
                'conflict_target': '(metric_id, dim_id)'
            },
            'metric_source_mapping': {
                'columns': ['id', 'metric_id', 'source_type', 'datasource', 'db_table', 'metric_column', 'metric_name', 'filter_condition', 'agg_func', 'priority', 'is_valid', 'source_level', 'biz_domain', 'cal_logic', 'unit', 'embedding_vector', 'create_time', 'modify_time'],
                'conflict_target': '(metric_id, db_table, metric_column)'
            },
            'metric_compound_rel': {
                'columns': ['id', 'metric_id', 'sub_metric_id', 'cal_operator', 'sort', 'create_time', 'modify_time'],
                'conflict_target': '(metric_id, sub_metric_id)'
            },
            # LINEAGE 层
            'table_lineage': {
                'columns': ['id', 'source_table', 'target_table', 'create_time', 'modify_time'],
                'conflict_target': '(source_table, target_table)'
            },
            'field_lineage': {
                'columns': ['id', 'table_lineage_id', 'source_table', 'source_field', 'target_table', 'target_field', 'target_field_mark', 'dim_id', 'formula', 'create_time', 'modify_time'],
                'conflict_target': '(source_table, source_field, target_table, target_field)'
            },
            'metric_lineage': {
                'columns': ['metric_id', 'field_lineage_id', 'create_time', 'modify_time'],
                'conflict_target': '(metric_id, field_lineage_id)'
            }
        }

        insert_sqls = []
        for table_name, data_list in merged_data.items():
            if not data_list:
                continue
            
            config = table_configs.get(table_name)
            if not config:
                logger.warning(f"[批量插入] 未知的表名: {table_name}")
                continue
            
            sql = SqlGenerator.generate_batch_upsert(table_name, config, data_list)
            if sql:
                insert_sqls.append(sql)
        return insert_sqls


