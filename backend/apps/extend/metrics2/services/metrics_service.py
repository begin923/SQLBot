from typing import List, Dict, Any, Optional
import logging
from sqlalchemy.orm import Session
from sqlalchemy import text

# ⚠️ 导入工具类
from apps.extend.metrics2.utils.id_generator import IdGenerator
from apps.extend.metrics2.utils.batch_query import BatchQueryHelper
from apps.extend.metrics2.utils.sql_generator import SqlGenerator
from apps.extend.metrics2.utils.timezone_helper import get_now_utc8

logger = logging.getLogger("MetricsService")


class MetricsService:
    """指标服务 - 专门处理 METRIC 层的指标数据"""
    
    def __init__(self, session: Session, lineage_service=None):
        """
        初始化指标服务
        
        Args:
            session: 数据库会话
            lineage_service: 血缘服务实例（可选，用于复用缓存）
        """
        self.session = session
        # ⚠️ 使用 IdGenerator 替代手动计数器
        self.metric_id_gen = IdGenerator(session, 'metric_definition', 'M')  # metric_definition 的主键是 id
        self.map_id_gen = IdGenerator(session, 'metric_source_mapping', 'S')  # ⚠️ metric_source_mapping 的 ID 前缀已改为 S
        
        # ⚠️ 创建表元数据服务实例
        from apps.extend.metrics2.services.table_metadata_service import TableMetadataService
        self.table_metadata_service = TableMetadataService(session)
        
        # ⚠️ 通过依赖注入获取 LineageService，避免重复创建实例
        if lineage_service is not None:
            self.lineage_service = lineage_service
            logger.info("[指标服务] 使用外部传入的 LineageService 实例（共享缓存）")
        else:
            from .lineage_service import LineageService
            self.lineage_service = LineageService(session)
            logger.warning("[指标服务] 未传入 LineageService 实例，创建了新的实例（可能产生缓存隔离）")
    
    def process(self, processed_results: List[Dict[str, Any]], layer_type: str = "AUTO") -> Dict[str, Any]:
        """
        处理 METRIC 层指标数据
        
        Args:
            processed_results: 规则引擎处理后的结果列表
            layer_type: 数仓层级类型（DWS/ADS），用于统一推断 source_level
            
        Returns:
            处理结果，包含成功状态、消息和表统计信息
            
        Raises:
            Exception: 当数据处理失败时抛出异常，由主流程统一回滚
        """
        try:
            # 初始化表数据收集字典
            table_data = {
                'metric_definition': [],
                'metric_dim_rel': [],
                'metric_source_mapping': [],
                'metric_compound_rel': [],
                'table_lineage': [],
                'field_lineage': [],
                'metric_lineage': []
            }
            
            # 遍历所有处理结果，收集指标数据
            for processed_result in processed_results:                self._collect_metric_data(processed_result, table_data, layer_type)  # ⚠️ 传递 layer_type
            
            # ⚠️ 校验数据完整性（失败时抛出异常）
            validation_result = self._validate_data_integrity(table_data)
            if not validation_result['success']:
                raise ValueError(validation_result['message'])  # ⚠️ 抛出异常
            
            # 执行数据库插入（不提交事务，由主流程统一提交）
            execution_result = self._execute_insert(table_data)
            
            # ⚠️ 在所有主要数据写入成功后，再处理表元数据
            if processed_results:
                self.table_metadata_service.process_table_metadata(processed_results[0], "指标服务", layer_type)  # ⚠️ 传递 layer_type
            
            return {
                'success': True,
                'message': f"METRIC层处理成功，写入 {len(table_data['metric_definition'])} 个指标",
                'table_stats': execution_result.get('table_stats', {})
            }
            
        except ValueError:
            # ⚠️ 业务逻辑错误，直接向上抛出
            raise
        except Exception as e:
            logger.error(f"[指标服务] 处理失败: {str(e)}")
            raise  # ⚠️ 重新抛出异常
    
    def _validate_data_integrity(self, table_data: Dict[str, List]) -> Dict[str, Any]:
        """
        校验数据完整性，确保关键表必须有数据
        
        Args:
            table_data: 表数据收集字典
            
        Returns:
            校验结果：{'success': bool, 'message': str}
        """
        errors = []
        
        # 1. metric_definition - 必须有指标定义
        if not table_data['metric_definition']:
            errors.append("❌ metric_definition 表：未解析到任何指标数据，请检查 SQL 中是否包含聚合字段（SUM/COUNT/AVG等）")
        
        # 2. field_lineage - 必须有字段血缘
        if not table_data['field_lineage']:
            errors.append("❌ field_lineage 表：未解析到任何字段血缘，请检查 AI 是否正确解析了 SELECT 子句中的字段")
        
        # 3. table_lineage - 必须有表血缘
        if not table_data['table_lineage']:
            errors.append("❌ table_lineage 表：未解析到任何表血缘，请检查 SQL 中是否有 FROM/JOIN 子句")
        
        # 4. metric_source_mapping - 必须有指标源映射（至少一个指标有一个源映射）
        if not table_data['metric_source_mapping']:
            errors.append("❌ metric_source_mapping 表：未解析到任何指标源映射，请检查指标的 source_mappings 配置")
        
        # 5. metric_dim_rel - 可选（可能没有公共维度）
        if not table_data['metric_dim_rel']:
            logger.warning("⚠️ metric_dim_rel 表：未解析到任何指标-维度关联关系，可能原因：\n   - SQL 中没有 GROUP BY 字段\n   - GROUP BY 字段都是私有维度（private_dim），没有公共维度（public_dim）\n   - 公共维度在 dim_definition 表中不存在")
        
        # 6. metric_lineage - 可选（可能没有标记为 'metric' 的字段）
        if not table_data['metric_lineage']:
            logger.warning("⚠️ metric_lineage 表：未解析到任何指标-字段血缘关联，可能原因：\n   - 指标的 field_type 不是 'metric'\n   - field_lineage 中的 target_field_mark 不是 'metric'\n   - metric_en 与 field_name_en 不匹配")
        
        # 7. metric_compound_rel - 可选（复合指标才有）
        # 不强制要求，因为原子指标不需要这个表
        
        # 如果有错误，返回失败
        if errors:
            error_message = "数据完整性校验失败：\n\n" + "\n\n".join(errors)
            logger.error(f"[指标服务] {error_message}")
            return {
                'success': False,
                'message': error_message
            }
        
        # 校验通过
        logger.info(f"[指标服务] ✅ 数据完整性校验通过")
        return {
            'success': True,
            'message': '数据完整性校验通过'
        }
    
    def _collect_metric_data(self, processed_result: Dict[str, Any], table_data: Dict[str, List], layer_type: str = "AUTO"):
        """
        收集 METRIC 层指标数据（总控方法）
            
        Args:
            processed_result: 单个文件的处理结果
            table_data: 表数据收集字典
            layer_type: 数仓层级类型（DWS/ADS），用于统一推断 source_level
        """
        try:
            parsed_data = processed_result.get('parsed_data', {})
                
            logger.info(f"[指标服务] 开始处理 METRIC 层数据")
                
            # 1. 使用 LineageService 收集表级和字段级血缘
            self._collect_lineage_via_service(processed_result, table_data)
                
            # 2. 收集指标定义（独立，不依赖其他数据）
            self._collect_metric_definitions(parsed_data, table_data)
                
            # 3. 收集指标源映射（依赖 metric_definition 的数据，通过 code 匹配获取 metric_id）
            self._collect_all_source_mappings(parsed_data, table_data, layer_type)
                
            # 4. 收集复合指标关系
            self._collect_compound_relations(parsed_data, table_data)
                
            # ⚠️ 5. 收集指标-维度关联关系（metric_dim_rel）
            self._collect_metric_dim_rel(table_data)
                
            # 6. 关联指标和字段血缘
            self._collect_metric_field_lineage_rel(table_data)
                
            logger.info(f"[指标服务] 收集完成 - "
                       f"指标: {len(table_data['metric_definition'])}, "
                       f"源映射: {len(table_data['metric_source_mapping'])}, "
                       f"复合关系: {len(table_data['metric_compound_rel'])}, "
                       f"维度关联: {len(table_data['metric_dim_rel'])}, "
                       f"表血缘: {len(table_data['table_lineage'])}, "
                       f"字段血缘: {len(table_data['field_lineage'])}, "
                       f"指标血缘: {len(table_data['metric_lineage'])}")
                
        except Exception as e:
            logger.error(f"[指标服务] 收集数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _collect_lineage_via_service(self, processed_result: Dict[str, Any], table_data: Dict[str, List]):
        """
        通过 LineageService 收集表级和字段级血缘
        
        Args:
            processed_result: 单个文件的处理结果
            table_data: 表数据收集字典
        """
        # 创建一个临时的 table_data 用于 LineageService
        lineage_table_data = {
            'table_lineage': [],
            'field_lineage': []
        }
        
        # 调用 LineageService 的收集方法
        self.lineage_service.collect_lineage(processed_result, lineage_table_data)
        
        # 将收集到的血缘数据合并到主 table_data
        table_data['table_lineage'].extend(lineage_table_data['table_lineage'])
        table_data['field_lineage'].extend(lineage_table_data['field_lineage'])
        
        logger.debug(f"[指标服务] 从 LineageService 获取 - 表血缘: {len(lineage_table_data['table_lineage'])}, "
                    f"字段血缘: {len(lineage_table_data['field_lineage'])}")
    
    def _collect_metric_definitions(self, parsed_data: Dict[str, Any], table_data: Dict[str, List]):
        """
        收集指标定义（独立，不依赖其他数据）
            
        Args:
            parsed_data: AI 解析后的数据
            table_data: 表数据收集字典
        """
        ai_metric_definitions = parsed_data.get('metric_definition', [])
        ai_source_mappings = parsed_data.get('metric_source_mapping', [])
        
        # ⚠️ 去重 - 基于 metric_en (code)，使用字典自动去重
        unique_metrics_dict = {}
        duplicate_count = 0
        
        for metric_def in ai_metric_definitions:
            metric_en = metric_def.get('metric_en', '')
            if metric_en:
                if metric_en not in unique_metrics_dict:
                    unique_metrics_dict[metric_en] = metric_def
                else:
                    duplicate_count += 1
                    logger.warning(f"[指标服务] ⚠️ AI 输出中发现重复指标: {metric_en}")
        
        unique_metric_defs = list(unique_metrics_dict.values())
        
        if duplicate_count > 0:
            logger.info(f"[指标服务] ⚠️ AI 输出中去重: 原始 {len(ai_metric_definitions)} 条, 去重后 {len(unique_metric_defs)} 条, 跳过 {duplicate_count} 条重复")
        
        # ⚠️ 批量查询已存在的 metric_id（使用去重后的数据）
        existing_metrics = self._batch_get_metric_ids(unique_metric_defs)
        
        # ⚠️ 构建 metric_en 到 metric_id 的映射（包括新生成的）
        metric_en_to_id = {}
            
        for metric_def in unique_metric_defs:
            metric_name = metric_def.get('metric_name', '')
            metric_en = metric_def.get('metric_en', '')
            metric_type_raw = metric_def.get('metric_type', 'atomic')
            biz_domain = metric_def.get('biz_domain', '')
            status = metric_def.get('status', 1)
                
            # 转换 metric_type 为大写
            metric_type_map = {
                'atomic': 'ATOMIC',
                'derived': 'DERIVED',
                'composite': 'COMPOUND'
            }
            metric_type = metric_type_map.get(metric_type_raw.lower(), 'ATOMIC')
                
            # ⚠️ 从批量查询结果中获取 metric_id
            if metric_en in existing_metrics:
                metric_id = existing_metrics[metric_en]
            else:
                metric_id = self.metric_id_gen.get_next_id()  # ⚠️ 使用 IdGenerator
            
            # 记录映射关系
            metric_en_to_id[metric_en] = metric_id
                
            # 收集 metric_definition 数据（⚠️ 已删除 cal_logic 和 unit）
            table_data['metric_definition'].append({
                'id': metric_id,
                'name': metric_name,
                'code': metric_en,
                'metric_type': metric_type,
                'biz_domain': biz_domain,
                'status': status,
                'create_time': get_now_utc8(),
                'modify_time': get_now_utc8()
            })
    
    def _collect_all_source_mappings(self, parsed_data: Dict[str, Any], table_data: Dict[str, List], layer_type: str = "AUTO"):
        """
        批量收集所有指标的源映射（依赖 metric_definition 的数据）
        
        核心逻辑：
        1. 从 table_data['metric_definition'] 中获取已收集的指标定义（内存优先）
        2. 通过 metric_column 匹配 metric_definition.code，获取 metric_id
        3. 构建 metric_source_mapping 数据，打通业务-技术链路
            
        Args:
            parsed_data: AI 解析后的数据
            table_data: 表数据收集字典（包含已收集的 metric_definition）
            layer_type: 数仓层级类型（DWS/ADS），用于统一推断 source_level
        """
        ai_source_mappings = parsed_data.get('metric_source_mapping', [])
        
        if not ai_source_mappings:
            return
        
        # ⚠️ 第一步：从内存中获取已收集的 metric_definition，构建 code -> id 映射（优先使用当前批次数据）
        code_to_id = {}
        for metric_def in table_data['metric_definition']:
            code = metric_def.get('code', '')
            metric_id = metric_def.get('id', '')
            if code and metric_id:
                code_to_id[code] = metric_id
        
        if not code_to_id:
            logger.warning("[指标服务-源映射] ⚠️ 未找到任何指标定义，无法建立源映射")
            return
        
        logger.debug(f"[指标服务-源映射] 从内存中找到 {len(code_to_id)} 个指标定义")
        # ⚠️ 第二步：去重 - 基于 (metric_id, db_table, metric_column)，使用字典自动去重
        unique_mappings_dict = {}
        duplicate_count = 0
        
        for mapping in ai_source_mappings:
            metric_column = mapping.get('metric_column', '')
            if not metric_column or metric_column not in code_to_id:
                continue
            
            metric_id = code_to_id[metric_column]
            db_table = mapping.get('db_table', '')
            key = (metric_id, db_table, metric_column or '')
            
            if key not in unique_mappings_dict:
                unique_mappings_dict[key] = mapping
            else:
                duplicate_count += 1
                logger.warning(f"[指标服务] ⚠️ AI 输出中发现重复源映射: {key}")
        
        unique_mappings = list(unique_mappings_dict.values())
        
        if duplicate_count > 0:
            logger.info(f"[指标服务] ⚠️ 源映射去重: 原始 {len(ai_source_mappings)} 条, 去重后 {len(unique_mappings)} 条, 跳过 {duplicate_count} 条重复")
        
        if not unique_mappings:
            return
        
        # ⚠️ 第三步：按 metric_id 分组源映射（使用去重后的数据）
        mappings_by_metric = {}  # {metric_id: [mappings]}
        for mapping in unique_mappings:
            metric_column = mapping.get('metric_column', '')
            if not metric_column or metric_column not in code_to_id:
                continue
            
            metric_id = code_to_id[metric_column]
            if metric_id not in mappings_by_metric:
                mappings_by_metric[metric_id] = []
            mappings_by_metric[metric_id].append(mapping)
        
        if not mappings_by_metric:
            return
        
        # ⚠️ 第二步：批量查询所有已存在的 map_id
        all_existing_maps = {}
        try:
            # 提取所有 metric_id
            all_metric_ids = list(mappings_by_metric.keys())
            
            # 提取所有唯一的 (db_table, metric_column) 组合
            unique_keys = set()
            for mappings in mappings_by_metric.values():
                for mapping in mappings:
                    db_table = mapping.get('db_table', '')
                    metric_column = mapping.get('metric_column', '') or ''
                    unique_keys.add((db_table, metric_column))
            
            if unique_keys:
                db_tables = [key[0] for key in unique_keys]
                metric_columns = [key[1] for key in unique_keys]
                
                # 批量查询
                result = self.session.execute(
                    text("SELECT id, metric_id, db_table, COALESCE(metric_column, '') as metric_column FROM metric_source_mapping WHERE metric_id IN :metric_ids AND db_table IN :db_tables AND COALESCE(metric_column, '') IN :metric_columns"),
                    {"metric_ids": tuple(all_metric_ids), "db_tables": tuple(db_tables), "metric_columns": tuple(metric_columns)}
                ).fetchall()
                
                # 构建字典 {(metric_id, db_table, metric_column): map_id}
                for row in result:
                    key = (row[1], row[2], row[3])
                    all_existing_maps[key] = row[0]
                
                logger.debug(f"[指标服务-源映射] 批量查询到 {len(all_existing_maps)} 个已有源映射")
        except Exception as e:
            logger.warning(f"[指标服务-源映射] 批量查询失败: {str(e)}，将全部插入")
        
        # ⚠️ 第三步：处理每个源映射
        for metric_id, mappings in mappings_by_metric.items():
            for mapping in mappings:
                datasource = mapping.get('datasource', '')
                db_table = mapping.get('db_table', '')
                metric_column = mapping.get('metric_column', '')
                metric_name = mapping.get('metric_name', '')  # ⚠️ 新增：提取指标名称
                biz_domain = mapping.get('biz_domain', '')  # ⚠️ 新增：提取业务域
                filter_condition = mapping.get('filter_condition', '')
                agg_func = mapping.get('agg_func', '')
                priority = mapping.get('priority', 1)
                is_valid = mapping.get('is_valid', 1)
                source_type = mapping.get('source_type', 'OFFLINE').upper()
                    
                # ⚠️ 从批量查询结果中获取 map_id
                unique_key = (metric_id, db_table, metric_column or '')
                if unique_key in all_existing_maps:
                    map_id = all_existing_maps[unique_key]
                else:
                    map_id = self.map_id_gen.get_next_id()  # ⚠️ 使用 IdGenerator
                
                # ⚠️ 统一使用 MetricsPlatformService 推断的 layer_type 作为 source_level
                # 保证全局一致性，不依赖 AI 返回的 source_level
                inferred_source_level = layer_type.upper() if layer_type != "AUTO" else "AUTHORITY"
                    
                table_data['metric_source_mapping'].append({
                    'id': map_id,
                    'metric_id': metric_id,
                    'source_type': source_type,
                    'datasource': datasource,
                    'db_table': db_table,
                    'metric_column': metric_column,
                    'metric_name': metric_name,  # ⚠️ 新增：保存指标名称
                    'biz_domain': biz_domain,  # ⚠️ 新增：保存业务域
                    'filter_condition': filter_condition,
                    'agg_func': agg_func,
                    'priority': priority,
                    'is_valid': is_valid,
                    'source_level': inferred_source_level,
                    'cal_logic': mapping.get('cal_logic', ''),  # ⚠️ 从 AI 返回中提取
                    'unit': mapping.get('unit') or '',  # ⚠️ 从 AI 返回中提取
                    'create_time': get_now_utc8(),
                    'modify_time': get_now_utc8()
                })
        
    def _collect_compound_relations(self, parsed_data: Dict[str, Any], table_data: Dict[str, List]):
        """
        收集复合指标关系
        
        ⚠️ 重要：如果主指标或子指标在 metric_definition 中不存在，会自动创建默认记录
            
        Args:
            parsed_data: AI 解析后的数据
            table_data: 表数据收集字典
        """
        ai_compound_rels = parsed_data.get('metric_compound_rel', [])
        
        if not ai_compound_rels:
            return
        
        # ⚠️ 第一步：去重 - 基于 (main_metric_en, sub_metric_en)，使用字典自动去重
        unique_compound_dict = {}
        duplicate_count = 0
        
        for rel in ai_compound_rels:
            main_metric_field = rel.get('metric_en', '')
            sub_metric_fields = rel.get('sub_metric_fields', [])
            
            if not main_metric_field:
                logger.warning(f"[指标服务] 复合指标缺少 metric_en 字段，跳过")
                continue
            
            # 从 main_metric_field 中提取 metric_en
            main_metric_en = main_metric_field.split('.')[-1] if '.' in main_metric_field else main_metric_field
            
            # 为每个子指标生成唯一键
            for sub_field in sub_metric_fields:
                sub_metric_en = sub_field.split('.')[-1] if '.' in sub_field else sub_field
                key = (main_metric_en, sub_metric_en)
                
                if key not in unique_compound_dict:
                    unique_compound_dict[key] = rel
                else:
                    duplicate_count += 1
                    logger.warning(f"[指标服务] ⚠️ AI 输出中发现重复复合关系: {key}")
        
        unique_compound_rels = list(unique_compound_dict.values())
        
        if duplicate_count > 0:
            logger.info(f"[指标服务] ⚠️ 复合关系去重: 原始 {len(ai_compound_rels)} 条, 去重后 {len(unique_compound_rels)} 条, 跳过 {duplicate_count} 条重复")
        
        # ⚠️ 第二步：收集所有需要的 metric_en（主指标 + 子指标）
        all_metric_ens = set()
        compound_info = []  # 保存复合关系信息
        
        for rel in unique_compound_rels:
            metric_name = rel.get('metric_name', '')
            main_metric_field = rel.get('metric_en', '')
            sub_metric_fields = rel.get('sub_metric_fields', [])
            cal_operator = rel.get('cal_operator', '')
            sort = rel.get('sort', 1)
                
            if not main_metric_field:
                logger.warning(f"[指标服务] 复合指标缺少 metric_en 字段，跳过")
                continue
                
            # 从 main_metric_field 中提取 metric_en
            main_metric_en = main_metric_field.split('.')[-1] if '.' in main_metric_field else main_metric_field
            all_metric_ens.add(main_metric_en)
            
            # 提取所有子指标的 metric_en
            sub_metric_ens = []
            for sub_field in sub_metric_fields:
                sub_metric_en = sub_field.split('.')[-1] if '.' in sub_field else sub_field
                sub_metric_ens.append(sub_metric_en)
                all_metric_ens.add(sub_metric_en)
            
            compound_info.append({
                'metric_name': metric_name,
                'main_metric_en': main_metric_en,
                'sub_metric_ens': sub_metric_ens,
                'cal_operator': cal_operator,
                'sort': sort
            })
        
        if not all_metric_ens:
            return
        
        # ⚠️ 第二步：批量查询数据库中已存在的指标
        existing_metrics = self._batch_get_metric_ids_by_ens(list(all_metric_ens))
        
        # ⚠️ 第三步：处理每个复合关系
        for info in compound_info:
            main_metric_en = info['main_metric_en']
            metric_name = info['metric_name']
            sub_metric_ens = info['sub_metric_ens']
            cal_operator = info['cal_operator']
            sort = info['sort']
            
            # 获取或创建主指标 ID
            main_metric_id = self._get_or_create_metric_id_batch(
                main_metric_en, metric_name, existing_metrics, table_data
            )
            
            if not main_metric_id:
                logger.error(f"[指标服务] 无法获取主指标 {main_metric_en} 的 ID，跳过复合关系")
                continue
            
            # 为每个子指标创建关系记录
            for sub_metric_en in sub_metric_ens:
                # 获取或创建子指标 ID
                sub_metric_id = self._get_or_create_metric_id_batch(
                    sub_metric_en, '', existing_metrics, table_data
                )
                
                if not sub_metric_id:
                    logger.error(f"[指标服务] 无法获取子指标 {sub_metric_en} 的 ID，跳过该子指标关系")
                    continue
                    
                table_data['metric_compound_rel'].append({
                    'metric_id': main_metric_id,
                    'sub_metric_id': sub_metric_id,
                    'cal_operator': cal_operator,
                    'sort': sort,
                    'create_time': get_now_utc8(),  # ⚠️ 直接使用工具函数
                    'modify_time': get_now_utc8()
                })
    
    def _batch_get_metric_ids(self, metric_definitions: List[Dict]) -> Dict[str, str]:
        """
        批量查询已存在的 metric_id
        
        Args:
            metric_definitions: 指标定义列表
            
        Returns:
            {metric_en: metric_id} 字典
        """
        if not metric_definitions:
            return {}
        
        # 提取所有 code（⚠️ AI 返回的是 metric_en，对应数据库的 code 字段）
        codes = [m.get('metric_en', '') for m in metric_definitions if m.get('metric_en')]
        if not codes:
            return {}
        
        # ⚠️ 使用工具类批量查询
        existing_metrics = BatchQueryHelper.query_existing_metric_ids(self.session, codes)
        logger.debug(f"[指标服务] 批量查询到 {len(existing_metrics)} 个已有指标")
        return existing_metrics
    
    def _batch_get_metric_ids_by_ens(self, metric_ens: List[str]) -> Dict[str, str]:
        """
        批量查询已存在的 metric_id（用于复合指标场景）
        
        Args:
            metric_ens: 指标英文编码列表
            
        Returns:
            {metric_en: metric_id} 字典
        """
        if not metric_ens:
            return {}
        
        # ⚠️ 使用工具类批量查询
        existing_metrics = BatchQueryHelper.query_existing_metric_ids(self.session, metric_ens)
        logger.debug(f"[指标服务-复合指标] 批量查询到 {len(existing_metrics)} 个已有指标")
        return existing_metrics
    
    def _get_or_create_metric_id_batch(self, metric_en: str, metric_name: str = '', 
                                       existing_metrics: Dict[str, str] = None,
                                       table_data: Dict[str, List] = None) -> Optional[str]:
        """
        获取或创建指标 ID（批量优化版本）
        
        逻辑：
        1. 先从批量查询结果中查找
        2. 如果不存在，检查当前批次是否已创建（在 table_data 中）
        3. 如果都没有，创建一个新的指标定义记录（使用默认值）
        
        Args:
            metric_en: 指标英文编码
            metric_name: 指标中文名称（可选）
            existing_metrics: 批量查询结果 {metric_en: metric_id}
            table_data: 表数据收集字典
            
        Returns:
            metric_id，如果失败则返回 None
        """
        # 1. 先从批量查询结果中查找
        if existing_metrics and metric_en in existing_metrics:
            metric_id = existing_metrics[metric_en]
            logger.debug(f"[指标服务] 从批量查询找到已有指标: {metric_en} -> {metric_id}")
            return metric_id
        
        # 2. 检查当前批次是否已创建
        if table_data:
            for metric_def in table_data['metric_definition']:
                if metric_def.get('metric_en') == metric_en:  # ⚠️ AI 返回的是 metric_en，对应数据库的 code
                    metric_id = metric_def.get('id')  # ⚠️ 改为 id
                    logger.debug(f"[指标服务] 使用当前批次已创建的指标: {metric_en} -> {metric_id}")
                    return metric_id
        
        # 3. 都不存在，创建新的指标定义（使用默认值）
        new_metric_id = self.metric_id_gen.get_next_id()  # ⚠️ 使用 IdGenerator
        display_name = metric_name if metric_name else metric_en
        logger.info(f"[指标服务] ⚠️ 自动创建复合指标定义: {metric_en} ({display_name}) -> {new_metric_id}")
        
        # 添加到 table_data['metric_definition']
        if table_data is not None:
            table_data['metric_definition'].append({
                'id': new_metric_id,
                'name': display_name,
                'code': metric_en,
                'metric_type': 'COMPOUND',
                'biz_domain': '',
                'status': 1,
                'create_time': get_now_utc8(),
                'modify_time': get_now_utc8()
            })
        
        return new_metric_id
    
    def _collect_metric_dim_rel(self, table_data: Dict[str, List]):
        """
        收集 metric_dim_rel 数据（关联指标和公共维度）
        
        简化逻辑：
        1. 从 field_lineage 中找出标记为 'public_dim' 的字段，获取对应的 dim_id
        2. 为每个指标关联所有公共维度（利用数据库唯一约束自动去重）
        
        Args:
            table_data: 表数据收集字典
        """
        # ⚠️ 第一步：从 field_lineage 中找出所有标记为 'public_dim' 的字段，并获取 dim_id
        valid_dim_ids = set()
        
        # 提取所有唯一的 (db_table, dim_field) 组合
        unique_keys = set()
        for field_lineage in table_data['field_lineage']:
            if field_lineage.get('target_field_mark') == 'public_dim':
                source_table = field_lineage.get('source_table', '')
                source_field = field_lineage.get('source_field', '')
                if source_table and source_field:
                    unique_keys.add((source_table, source_field))
        
        if not unique_keys:
            logger.info(f"[指标服务-metric_dim_rel] ✅ 收集完成: 0 条记录（无公共维度）")
            return
        
        # 批量查询 dim_field_lineage 表，获取对应的 dim_id（关联到 dim_field_lineage.id）
        try:
            values_list = []
            params = {}
            for idx, (db_tbl, dim_fld) in enumerate(unique_keys):
                param_tbl = f'tbl_{idx}'
                param_fld = f'fld_{idx}'
                values_list.append(f'(:{param_tbl}, :{param_fld})')
                params[param_tbl] = db_tbl
                params[param_fld] = dim_fld
            
            values_clause = ', '.join(values_list)
            # ⚠️ 查询 dim_field_lineage 表的 id 字段（不是 dim_id）
            sql = f"SELECT DISTINCT id FROM dim_field_lineage WHERE (db_table, field) IN ({values_clause})"
            logger.debug(f"[指标服务-metric_dim_rel] 批量查询维度字段血缘ID SQL: {sql}")
            logger.debug(f"[指标服务-metric_dim_rel] 查询参数: {params}")
            
            result = self.session.execute(text(sql), params).fetchall()
            valid_dim_ids = {row[0] for row in result}  # 提取 dim_field_lineage.id
            
            if not valid_dim_ids:
                logger.info(f"[指标服务-metric_dim_rel] ✅ 收集完成: 0 条记录（dim_field_lineage 中无匹配数据）")
                return
                
            logger.debug(f"[指标服务-metric_dim_rel] 找到 {len(valid_dim_ids)} 个维度字段血缘ID: {valid_dim_ids}")
        except Exception as e:
            logger.warning(f"[指标服务-metric_dim_rel] 批量查询 dim_field_lineage 失败: {str(e)}")
            return
        
        # ⚠️ 第二步：为每个指标关联所有公共维度（不需要检查是否已存在）
        # 数据库的唯一约束 (metric_id, dim_id) 会自动处理重复
        rel_count = 0
        for metric_def in table_data['metric_definition']:
            metric_id = metric_def.get('id', '')  # ⚠️ 改为 id
            
            for dim_id in valid_dim_ids:
                table_data['metric_dim_rel'].append({
                    'metric_id': metric_id,
                    'dim_id': dim_id,
                    'is_required': 0,  # 默认为非必填
                    'create_time': get_now_utc8(),  # ⚠️ 直接使用工具函数
                    'modify_time': get_now_utc8()
                })
                rel_count += 1
        
        logger.info(f"[指标服务-metric_dim_rel] ✅ 收集完成: {rel_count} 条记录")
    
    def _collect_metric_field_lineage_rel(self, table_data: Dict[str, List]):
        """
        收集 metric_lineage 数据（关联指标和字段血缘）
        
        作用：将 metric_definition 表中的指标与 field_lineage 表中标记为 'metric' 的字段建立关联
        
        Args:
            table_data: 表数据收集字典
        """
        # 构建 code 到 id 的映射（⚠️ AI 返回的是 metric_en，对应数据库的 code）
        code_to_id = {}
        for metric_def in table_data['metric_definition']:
            code = metric_def.get('code', '')  # ⚠️ 改为 code
            metric_id = metric_def.get('id', '')  # ⚠️ 改为 id
            if code and metric_id:
                code_to_id[code] = metric_id
        
        # 遍历所有 field_lineage 记录
        lineage_count = 0
        for field_lineage in table_data['field_lineage']:
            target_field_mark = field_lineage.get('target_field_mark', '')
            target_field = field_lineage.get('target_field', '')
            field_lineage_id = field_lineage.get('id', '')  # ⚠️ 改为 id（之前是 lineage_id）
            
            # 只处理标记为 'metric' 的字段
            if target_field_mark != 'metric':
                continue
            
            # 精确匹配 code（⚠️ target_field 对应数据库的 code）
            matched_metric_id = code_to_id.get(target_field)
            
            if matched_metric_id and field_lineage_id:
                table_data['metric_lineage'].append({
                    'metric_id': matched_metric_id,  # ⚠️ metric_id 在其他表中保持不变
                    'field_lineage_id': field_lineage_id,  # ⚠️ 注意：这里应该是 field_lineage 的 id，不是 lineage_id
                    'create_time': get_now_utc8(),  # ⚠️ 直接使用工具函数
                    'modify_time': get_now_utc8()
                })
                lineage_count += 1
        
        logger.info(f"[指标服务-metric_lineage] ✅ 收集完成: {lineage_count} 条记录")
    

    def _execute_insert(self, table_data: Dict[str, List]) -> Dict[str, Any]:
        """执行数据库插入操作（使用 SqlGenerator）"""
        table_stats = {}
        
        try:
            # 定义各表的配置
            table_configs = {
                'metric_definition': {
                    'columns': ['id', 'name', 'code', 'metric_type', 'biz_domain', 'status', 'create_time', 'modify_time'],  # ⚠️ 已删除 cal_logic, unit
                    'conflict_target': '(code)'
                },
                'metric_dim_rel': {
                    'columns': ['metric_id', 'dim_id', 'is_required', 'create_time', 'modify_time'],
                    'conflict_target': '(metric_id, dim_id)'
                },
                'metric_source_mapping': {
                    'columns': ['id', 'metric_id', 'source_type', 'datasource', 'db_table', 'metric_column', 'metric_name', 'biz_domain', 'filter_condition', 'agg_func', 'priority', 'is_valid', 'source_level', 'cal_logic', 'unit', 'create_time', 'modify_time'],  # ⚠️ 新增 metric_name, biz_domain, cal_logic, unit
                    'conflict_target': '(metric_id, db_table, metric_column)'
                },
                'metric_compound_rel': {
                    'columns': ['metric_id', 'sub_metric_id', 'cal_operator', 'sort', 'create_time', 'modify_time'],
                    'conflict_target': '(metric_id, sub_metric_id)'
                },
                'table_lineage': {
                    'columns': ['id', 'source_table', 'source_table_name', 'target_table', 'target_table_name', 'create_time', 'modify_time'],
                    'conflict_target': '(id)'
                },
                'field_lineage': {
                    'columns': ['id', 'table_lineage_id', 'source_table', 'source_table_name', 'source_field', 'source_field_name', 'target_table', 'target_table_name', 'target_field', 'target_field_name', 'target_field_mark', 'dim_id', 'formula', 'create_time', 'modify_time'],
                    'conflict_target': '(source_table, source_field, target_table, target_field)'
                },
                'metric_lineage': {
                    'columns': ['metric_id', 'field_lineage_id', 'create_time', 'modify_time'],
                    'conflict_target': '(metric_id, field_lineage_id)'
                }
            }
            
            # ⚠️ 批量插入各表数据（使用 SqlGenerator）
            for table_name, data_list in table_data.items():
                if not data_list:
                    continue
                
                config = table_configs.get(table_name)
                if not config:
                    logger.warning(f"[指标服务] 未知的表名: {table_name}")
                    continue
                
                # ⚠️ 使用 SqlGenerator 生成 SQL
                sql = SqlGenerator.generate_batch_upsert(table_name, config, data_list)
                if sql:
                    self.session.execute(text(sql))
                    table_stats[table_name] = len(data_list)
                    logger.info(f"[指标服务] 批量插入 {table_name}: {len(data_list)} 条")
            
            # ⚠️ 不再提交事务，由主流程统一提交
            logger.info(f"[指标服务] ✅ 处理完成 - 共写入 {sum(table_stats.values())} 条记录，等待主流程提交")
            
            return {
                'success': True,
                'table_stats': table_stats
            }
            
        except Exception as e:
            logger.error(f"[指标服务] 执行插入失败: {str(e)}", exc_info=True)
            # ⚠️ 不再返回失败字典，直接抛出异常由主流程统一回滚
            raise
