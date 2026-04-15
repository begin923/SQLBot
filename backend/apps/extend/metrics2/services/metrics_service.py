from typing import List, Dict, Any, Optional
import logging
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger("MetricsService")


class MetricsService:
    """指标服务 - 专门处理 METRIC 层的指标数据"""
    
    def __init__(self, session: Session):
        """
        初始化指标服务
        
        Args:
            session: 数据库会话
        """
        self.session = session
        self._metric_id_counter = 0
        self._map_id_counter = 0
        # ⚠️ 创建 LineageService 实例处理血缘数据
        from .lineage_service import LineageService
        self.lineage_service = LineageService(session)
    
    def process(self, processed_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        处理 METRIC 层指标数据
        
        Args:
            processed_results: 规则引擎处理后的结果列表
            
        Returns:
            处理结果，包含成功状态、消息和表统计信息
        """
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
        for processed_result in processed_results:
            self._collect_metric_data(processed_result, table_data)
        
        # ⚠️ 校验数据完整性
        validation_result = self._validate_data_integrity(table_data)
        if not validation_result['success']:
            return {
                'success': False,
                'message': validation_result['message']
            }
        
        # 执行数据库插入
        execution_result = self._execute_insert(table_data)
        
        return {
            'success': True,
            'message': f"METRIC层处理成功，写入 {len(table_data['metric_definition'])} 个指标",
            'table_stats': execution_result.get('table_stats', {})
        }
    
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
    
    def _collect_metric_data(self, processed_result: Dict[str, Any], table_data: Dict[str, List]):
        """
        收集 METRIC 层指标数据（总控方法）
            
        Args:
            processed_result: 单个文件的处理结果
            table_data: 表数据收集字典
        """
        try:
            parsed_data = processed_result.get('parsed_data', {})
                
            logger.info(f"[指标服务] 开始处理 METRIC 层数据")
                
            # 1. 使用 LineageService 收集表级和字段级血缘
            self._collect_lineage_via_service(processed_result, table_data)
                
            # 2. 收集指标定义和源映射
            self._collect_metric_definitions(parsed_data, table_data)
                
            # 3. 收集复合指标关系
            self._collect_compound_relations(parsed_data, table_data)
                
            # ⚠️ 4. 收集指标-维度关联关系（metric_dim_rel）
            self._collect_metric_dim_rel(table_data)
                
            # 5. 关联指标和字段血缘
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
        收集指标定义和源映射
            
        Args:
            parsed_data: AI 解析后的数据
            table_data: 表数据收集字典
        """
        ai_metric_definitions = parsed_data.get('metric_definition', [])
        ai_source_mappings = parsed_data.get('metric_source_mapping', [])
        
        # ⚠️ 批量查询已存在的 metric_id
        existing_metrics = self._batch_get_metric_ids(ai_metric_definitions)
        
        # ⚠️ 构建 metric_en 到 metric_id 的映射（包括新生成的）
        metric_en_to_id = {}
            
        for metric_def in ai_metric_definitions:
            metric_name = metric_def.get('metric_name', '')
            metric_en = metric_def.get('metric_en', '')
            metric_type_raw = metric_def.get('metric_type', 'atomic')
            biz_domain = metric_def.get('biz_domain', '')
            cal_logic = metric_def.get('cal_logic', '')
            unit = metric_def.get('unit')
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
                metric_id = self._get_next_metric_id()
            
            # 记录映射关系
            metric_en_to_id[metric_en] = metric_id
                
            # 收集 metric_definition 数据
            table_data['metric_definition'].append({
                'id': metric_id,  # ⚠️ 改为 id
                'name': metric_name,  # ⚠️ 改为 name
                'code': metric_en,  # ⚠️ 改为 code（AI 返回的 metric_en）
                'metric_type': metric_type,
                'biz_domain': biz_domain,
                'cal_logic': cal_logic,
                'unit': unit or '',
                'status': status
            })
        
        # ⚠️ 批量收集所有指标的源映射（一次性查询）
        self._collect_all_source_mappings(ai_source_mappings, metric_en_to_id, table_data)
        
    def _collect_all_source_mappings(self, ai_source_mappings: List[Dict], 
                                     metric_en_to_id: Dict[str, str],
                                     table_data: Dict[str, List]):
        """
        批量收集所有指标的源映射（一次性查询）
            
        Args:
            ai_source_mappings: AI 返回的源映射列表
            metric_en_to_id: {metric_en: metric_id} 映射
            table_data: 表数据收集字典
        """
        if not ai_source_mappings:
            return
        
        # ⚠️ 第一步：按 metric_id 分组源映射
        mappings_by_metric = {}  # {metric_id: [mappings]}
        for mapping in ai_source_mappings:
            metric_column = mapping.get('metric_column', '')
            if not metric_column or metric_column not in metric_en_to_id:
                continue
            
            metric_id = metric_en_to_id[metric_column]
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
                    text("SELECT map_id, metric_id, db_table, COALESCE(metric_column, '') as metric_column FROM metric_source_mapping WHERE metric_id IN :metric_ids AND db_table IN :db_tables AND COALESCE(metric_column, '') IN :metric_columns"),
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
                filter_condition = mapping.get('filter_condition', '')
                agg_func = mapping.get('agg_func', '')
                priority = mapping.get('priority', 1)
                is_valid = mapping.get('is_valid', 1)
                source_level = mapping.get('source_level', 'AUTHORITY')
                source_type = mapping.get('source_type', 'OFFLINE').upper()
                    
                # 从批量查询结果中获取 map_id
                unique_key = (metric_id, db_table, metric_column or '')
                if unique_key in all_existing_maps:
                    map_id = all_existing_maps[unique_key]
                else:
                    map_id = self._get_next_map_id()
                    
                table_data['metric_source_mapping'].append({
                    'map_id': map_id,
                    'metric_id': metric_id,
                    'source_type': source_type,
                    'datasource': datasource,
                    'db_table': db_table,
                    'metric_column': metric_column,
                    'filter_condition': filter_condition,
                    'agg_func': agg_func,
                    'priority': priority,
                    'is_valid': is_valid,
                    'source_level': source_level
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
        
        # ⚠️ 第一步：收集所有需要的 metric_en（主指标 + 子指标）
        all_metric_ens = set()
        compound_info = []  # 保存复合关系信息
        
        for rel in ai_compound_rels:
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
                    'sort': sort
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
        
        try:
            result = self.session.execute(
                text("SELECT code, id FROM metric_definition WHERE code IN :codes"),
                {"codes": tuple(codes)}
            ).fetchall()
            
            # 构建字典
            existing_metrics = {row[0]: row[1] for row in result}
            logger.debug(f"[指标服务] 批量查询到 {len(existing_metrics)} 个已有指标")
            return existing_metrics
        except Exception as e:
            logger.warning(f"[指标服务] 批量查询 metric_id 失败: {str(e)}")
            return {}
    
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
        
        try:
            result = self.session.execute(
                text("SELECT metric_en, metric_id FROM metric_definition WHERE metric_en IN :metric_ens"),
                {"metric_ens": tuple(metric_ens)}
            ).fetchall()
            
            # 构建字典
            existing_metrics = {row[0]: row[1] for row in result}
            logger.debug(f"[指标服务-复合指标] 批量查询到 {len(existing_metrics)} 个已有指标")
            return existing_metrics
        except Exception as e:
            logger.warning(f"[指标服务-复合指标] 批量查询 metric_id 失败: {str(e)}")
            return {}
    
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
        new_metric_id = self._get_next_metric_id()
        display_name = metric_name if metric_name else metric_en
        logger.info(f"[指标服务] ⚠️ 自动创建复合指标定义: {metric_en} ({display_name}) -> {new_metric_id}")
        
        # 添加到 table_data['metric_definition']
        if table_data is not None:
            table_data['metric_definition'].append({
                'id': new_metric_id,  # ⚠️ 改为 id
                'name': display_name,  # ⚠️ 改为 name
                'code': metric_en,  # ⚠️ 改为 code（AI 返回的 metric_en）
                'metric_type': 'COMPOUND',
                'biz_domain': '',
                'cal_logic': '',
                'unit': '',
                'status': 1
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
        
        # 批量查询 dim_field_mapping 表，获取对应的 dim_id
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
            sql = f"SELECT DISTINCT dim_id FROM dim_field_mapping WHERE (db_table, field) IN ({values_clause})"
            logger.debug(f"[指标服务-metric_dim_rel] 批量查询维度映射 SQL: {sql}")
            logger.debug(f"[指标服务-metric_dim_rel] 查询参数: {params}")
            
            result = self.session.execute(text(sql), params).fetchall()
            valid_dim_ids = {row[0] for row in result}  # 直接提取 dim_id
            
            if not valid_dim_ids:
                logger.info(f"[指标服务-metric_dim_rel] ✅ 收集完成: 0 条记录（dim_field_mapping 中无匹配数据）")
                return
                
            logger.debug(f"[指标服务-metric_dim_rel] 找到 {len(valid_dim_ids)} 个公共维度ID: {valid_dim_ids}")
        except Exception as e:
            logger.warning(f"[指标服务-metric_dim_rel] 批量查询 dim_field_mapping 失败: {str(e)}")
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
                    'is_required': 0  # 默认为非必填
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
                    'field_lineage_id': field_lineage_id  # ⚠️ 注意：这里应该是 field_lineage 的 id，不是 lineage_id
                })
                lineage_count += 1
        
        logger.info(f"[指标服务-metric_lineage] ✅ 收集完成: {lineage_count} 条记录")
    

    def _get_next_metric_id(self) -> str:
        """生成下一个唯一的指标ID"""
        if self._metric_id_counter == 0:
            try:
                result = self.session.execute(
                    text("SELECT MAX(CAST(SUBSTRING(id FROM 2) AS INTEGER)) FROM metric_definition")  # ⚠️ 改为 id
                ).scalar()
                
                if result is not None:
                    self._metric_id_counter = int(result)
                else:
                    self._metric_id_counter = 0
            except Exception as e:
                logger.warning(f"[指标服务] 查询最大metric_id失败: {str(e)}，从0开始")
                self._metric_id_counter = 0
        
        self._metric_id_counter += 1
        return f"M{self._metric_id_counter:06d}"
    
    def _get_next_map_id(self) -> str:
        """生成下一个唯一的映射ID"""
        if self._map_id_counter == 0:
            try:
                result = self.session.execute(
                    text("SELECT MAX(CAST(SUBSTRING(map_id FROM 4) AS INTEGER)) FROM metric_source_mapping")
                ).scalar()
                
                if result is not None:
                    self._map_id_counter = int(result)
                else:
                    self._map_id_counter = 0
            except Exception as e:
                logger.warning(f"[指标服务] 查询最大map_id失败: {str(e)}，从0开始")
                self._map_id_counter = 0
        
        self._map_id_counter += 1
        return f"MAP{self._map_id_counter:06d}"
    

    def _execute_insert(self, table_data: Dict[str, List]) -> Dict[str, Any]:
        """执行数据库插入操作"""
        table_stats = {}
        
        try:
            # 批量插入各表数据
            for table_name, data_list in table_data.items():
                if not data_list:
                    continue
                
                sql = self._generate_batch_upsert_sql(table_name, data_list)
                if sql:
                    self.session.execute(text(sql))
                    table_stats[table_name] = len(data_list)
                    logger.info(f"[指标服务] 批量插入 {table_name}: {len(data_list)} 条")
            
            # 提交事务
            self.session.commit()
            logger.info(f"[指标服务] ✅ 处理完成 - 共写入 {sum(table_stats.values())} 条记录")
            
            return {
                'success': True,
                'table_stats': table_stats
            }
            
        except Exception as e:
            logger.error(f"[指标服务] 执行插入失败: {str(e)}")
            self.session.rollback()
            return {
                'success': False,
                'message': f'数据库插入失败: {str(e)}'
            }
    
    def _generate_batch_upsert_sql(self, table_name: str, data_list: List[Dict]) -> Optional[str]:
        """生成批量 UPSERT SQL"""
        if not data_list:
            return None
        
        try:
            # 定义各表的字段映射和冲突键
            table_config = {
                'metric_definition': {
                    'columns': ['id', 'name', 'code', 'metric_type', 'biz_domain', 'cal_logic', 'unit', 'status'],  # ⚠️ 改为新字段名
                    'conflict_target': '(code)'  # ⚠️ 使用 code 作为唯一约束键
                },
                'metric_dim_rel': {
                    'columns': ['metric_id', 'dim_id', 'is_required'],
                    'conflict_target': '(metric_id, dim_id)'
                },
                'metric_source_mapping': {
                    'columns': ['map_id', 'metric_id', 'source_type', 'datasource', 'db_table', 'metric_column', 'filter_condition', 'agg_func', 'priority', 'is_valid', 'source_level'],
                    'conflict_target': '(metric_id, db_table, metric_column)'  # ⚠️ 使用唯一约束键
                },
                'metric_compound_rel': {
                    'columns': ['metric_id', 'sub_metric_id', 'cal_operator', 'sort'],
                    'conflict_target': '(metric_id, sub_metric_id)'
                },
                'table_lineage': {
                    'columns': ['id', 'source_table', 'source_table_name', 'target_table', 'target_table_name'],  # ⚠️ lineage_id 改为 id
                    'conflict_target': '(id)'  # ⚠️ 使用主键作为冲突键
                },
                'field_lineage': {
                    'columns': ['id', 'table_lineage_id', 'source_table', 'source_table_name', 'source_field', 'source_field_name', 'target_table', 'target_table_name', 'target_field', 'target_field_name', 'target_field_mark', 'dim_id', 'formula'],  # ⚠️ lineage_id 改为 id
                    'conflict_target': '(source_table, source_field, target_table, target_field)'  # ⚠️ 使用业务唯一键作为冲突键
                },
                'metric_lineage': {
                    'columns': ['metric_id', 'field_lineage_id'],
                    'conflict_target': '(metric_id, field_lineage_id)'
                }
            }
            
            config = table_config.get(table_name)
            if not config:
                logger.warning(f"[指标服务] 未知的表名: {table_name}")
                return None
            
            columns = config['columns']
            conflict_target = config['conflict_target']
            
            # 构建 VALUES 子句
            values_list = []
            for data in data_list:
                values = []
                for col in columns:
                    value = data.get(col)
                    if value is None:
                        values.append('NULL')
                    elif isinstance(value, str):
                        escaped_value = value.replace("'", "''")
                        values.append(f"'{escaped_value}'")
                    else:
                        values.append(str(value))
                values_list.append(f"({', '.join(values)})")
            
            values_str = ',\n            '.join(values_list)
            
            # 构建 UPDATE 子句（排除主键和冲突键）
            conflict_columns = [col.strip() for col in conflict_target.strip('()').split(',')]
            
            # ⚠️ 额外排除主键字段（如 map_id, id 等），即使它们不在冲突键中
            primary_key_columns = ['map_id', 'id']  # ⚠️ lineage_id 改为 id
            update_columns = [col for col in columns if col not in conflict_columns and col not in primary_key_columns]
            
            # ⚠️ 如果所有字段都是冲突键，使用 DO NOTHING
            if not update_columns:
                sql = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES {values_str}
                ON CONFLICT {conflict_target} DO NOTHING
                """
            else:
                # 否则使用 DO UPDATE SET
                update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
                sql = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES {values_str}
                ON CONFLICT {conflict_target} DO UPDATE SET
                    {update_str}
                """
            
            return sql
            
        except Exception as e:
            logger.error(f"[指标服务] 生成 SQL 失败: {str(e)}")
            return None
