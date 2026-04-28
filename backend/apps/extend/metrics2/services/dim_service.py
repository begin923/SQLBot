"""
维度服务 - 处理 DIM 层维度定义和字段映射
"""

import logging
from typing import Dict, List, Any

# ⚠️ 导入工具类
from apps.extend.metrics2.services.check_service import CheckService
from apps.extend.metrics2.services.metadata_service import metadataService
from apps.extend.metrics2.utils.id_generator import IdGenerator
from apps.extend.metrics2.utils.batch_query import BatchQueryHelper
from apps.extend.metrics2.utils.timezone_helper import get_now_utc8

logger = logging.getLogger(__name__)


class DimService:
    """
    维度服务 - 专门处理 DIM 层的维度数据
    
    职责：
    1. 从 AI 解析的 fields 中提取维度定义
    2. 生成 dim_id
    3. 收集 dim_definition 和 dim_field_lineage
    4. 执行数据库插入
    """
    
    # ⚠️ 定义关键表配置
    CRITICAL_TABLES = ['dim_definition', 'dim_field_lineage','table_metadata']
    
    def __init__(self, session, check_service:CheckService):
        """
        初始化维度服务
        
        Args:
            session: 数据库会话
            check_service: 校验服务实例（可选）
        """
        self.session = session
        self.check_service = check_service  # ⚠️ 注入 CheckService
        self.metadata_service = metadataService(session)  # ⚠️ 注入 metadataService
        # ⚠️ 使用 IdGenerator 替代手动计数器
        self.dim_definition_id_gen = IdGenerator(session, 'dim_definition', 'D')
        self.dim_field_lineage_id_gen = IdGenerator(session, 'dim_field_lineage', 'DFL')  # ⚠️ 新增
    
    def process(self, processed_results: List[Dict[str, Any]], layer_type: str, dim_cache: Dict[str, str] = None) -> Dict[str, Any]:
        """
        处理 DIM 层维度数据
        
        Args:
            processed_results: 规则引擎处理后的结果列表
            layer_type: 数仓层级类型（DIM），用于统一推断 source_level
            dim_cache: 维度缓存 {code: dim_id}，保证同一批次中相同 code 使用相同 ID
            
        Returns:
            执行结果 {'success': bool, 'message': str, 'table_stats': dict}
            
        Raises:
            Exception: 当数据处理失败时抛出异常，由主流程统一回滚
        """
        try:
            # ⚠️ 根据 CRITICAL_TABLES 动态初始化表数据收集字典
            table_data = {table_name: [] for table_name in self.CRITICAL_TABLES}
            
            # ⚠️ 初始化或获取缓存
            if dim_cache is None:
                dim_cache = {}
            
            # 遍历所有处理结果，收集维度数据
            for idx, processed_result in enumerate(processed_results, 1):
                if not processed_result.get('success', False):
                    logger.warning(f"[DIM服务] 跳过失败的结果 #{idx}")
                    continue
                
                self._collect_dim_data(processed_result, table_data, dim_cache)
                self.metadata_service.collect_table_metadata(processed_result, table_data, layer_type)
            
            # ⚠️ 校验数据完整性（调用 CheckService）
            check_result = self.check_service.check_data_integrity(
                table_data=table_data,
                critical_tables=self.CRITICAL_TABLES  # ⚠️ 传入关键表列表
            )
                
            if not check_result['success']:
                logger.error(check_result['message'])
                raise ValueError(check_result['message'])  # ⚠️ 抛出异常
            
            # ⚠️ 详细输出每张表的数据量
            table_stats = ', '.join([f"{table_name}: {len(records)} 条" for table_name, records in table_data.items()])
            logger.info(f"[DIM服务] ✅ 数据收集成功 - {table_stats}")
            
            return {
                'success': True,
                'message': f"DIM层数据收集成功",
                'table_data': table_data  # ⚠️ 返回原始数据，由主流程统一批量插入
            }
            
        except ValueError:
            # ⚠️ 业务逻辑错误，直接向上抛出
            raise
        except Exception as e:
            error_msg = f"[DIM服务] 处理失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise  # ⚠️ 重新抛出异常
    
    def _collect_dim_data(self, processed_result: Dict[str, Any], table_data: Dict[str, List], dim_cache: Dict[str, str] = None):
        """
        收集 DIM 层维度数据
        
        Args:
            processed_result: 单个文件的处理结果
            table_data: 表数据收集字典
            dim_cache: 全局缓存 {code: dim_id}，保证同一批次中相同 code 使用相同 ID
        """
        try:
            parsed_data = processed_result.get('parsed_data', {})
            basic_info = parsed_data.get('basic_info', {})
            target_table = basic_info.get('target_table', '')
            fields = parsed_data.get('fields', [])
            
            if not fields:
                logger.warning(f"[DIM服务] 没有字段数据，跳过")
                return
            
            if not target_table:
                logger.error(f"[DIM服务] ❌ target_table 为空")
                return
            
            # ⚠️ 初始化缓存
            if dim_cache is None:
                dim_cache = {}
            
            # 1. 批量查询已存在的 dim_id（从数据库）
            existing_dims = self._batch_query_existing_dims(fields)
            
            # 2. 处理每个字段，生成维度定义和字段映射
            for field in fields:
                field_name = field.get('field_name', '')  # 维度字段中文名称 -> dim_name
                field_en = field.get('field', '')          # 维度字段英文名称 -> dim_code（⚠️ AI 返回的是 field，不是 field_en）
                dim_type = field.get('dim_type', '其他')   # 维度类型
                
                if not field_en:  # dim_code 不能为空
                    continue
                
                # ⚠️ 优先级：全局缓存 > 数据库查询 > 新生成
                dim_id = None
                
                # 1. 先检查全局缓存（同一批次内复用）
                if field_en in dim_cache:
                    dim_id = dim_cache[field_en]
                    logger.debug(f"[DIM服务] 从全局缓存复用 dim_id: {dim_id} for {field_en}")
                
                # 2. 再检查数据库（跨批次复用）
                elif field_en in existing_dims:
                    dim_id = existing_dims[field_en]
                    logger.debug(f"[DIM服务] 从数据库复用 dim_id: {dim_id} for {field_en}")
                    # ⚠️ 写入全局缓存，供后续文件使用
                    dim_cache[field_en] = dim_id
                
                # 3. 都不存在，生成新的 dim_id
                else:
                    dim_id = self.dim_definition_id_gen.get_next_id()
                    logger.debug(f"[DIM服务] 生成新 dim_id: {dim_id} for {field_en}")
                    # ⚠️ 写入全局缓存
                    dim_cache[field_en] = dim_id
                
                # 收集 dim_definition
                table_data['dim_definition'].append({
                    'id': dim_id,
                    'name': field_name,
                    'code': field_en,
                    'type': dim_type,
                    'is_valid': 1,
                    'create_time': get_now_utc8(),
                    'modify_time': get_now_utc8()
                })
                
                logger.debug(f"[DIM服务] 收集 dim_definition: {dim_id} - {field_name} ({field_en})")
                
                # 生成 dim_field_lineage 的 id（使用 IdGenerator）
                field_lineage_id = self.dim_field_lineage_id_gen.get_next_id()
                
                # 同时收集 dim_field_lineage（指向同一个 dim_id）
                table_data['dim_field_lineage'].append({
                    'id': field_lineage_id,
                    'dim_id': dim_id,  # ⚠️ 确保使用正确的 dim_id
                    'db_table': target_table,
                    'field': field_en,
                    'field_name': field_name,
                    'create_time': get_now_utc8(),
                    'modify_time': get_now_utc8()
                })
            
            logger.info(f"[DIM服务] 收集完成 - dim_definition: {len(table_data['dim_definition'])} 条, dim_field_lineage: {len(table_data['dim_field_lineage'])} 条")
            
        except Exception as e:
            logger.error(f"[DIM服务] 收集数据失败: {str(e)}", exc_info=True)
            raise
    
    def _batch_query_existing_dims(self, fields: List[Dict]) -> Dict[str, str]:
        """
        批量查询已存在的维度
        
        Args:
            fields: 字段列表
            
        Returns:
            {dim_code: dim_id} 字典
        """
        # 第一步：收集所有 dim_code（直接使用 field_en，不需要规范化）
        all_dim_codes = []
        for field in fields:
            field_en = field.get('field_en', '')
            if field_en:
                all_dim_codes.append(field_en)
        
        # 第二步：批量查询已存在的 dim_id（使用工具类）
        existing_dims = BatchQueryHelper.query_existing_dim_ids(self.session, all_dim_codes)
        logger.debug(f"[DIM服务] 批量查询到 {len(existing_dims)} 个已有维度")
        return existing_dims
