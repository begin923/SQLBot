"""
维度服务 - 处理 DIM 层维度定义和字段映射
"""

import logging
from typing import Dict, List, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class DimService:
    """
    维度服务 - 专门处理 DIM 层的维度数据
    
    职责：
    1. 从 AI 解析的 fields 中提取维度定义
    2. 生成 dim_id
    3. 收集 dim_definition 和 dim_field_mapping
    4. 执行数据库插入
    """
    
    def __init__(self, session):
        """
        初始化维度服务
        
        Args:
            session: 数据库会话
        """
        self.session = session
        self._dim_id_counter = 0  # 实例级别的计数器，避免重复生成 ID
    
    def process(self, processed_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        处理 DIM 层维度数据
        
        Args:
            processed_results: 规则引擎处理后的结果列表
            
        Returns:
            执行结果 {'success': bool, 'message': str, 'table_stats': dict}
        """
        try:
            # 初始化表数据收集字典
            table_data = {
                'dim_definition': [],
                'dim_field_mapping': []
            }
            
            # 遍历所有处理结果，收集维度数据
            for idx, processed_result in enumerate(processed_results, 1):
                if not processed_result.get('success', False):
                    logger.warning(f"[DIM服务] 跳过失败的结果 #{idx}")
                    continue
                
                logger.debug(f"[DIM服务] 处理结果 #{idx}")
                self._collect_dim_data(processed_result, table_data)
            
            # 校验数据完整性
            if not table_data['dim_definition']:
                error_msg = "❌ DIM 层未生成任何 dim_definition 数据"
                logger.error(error_msg)
                return {'success': False, 'message': error_msg}
            
            if not table_data['dim_field_mapping']:
                error_msg = "❌ DIM 层未生成任何 dim_field_mapping 数据"
                logger.error(error_msg)
                return {'success': False, 'message': error_msg}
            
            # 执行数据库插入
            execution_result = self._execute_insert(table_data)
            
            logger.info(f"[DIM服务] ✅ 处理完成 - 维度数: {len(table_data['dim_definition'])}")
            
            return {
                'success': True,
                'message': f"DIM层处理成功，写入 {len(table_data['dim_definition'])} 个维度",
                'table_stats': execution_result.get('table_stats', {})
            }
            
        except Exception as e:
            error_msg = f"[DIM服务] 处理失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {'success': False, 'message': error_msg}
    
    def _collect_dim_data(self, processed_result: Dict[str, Any], table_data: Dict[str, List]):
        """
        收集 DIM 层维度数据（总控方法）
        
        Args:
            processed_result: 单个文件的处理结果
            table_data: 表数据收集字典
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
            
            # 1. 批量查询已存在的 dim_id
            existing_dims = self._batch_query_existing_dims(fields)
            
            # 2. 处理每个字段，生成维度定义和字段映射
            now = datetime.now()
            self._process_fields(fields, target_table, existing_dims, now, table_data)
            
            logger.info(f"[DIM服务] 收集完成 - dim_definition: {len(table_data['dim_definition'])} 条, dim_field_mapping: {len(table_data['dim_field_mapping'])} 条")
            
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
        
        # 第二步：批量查询已存在的 dim_id
        existing_dims = self._batch_get_dim_ids(all_dim_codes)
        logger.debug(f"[DIM服务] 批量查询到 {len(existing_dims)} 个已有维度")
        return existing_dims
    
    def _process_fields(self, fields: List[Dict], target_table: str, 
                       existing_dims: Dict[str, str], now: datetime,
                       table_data: Dict[str, List]):
        """
        处理字段列表，生成维度定义和字段映射
        
        Args:
            fields: 字段列表
            target_table: 目标表名
            existing_dims: 已存在的维度 {dim_code: dim_id}
            now: 当前时间
            table_data: 表数据收集字典
        """
        for field in fields:
            field_name = field.get('field_name', '')  # 维度字段中文名称 -> dim_name
            field_en = field.get('field', '')          # 维度字段英文名称 -> dim_code（⚠️ AI 返回的是 field，不是 field_en）
            dim_type = field.get('dim_type', '其他')   # 维度类型
            
            if not field_en:  # dim_code 不能为空
                continue
            
            # 从批量查询结果中获取 dim_id
            dim_id = self._get_or_create_dim_id(field_en, existing_dims)
            
            # 收集 dim_definition
            table_data['dim_definition'].append({
                'id': dim_id,
                'name': field_name,
                'code': field_en,
                'type': dim_type,
                'is_valid': 1,
                'create_time': now,
                'modify_time': now
            })
            
            logger.debug(f"[DIM服务] 收集 dim_definition: {dim_id} - {field_name} ({field_en})")
            
            # 同时收集 dim_field_mapping（指向同一个 dim_id）
            table_data['dim_field_mapping'].append({
                'dim_id': dim_id,
                'db_table': target_table,
                'field': field_en,
                'field_name': field_name
            })
    
    def _get_or_create_dim_id(self, dim_code: str, existing_dims: Dict[str, str]) -> str:
        """
        获取或创建 dim_id
        
        Args:
            dim_code: 维度编码
            existing_dims: 已存在的维度 {dim_code: dim_id}
            
        Returns:
            dim_id
        """
        if dim_code in existing_dims:
            dim_id = existing_dims[dim_code]
            logger.debug(f"[DIM服务] 复用已有 dim_id: {dim_id} for {dim_code}")
        else:
            # 生成新的 dim_id
            dim_id = self._get_next_dim_id()
            logger.debug(f"[DIM服务] 生成新 dim_id: {dim_id} for {dim_code}")
        
        return dim_id
    

    def _get_next_dim_id(self) -> str:
        """
        生成下一个 dim_id（使用实例计数器避免重复）
        
        Returns:
            新的 dim_id (格式: D000001, D000002, ...)
        """
        # 首次调用时，从数据库查询最大 ID
        if self._dim_id_counter == 0:
            try:
                from sqlalchemy import text
                result = self.session.execute(
                    text("SELECT MAX(CAST(SUBSTRING(id FROM 2) AS INTEGER)) FROM dim_definition")
                ).fetchone()
                
                max_num = result[0] if result and result[0] else 0
                self._dim_id_counter = int(max_num)
                logger.debug(f"[DIM服务] 初始化 dim_id 计数器: {self._dim_id_counter}")
            except Exception as e:
                logger.warning(f"[DIM服务] 查询最大 dim_id 失败，从 0 开始: {str(e)}")
                self._dim_id_counter = 0
        
        # 递增计数器并生成新 ID
        self._dim_id_counter += 1
        return f"D{self._dim_id_counter:06d}"
    
    def _batch_get_dim_ids(self, dim_codes: List[str]) -> Dict[str, str]:
        """
        批量查询已存在的 dim_id
        
        Args:
            dim_codes: 维度编码列表
            
        Returns:
            {dim_code: dim_id} 字典
        """
        if not dim_codes:
            return {}
        
        # 去重
        unique_codes = list(set(dim_codes))
        
        try:
            from sqlalchemy import text
            result = self.session.execute(
                text("SELECT code, id FROM dim_definition WHERE code IN :codes"),
                {"codes": tuple(unique_codes)}
            ).fetchall()
            
            # 构建字典
            existing_dims = {row[0]: row[1] for row in result}
            logger.debug(f"[DIM服务] 批量查询到 {len(existing_dims)} 个已有维度")
            return existing_dims
        except Exception as e:
            logger.warning(f"[DIM服务] 批量查询 dim_id 失败: {str(e)}")
            return {}
    
    def _execute_insert(self, table_data: Dict[str, List]) -> Dict[str, Any]:
        """
        执行数据库插入操作
        
        Args:
            table_data: 表数据收集字典
            
        Returns:
            执行结果
        """
        try:
            from sqlalchemy import text
            
            table_stats = {}
            
            # 批量插入 dim_definition
            if table_data['dim_definition']:
                values_list = []
                for item in table_data['dim_definition']:
                    name_escaped = item.get('name', '').replace("'", "''")
                    values_list.append(
                        f"('{item['id']}', '{name_escaped}', '{item['code']}', "
                        f"'{item['type']}', {item['is_valid']}, "
                        f"'{item['create_time']}', '{item['modify_time']}')"
                    )
                
                values_str = ',\n'.join(values_list)
                sql = f"""
                    INSERT INTO dim_definition (id, name, code, type, is_valid, create_time, modify_time)
                    VALUES {values_str}
                    ON CONFLICT (code) DO UPDATE SET
                        id = EXCLUDED.id,
                        name = EXCLUDED.name,
                        type = EXCLUDED.type,
                        is_valid = EXCLUDED.is_valid,
                        modify_time = EXCLUDED.modify_time
                """
                
                self.session.execute(text(sql))
                table_stats['dim_definition'] = len(table_data['dim_definition'])
                logger.info(f"[DIM服务] 批量插入 dim_definition: {len(table_data['dim_definition'])} 条")
            
            # 批量插入 dim_field_mapping
            if table_data['dim_field_mapping']:
                values_list = []
                for item in table_data['dim_field_mapping']:
                    field_name_escaped = item.get('field_name', '').replace("'", "''")
                    values_list.append(
                        f"('{item['db_table']}', '{item['field']}', '{item['dim_id']}', '{field_name_escaped}')"
                    )
                
                values_str = ',\n'.join(values_list)
                sql = f"""
                    INSERT INTO dim_field_mapping (db_table, field, dim_id, field_name)
                    VALUES {values_str}
                    ON CONFLICT (db_table, field) DO UPDATE SET
                        dim_id = EXCLUDED.dim_id,
                        field_name = EXCLUDED.field_name,
                        modify_time = EXCLUDED.modify_time
                """
                
                self.session.execute(text(sql))
                table_stats['dim_field_mapping'] = len(table_data['dim_field_mapping'])
                logger.info(f"[DIM服务] 批量插入 dim_field_mapping: {len(table_data['dim_field_mapping'])} 条")
            
            # 提交事务
            self.session.commit()
            
            return {
                'success': True,
                'table_stats': table_stats
            }
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"[DIM服务] 数据库插入失败: {str(e)}", exc_info=True)
            raise
