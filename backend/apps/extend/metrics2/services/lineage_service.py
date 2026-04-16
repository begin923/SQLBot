"""
血缘服务 - 处理 DWD 层的表级和字段级血缘关系
"""

import logging
from typing import Dict, List, Any, Tuple
from sqlalchemy import text

# ⚠️ 导入工具类
from apps.extend.metrics2.utils.id_generator import IdGenerator
from apps.extend.metrics2.utils.batch_query import BatchQueryHelper
from apps.extend.metrics2.utils.sql_generator import SqlGenerator
from apps.extend.metrics2.utils.timezone_helper import get_now_utc8

logger = logging.getLogger(__name__)


class LineageService:
    """
    血缘服务 - 专门处理 DWD 层的血缘数据
    
    职责：
    1. 从 AI 解析的 table_lineage 和 field_lineage 中提取血缘关系
    2. 生成 lineage_id（T000001, F000001）
    3. 收集 table_lineage 和 field_lineage
    4. 执行数据库插入
    """
    
    def __init__(self, session):
        """
        初始化血缘服务
        
        Args:
            session: 数据库会话
        """
        self.session = session
        # ⚠️ 使用 IdGenerator 替代手动计数器
        self.table_lineage_id_gen = IdGenerator(session, 'table_lineage', 'T')
        self.field_lineage_id_gen = IdGenerator(session, 'field_lineage', 'F')
    
    def process(self, processed_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        处理 DWD 层血缘数据
        
        Args:
            processed_results: 规则引擎处理后的结果列表
            
        Returns:
            执行结果 {'success': bool, 'message': str, 'table_stats': dict}
        """
        try:
            # 初始化表数据收集字典
            table_data = {
                'table_lineage': [],
                'field_lineage': []
            }
            
            # 遍历所有处理结果，收集血缘数据
            for idx, processed_result in enumerate(processed_results, 1):
                if not processed_result.get('success', False):
                    logger.warning(f"[血缘服务] 跳过失败的结果 #{idx}")
                    continue
                
                logger.debug(f"[血缘服务] 处理结果 #{idx}")
                self.collect_lineage(processed_result, table_data)
            
            # 校验数据完整性
            if not table_data['table_lineage']:
                error_msg = "❌ DWD 层未生成任何 table_lineage 数据"
                logger.error(error_msg)
                return {'success': False, 'message': error_msg}
            
            if not table_data['field_lineage']:
                error_msg = "❌ DWD 层未生成任何 field_lineage 数据"
                logger.error(error_msg)
                return {'success': False, 'message': error_msg}
            
            # 执行数据库插入
            execution_result = self._execute_insert(table_data)
            
            logger.info(f"[血缘服务] ✅ 处理完成 - 表血缘: {len(table_data['table_lineage'])}, 字段血缘: {len(table_data['field_lineage'])}")
            
            return {
                'success': True,
                'message': f"DWD层处理成功，写入 {len(table_data['table_lineage'])} 条表血缘, {len(table_data['field_lineage'])} 条字段血缘",
                'table_stats': execution_result.get('table_stats', {})
            }
            
        except Exception as e:
            # ⚠️ 确保异常时回滚事务，避免 PendingRollbackError
            try:
                self.session.rollback()
                logger.warning("[血缘服务] 已回滚事务")
            except Exception as rollback_error:
                logger.error(f"[血缘服务] 回滚事务失败: {str(rollback_error)}")
            
            # ⚠️ 简化错误信息，不打印完整SQL
            error_type = type(e).__name__
            if 'StringDataRightTruncation' in str(e):
                error_msg = f"[血缘服务] 处理失败: 字段长度超限 (VARCHAR(500))"
            elif 'UniqueViolation' in str(e):
                error_msg = f"[血缘服务] 处理失败: 唯一约束冲突"
            elif 'PendingRollback' in str(e):
                error_msg = f"[血缘服务] 处理失败: 事务状态异常"
            else:
                error_msg = f"[血缘服务] 处理失败: {error_type}"
            
            logger.error(error_msg)  # ⚠️ 不使用 exc_info，避免打印完整SQL
            return {'success': False, 'message': error_msg}
    
    def collect_lineage(self, processed_result: Dict[str, Any], table_data: Dict[str, List]):
        """
        收集血缘数据（总控方法）
        
        Args:
            processed_result: 单个文件的处理结果
            table_data: 表数据收集字典
        """
        try:
            parsed_data = processed_result.get('parsed_data', {})
            basic_info = parsed_data.get('basic_info', {})
            target_table = basic_info.get('target_table', '')
            
            # ⚠️ Fallback：如果 AI 没有返回 target_table，尝试从 SQL 中提取
            if not target_table:
                sql_content = basic_info.get('sql_content', '')
                target_table = self._extract_target_table_from_sql(sql_content)
                if target_table:
                    logger.warning(f"[血缘服务] ⚠️ AI 未返回 target_table，已从 SQL 中提取: {target_table}")
                else:
                    logger.error("[血缘服务] ❌ target_table 为空，且无法从 SQL 中提取")
                    return
            
            # 1. 收集 table_lineage
            self._collect_table_lineage(parsed_data, target_table, table_data)
            
            # 2. 收集 field_lineage
            self._collect_field_lineage(parsed_data, target_table, table_data)
            
        except Exception as e:
            logger.error(f"[血缘服务] 收集数据失败: {str(e)}", exc_info=True)
            raise
    
    def _collect_table_lineage(self, parsed_data: Dict[str, Any], target_table: str, table_data: Dict[str, List]):
        """
        收集表级血缘数据
        
        Args:
            parsed_data: AI 解析后的数据
            target_table: 目标表名
            table_data: 表数据收集字典
        """
        ai_table_lineage = parsed_data.get('table_lineage', [])
        logger.info(f"[血缘服务] AI 输出 table_lineage 数量: {len(ai_table_lineage)}")
        
        # ⚠️ 第一步：去重 - 基于 (source_table, target_table)
        seen_keys = set()
        unique_table_lineage = []
        duplicate_count = 0
        
        for tl in ai_table_lineage:
            source_table = tl.get('source_table', '')
            tgt_table = tl.get('target_table', target_table)
            
            if not source_table:
                continue
            
            key = (source_table, tgt_table)
            if key not in seen_keys:
                seen_keys.add(key)
                unique_table_lineage.append(tl)
            else:
                duplicate_count += 1
                logger.warning(f"[血缘服务] ⚠️ AI 输出中发现重复表血缘: {key}")
        
        if duplicate_count > 0:
            logger.info(f"[血缘服务] ⚠️ AI 输出中去重: 原始 {len(ai_table_lineage)} 条, 去重后 {len(unique_table_lineage)} 条, 跳过 {duplicate_count} 条重复")
        
        # ⚠️ 第二步：收集所有源表和目标表（使用去重后的数据）
        all_source_tables = []
        all_target_tables = []
        for tl in unique_table_lineage:
            source_table = tl.get('source_table', '')
            tgt_table = tl.get('target_table', target_table)
            if source_table:
                all_source_tables.append(source_table)
            if tgt_table:
                all_target_tables.append(tgt_table)
        
        # ⚠️ 第三步：批量查询已有记录（使用工具类）
        existing_table_lineage = BatchQueryHelper.query_existing_table_lineage(
            self.session, all_source_tables, all_target_tables
        )
        logger.debug(f"[血缘服务] 数据库中已存在 {len(existing_table_lineage)} 条表血缘记录")
        
        # ⚠️ 第四步：为所有表对生成或复用 lineage_id（包括已存在的）
        new_count = 0
        skipped_count = 0
        
        # ⚠️ 使用 seen_ids 防止批次内 ID 冲突
        seen_ids = set()
        
        for tl in unique_table_lineage:
            source_table = tl.get('source_table', '')
            tgt_table = tl.get('target_table', target_table)
            
            if not source_table:
                continue
            
            key = (source_table, tgt_table)
            
            # 检查是否已存在
            if key in existing_table_lineage:
                # ⚠️ 已存在的记录不需要插入数据库，但需要保留在 table_data 中供 field_lineage 查找
                lineage_id = existing_table_lineage[key]
                skipped_count += 1
                logger.debug(f"[血缘服务] 表血缘已存在，跳过插入: {source_table} -> {tgt_table} (ID: {lineage_id})")
                
                table_data['table_lineage'].append({
                    'id': lineage_id,
                    'source_table': source_table,
                    'source_table_name': tl.get('source_table_name', ''),
                    'target_table': tgt_table,
                    'target_table_name': tl.get('target_table_name', ''),
                    'create_time': get_now_utc8(),  # ⚠️ 直接使用工具函数
                    'modify_time': get_now_utc8(),
                    'is_exists': True  # ⚠️ 标记为已存在，_execute_insert 会过滤
                })
                continue
            
            # ⚠️ 生成新的 lineage_id（使用 IdGenerator），并确保不与批次内已有 ID 冲突
            max_retries = 10
            lineage_id = None
            for retry in range(max_retries):
                candidate_id = self.table_lineage_id_gen.get_next_id()
                if candidate_id not in seen_ids:
                    lineage_id = candidate_id
                    seen_ids.add(lineage_id)
                    break
                else:
                    logger.warning(f"[血缘服务] ⚠️ ID {candidate_id} 已在批次中使用，重新生成...")
            
            if not lineage_id:
                logger.error(f"[血缘服务] ❌ 无法生成唯一的 lineage_id，已达到最大重试次数 {max_retries}")
                raise Exception(f"无法生成唯一的 lineage_id")
            
            new_count += 1
            logger.debug(f"[血缘服务] 生成新表血缘ID: {source_table} -> {tgt_table} (ID: {lineage_id})")
            
            table_data['table_lineage'].append({
                'id': lineage_id,
                'source_table': source_table,
                'source_table_name': tl.get('source_table_name', ''),
                'target_table': tgt_table,
                'target_table_name': tl.get('target_table_name', ''),
                'create_time': get_now_utc8(),  # ⚠️ 直接使用工具函数
                'modify_time': get_now_utc8(),
                'is_exists': False  # ⚠️ 标记为新记录
            })
        
        logger.info(f"[血缘服务] table_lineage 收集完成: 新增 {new_count} 条, 复用 {skipped_count} 条已存在记录")
    
    def _dedup_and_normalize_field_lineage(
        self, 
        ai_field_lineage: List[Dict], 
        target_table: str
    ) -> List[Dict]:
        """
        对字段血缘数据进行去重和空值填充
        
        Args:
            ai_field_lineage: AI 输出的字段血缘列表
            target_table: 目标表名
            
        Returns:
            去重并规范化后的字段血缘列表
        """
        # ⚠️ 第一步：去重 - 基于 (source_table, source_field, target_table, target_field)
        seen_keys = set()
        unique_field_lineage = []
        duplicate_before_count = 0
        
        for fl in ai_field_lineage:
            source_table = fl.get('source_table') or ''
            source_field = fl.get('source_field') or ''
            tgt_table = fl.get('target_table', target_table)
            target_field = fl.get('target_field', '')
            
            # 构建去重键
            dedup_key = (source_table, source_field, tgt_table, target_field)
            
            if dedup_key not in seen_keys:
                seen_keys.add(dedup_key)
                unique_field_lineage.append(fl)
            else:
                duplicate_before_count += 1
                logger.debug(f"[血缘服务] ⚠️ AI 输出中发现重复字段血缘: {dedup_key}")
        
        if duplicate_before_count > 0:
            logger.info(f"[血缘服务] ⚠️ AI 输出中去重: 原始 {len(ai_field_lineage)} 条, 去重后 {len(unique_field_lineage)} 条, 跳过 {duplicate_before_count} 条重复")
            logger.info(f"[血缘服务] 跳过重复字段血缘: {seen_keys}")
        
        # ⚠️ 第二步：空值填充 - 处理 source_table 和 source_field 为空的情况
        normalized_field_lineage = []
        
        for fl in unique_field_lineage:
            source_table = fl.get('source_table') or ''
            source_field = fl.get('source_field') or ''
            tgt_table = fl.get('target_table', target_table)
            target_field = fl.get('target_field', '')
            
            # 跳过没有 target_field 的记录
            if not target_field:
                continue
            
            # ⚠️ 如果 source_table 或 source_field 为空，用 target 的值填充（常量字段、系统函数等场景）
            if not source_table:
                source_table = tgt_table
                logger.debug(f"[血缘服务] source_table 为空，使用 target_table: {source_table}")
            if not source_field:
                source_field = target_field
                logger.debug(f"[血缘服务] source_field 为空，使用 target_field: {source_field}")
            
            # 处理 source_table 为空的情况：从 source_field 中解析表名
            if not source_table and source_field:
                field_parts = [f.strip() for f in source_field.split(',') if f.strip()]
                if field_parts:
                    first_field = field_parts[0]
                    if '.' in first_field:
                        parts = first_field.rsplit('.', 2)
                        if len(parts) >= 2:
                            source_table = '.'.join(parts[:-1])
                            logger.debug(f"[血缘服务] 从 source_field 解析出 source_table: {source_table}")
            
            # ⚠️ 如果 source_table 包含多个表（逗号分隔），拆分为多条记录
            if source_table and ',' in source_table:
                tables = [t.strip() for t in source_table.split(',') if t.strip()]
                logger.debug(f"[血缘服务] source_table 包含多个表，拆分为: {tables}")
                for tbl in tables:
                    normalized_fl = fl.copy()
                    normalized_fl['source_table'] = tbl
                    normalized_fl['source_field'] = source_field
                    normalized_fl['target_table'] = tgt_table
                    normalized_field_lineage.append(normalized_fl)
            else:
                # 单个表，直接添加
                normalized_fl = fl.copy()
                normalized_fl['source_table'] = source_table
                normalized_fl['source_field'] = source_field
                normalized_fl['target_table'] = tgt_table
                normalized_field_lineage.append(normalized_fl)
        
        logger.info(f"[血缘服务] 字段血缘数据规范化完成: {len(normalized_field_lineage)} 条")
        return normalized_field_lineage
    
    def _collect_field_lineage(self, parsed_data: Dict[str, Any], target_table: str, table_data: Dict[str, List], now=None):
        """
        收集字段级血缘数据
        
        Args:
            parsed_data: AI 解析后的数据
            target_table: 目标表名
            table_data: 表数据收集字典
            now: 当前时间（UTC+8）
        """
        ai_field_lineage = parsed_data.get('field_lineage', [])
        logger.info(f"[血缘服务] AI 输出 field_lineage 数量: {len(ai_field_lineage)}")
        
        # ⚠️ 去重和空值填充
        normalized_field_lineage = self._dedup_and_normalize_field_lineage(ai_field_lineage, target_table)
        
        # ⚠️ 批量查询数据库中已有的字段血缘记录（使用工具类）
        # 构建 (source_table, target_table) 组合列表，避免笛卡尔积查询
        # ⚠️ 只添加 source_table 和 target_table 都不为空的记录
        table_pairs = list(set([
            (fl.get('source_table') or '', fl.get('target_table', target_table))
            for fl in normalized_field_lineage
            if (fl.get('source_table') or '') and (fl.get('target_table', target_table))
        ]))
        existing_field_lineage = BatchQueryHelper.query_existing_field_lineage(self.session, table_pairs)
        
        # ⚠️ 遍历 normalized_field_lineage，查找对应的 table_lineage_id 并组装 field_lineage 数据
        # ⚠️ 使用 seen_business_keys 防止同一批数据中的重复
        seen_business_keys = set()
                
        for fl in normalized_field_lineage:
            source_table = fl.get('source_table') or ''  # ⚠️ None 转为空字符串
            source_table_name = fl.get('source_table_name') or ''  # ⚠️ 新增，None 转为空字符串
            source_field = fl.get('source_field') or ''  # ⚠️ None 转为空字符串
            source_field_name = fl.get('source_field_name') or ''  # ⚠️ 新增，None 转为空字符串
            tgt_table = fl.get('target_table', target_table)
            target_table_name = fl.get('target_table_name') or ''  # ⚠️ 新增，None 转为空字符串
            target_field = fl.get('target_field', '')
            target_field_name = fl.get('target_field_name') or ''  # ⚠️ 新增，None 转为空字符串
            target_field_mark = fl.get('target_field_mark', 'normal')  # ⚠️ 默认为 normal
            dim_id = fl.get('dim_id')  # ⚠️ 可能为空
            formula = fl.get('formula', '')
                    
            # ⚠️ 从 table_data['table_lineage'] 中查找对应的 lineage_id
            logger.debug(f"[血缘服务] 🔍 查找表血缘: source_table='{source_table}', target_table='{tgt_table}'")
                    
            # ⚠️ 直接遍历 table_data['table_lineage'] 查找匹配的记录（source_table 已经是单个表）
            table_lineage_id = ''
            for tl in table_data['table_lineage']:
                if tl['source_table'] == source_table and tl['target_table'] == tgt_table:
                    table_lineage_id = tl['id']  # ⚠️ 改为 id
                    logger.debug(f"[血缘服务]   ✅ 找到匹配: {source_table} -> {tgt_table} (ID: {table_lineage_id})")
                    break
                    
            # ⚠️ 只跳过“找不到表血缘”的情况
            if not table_lineage_id:
                # ⚠️ 记录失败的数据（不添加到 table_data['field_lineage']）
                if 'failed_table_lineage' not in table_data:
                    table_data['failed_table_lineage'] = []
                table_data['failed_table_lineage'].append({
                    'reason': '未找到表血缘',
                    'source_table': source_table,
                    'source_field': source_field,
                    'target_table': tgt_table,
                    'target_field': target_field,
                    'target_field_mark': target_field_mark,
                    'dim_id': dim_id,
                    'formula': formula
                })
                continue  # ⚠️ 跳过这条记录
        
            # 构建业务唯一键（包含 table_lineage_id）
            business_key_with_tl = (table_lineage_id, source_table, source_field, tgt_table, target_field)
                    
            # ⚠️ 检查是否在当前批次中已经处理过
            if business_key_with_tl in seen_business_keys:
                logger.warning(f"[血缘服务] ⚠️ 当前批次中发现重复字段血缘: {business_key_with_tl}")
                continue
            seen_business_keys.add(business_key_with_tl)
                    
            # 检查数据库中是否已存在
            business_key = (source_table, source_field, tgt_table, target_field)
            if business_key in existing_field_lineage:
                lineage_id = existing_field_lineage[business_key]
                logger.debug(f"[血缘服务] 复用已有字段血缘ID: {business_key} (ID: {lineage_id})")
            else:
                # 生成新的 lineage_id（使用 IdGenerator）
                lineage_id = self.field_lineage_id_gen.get_next_id()
                logger.debug(f"[血缘服务] 生成新字段血缘ID: {business_key} (ID: {lineage_id})")
                    
            table_data['field_lineage'].append({
                'id': lineage_id,  # ⚠️ 改为 id
                'table_lineage_id': table_lineage_id,
                'source_table': source_table,
                'source_table_name': source_table_name,  # ⚠️ 新增
                'source_field': source_field,
                'source_field_name': source_field_name,  # ⚠️ 新增
                'target_table': tgt_table,
                'target_table_name': target_table_name,  # ⚠️ 新增
                'target_field': target_field,
                'target_field_name': target_field_name,  # ⚠️ 新增
                'target_field_mark': target_field_mark,  # ⚠️ 添加 target_field_mark
                'dim_id': dim_id,  # ⚠️ 添加 dim_id
                'formula': formula,
                'create_time': now,
                'modify_time': now
            })
        
        logger.info(f"[血缘服务] field_lineage 收集完成: {len(table_data['field_lineage'])} 条")
        
        # ⚠️ 输出失败记录统计
        if 'failed_table_lineage' in table_data and table_data['failed_table_lineage']:
            failed_count = len(table_data['failed_table_lineage'])
            logger.warning(f"[血缘服务] ⚠️ 共有 {failed_count} 条字段血缘因未找到表血缘")
            for failed in table_data['failed_table_lineage']:
                logger.warning(
                    f"[血缘服务-未找到表血缘]   - {failed['source_table']}.{failed['source_field']} -> "
                    f"{failed['target_table']}.{failed['target_field']} "
                    f"(target_field_mark: {failed['target_field_mark']}, dim_id: {failed['dim_id']})"
                )
    
    def _extract_target_table_from_sql(self, sql_content: str) -> str:
        """
        从 SQL 中提取目标表名
        
        支持的模式：
        1. INSERT INTO table_name ...
        2. TRUNCATE TABLE table_name
        3. CREATE TABLE table_name
        
        Args:
            sql_content: SQL 内容
            
        Returns:
            目标表名，如果提取失败则返回空字符串
        """
        import re
        
        if not sql_content:
            return ''
        
        # 移除注释
        sql_clean = re.sub(r'--.*?$', '', sql_content, flags=re.MULTILINE)
        sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)
        
        # 模式1：INSERT INTO [schema.]table_name
        insert_match = re.search(r'\bINSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_.]*)', sql_clean, re.IGNORECASE)
        if insert_match:
            return insert_match.group(1)
        
        # 模式2：TRUNCATE TABLE [schema.]table_name
        truncate_match = re.search(r'\bTRUNCATE\s+(?:TABLE\s+)?([a-zA-Z_][a-zA-Z0-9_.]*)', sql_clean, re.IGNORECASE)
        if truncate_match:
            return truncate_match.group(1)
        
        # 模式3：CREATE TABLE [schema.]table_name
        create_match = re.search(r'\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_.]*)', sql_clean, re.IGNORECASE)
        if create_match:
            return create_match.group(1)
        
        return ''
    
    def _collect_table_lineage_ids(
        self, 
        source_table: str, 
        target_table: str,
        table_to_lineage_id: Dict[Tuple[str, str], str]
    ) -> List[str]:
        """
        收集所有相关的表血缘ID（支持多表关联）- 仅使用内存数据
        
        Args:
            source_table: 源表（可能包含多个表，用逗号分隔）
            target_table: 目标表
            table_to_lineage_id: 表到 lineage_id 的映射
            
        Returns:
            表血缘ID列表
        """
        table_lineage_ids = []
        
        # 1. 如果 source_table 包含多个表（逗号分隔），分别查找
        if source_table and ',' in source_table:
            tables = [t.strip() for t in source_table.split(',') if t.strip()]
            for tbl in tables:
                lineage_key = (tbl, target_table)
                tl_id = table_to_lineage_id.get(lineage_key, '')
                if tl_id and tl_id not in table_lineage_ids:
                    table_lineage_ids.append(tl_id)
        # 2. 否则尝试精确匹配
        elif source_table:
            lineage_key = (source_table, target_table)
            exact_match_id = table_to_lineage_id.get(lineage_key, '')
            if exact_match_id:
                table_lineage_ids.append(exact_match_id)
        
        return table_lineage_ids
    
    def _execute_insert(self, table_data: Dict[str, List]) -> Dict[str, Any]:
        """
        执行数据库插入操作（使用 SqlGenerator）
        
        Args:
            table_data: 表数据收集字典
            
        Returns:
            执行结果
        """
        try:
            # 定义各表的配置
            table_configs = {
                'table_lineage': {
                    'columns': ['id', 'source_table', 'source_table_name', 'target_table', 'target_table_name', 'create_time', 'modify_time'],
                    'conflict_target': '(source_table, target_table)'
                },
                'field_lineage': {
                    'columns': ['id', 'table_lineage_id', 'source_table', 'source_table_name', 'source_field', 'source_field_name', 'target_table', 'target_table_name', 'target_field', 'target_field_name', 'target_field_mark', 'dim_id', 'formula', 'create_time', 'modify_time'],
                    'conflict_target': '(source_table, source_field, target_table, target_field)'
                }
            }
            
            table_stats = {}
            
            # 批量插入 table_lineage（使用 SqlGenerator）
            # ⚠️ 过滤掉 is_exists=True 的记录（已存在于数据库）
            new_table_lineage = [item for item in table_data['table_lineage'] if not item.get('is_exists', False)]
            
            if new_table_lineage:
                config = table_configs['table_lineage']
                sql = SqlGenerator.generate_batch_upsert('table_lineage', config, new_table_lineage)
                if sql:
                    result = self.session.execute(text(sql))
                    inserted_count = result.rowcount if result.rowcount is not None else len(new_table_lineage)
                    skipped_count = len(new_table_lineage) - inserted_count
                    
                    table_stats['table_lineage'] = inserted_count
                    logger.info(f"[血缘服务] 批量插入 table_lineage: {inserted_count} 条新增, {skipped_count} 条已存在跳过")
            else:
                table_stats['table_lineage'] = 0
                logger.info(f"[血缘服务] 无需插入 table_lineage（所有记录均已存在）")
            
            # 批量插入 field_lineage（使用 SqlGenerator）
            if table_data['field_lineage']:
                config = table_configs['field_lineage']
                sql = SqlGenerator.generate_batch_upsert('field_lineage', config, table_data['field_lineage'])
                if sql:
                    self.session.execute(text(sql))
                    table_stats['field_lineage'] = len(table_data['field_lineage'])
                    logger.info(f"[血缘服务] 批量插入 field_lineage: {len(table_data['field_lineage'])} 条")
            
            # 提交事务
            self.session.commit()
            
            return {
                'success': True,
                'table_stats': table_stats
            }
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"[血缘服务] 数据库插入失败: {str(e)}", exc_info=True)
            raise
