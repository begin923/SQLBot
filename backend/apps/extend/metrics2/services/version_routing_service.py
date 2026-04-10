from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
from sqlalchemy.orm import Session

from apps.extend.metrics2.curd import (
    get_metric_source_mapping_by_metric_id,
    get_metric_lineage_by_metric_id,
    get_field_lineage_by_target_field,
    validate_field_exists,
    get_current_version_by_metric_id,
    get_version_by_time
)

logger = logging.getLogger("VersionRoutingService")


class VersionRoutingService:
    """
    版本匹配路由服务 - 核心算法3
    
    功能：
    1. 根据查询时间匹配历史版本
    2. 多源路由（按优先级选择最优数据源）
    3. 字段合法性校验
    4. 自动降级（主源失败时切换备用源）
    """

    def __init__(self, session: Session):
        """
        初始化版本路由服务
        
        Args:
            session: 数据库会话
        """
        self.session = session

    def route_metric_query(self, metric_id: str, query_time: Optional[datetime] = None,
                          dimensions: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        路由指标查询请求
        
        Args:
            metric_id: 指标ID
            query_time: 查询时间（用于版本匹配）
            dimensions: 维度列表
            
        Returns:
            路由结果：包含SQL、数据源、版本信息等
        """
        try:
            # 1. 版本匹配
            version_info = self._match_version(metric_id, query_time)

            # 2. 获取所有映射源
            source_mappings = get_metric_source_mapping_by_metric_id(self.session, metric_id)

            if not source_mappings:
                return {
                    'success': False,
                    'message': f'指标 {metric_id} 无可用数据源'
                }

            # 3. 按优先级排序
            sorted_mappings = sorted(source_mappings, key=lambda x: x.priority if hasattr(x, 'priority') else 999)

            # 4. 选择最优源并校验
            selected_source = None
            validation_errors = []

            for mapping in sorted_mappings:
                # 字段合法性校验
                is_valid = self._validate_source_field(
                    mapping.db_table,
                    mapping.metric_column
                )

                if is_valid:
                    selected_source = mapping
                    break
                else:
                    validation_errors.append({
                        'source': mapping.datasource,
                        'table': mapping.db_table,
                        'field': mapping.metric_column,
                        'error': '字段不存在于血缘映射中'
                    })

            if not selected_source:
                return {
                    'success': False,
                    'message': '所有数据源字段校验失败',
                    'validation_errors': validation_errors
                }

            # 5. 生成SQL
            sql = self._generate_sql(metric_id, selected_source, dimensions)

            return {
                'success': True,
                'sql': sql,
                'source': {
                    'datasource': selected_source.datasource,
                    'db_table': selected_source.db_table,
                    'metric_column': selected_source.metric_column,
                    'priority': selected_source.priority
                },
                'version': version_info,
                'dimensions': dimensions or []
            }

        except Exception as e:
            logger.error(f"路由指标查询失败：{str(e)}")
            return {
                'success': False,
                'message': f'路由失败：{str(e)}'
            }

    def _match_version(self, metric_id: str, query_time: Optional[datetime]) -> Optional[Dict[str, Any]]:
        """
        匹配指标版本
        
        Args:
            metric_id: 指标ID
            query_time: 查询时间
            
        Returns:
            版本信息
        """
        if query_time:
            # 按时间匹配历史版本
            version = get_version_by_time(self.session, metric_id, query_time)
        else:
            # 获取当前版本
            version = get_current_version_by_metric_id(self.session, metric_id)

        if version:
            return {
                'version_id': version.version_id,
                'version': version.version,
                'cal_logic': version.cal_logic,
                'effective_time': version.effective_time,
                'expire_time': version.expire_time
            }

        return None

    def _validate_source_field(self, db_table: str, metric_column: str) -> bool:
        """
        校验源字段合法性
        
        Args:
            db_table: 物理表名
            metric_column: 指标字段名
            
        Returns:
            是否合法
        """
        return validate_field_exists(self.session, db_table, metric_column)

    def _generate_sql(self, metric_id: str, source_mapping,
                     dimensions: Optional[List[str]] = None) -> str:
        """
        生成查询SQL
        
        Args:
            metric_id: 指标ID
            source_mapping: 源映射信息
            dimensions: 维度列表
            
        Returns:
            SQL语句
        """
        # 基础SELECT
        select_fields = [f"{source_mapping.metric_column} AS metric_value"]

        # 添加维度字段
        if dimensions:
            for dim in dimensions:
                select_fields.append(dim)

        select_clause = ', '.join(select_fields)

        # FROM子句
        from_clause = source_mapping.db_table

        # WHERE子句（筛选条件）
        where_clause = ''
        if source_mapping.filter_condition:
            where_clause = f"WHERE {source_mapping.filter_condition}"

        # GROUP BY（聚合）
        group_by_clause = ''
        if dimensions and source_mapping.agg_func:
            group_by_clause = f"GROUP BY {', '.join(dimensions)}"

        # 聚合函数
        if source_mapping.agg_func:
            select_clause = f"{source_mapping.agg_func}({source_mapping.metric_column}) AS metric_value"
            if dimensions:
                select_clause += ', ' + ', '.join(dimensions)

        sql = f"SELECT {select_clause} FROM {from_clause} {where_clause} {group_by_clause}".strip()

        return sql

    def auto_fallback(self, metric_id: str, failed_source: str,
                     query_time: Optional[datetime] = None) -> Dict[str, Any]:
        """
        自动降级（主源失败时切换备用源）
        
        Args:
            metric_id: 指标ID
            failed_source: 失败的数据源
            query_time: 查询时间
            
        Returns:
            降级后的路由结果
        """
        try:
            # 获取所有映射源
            source_mappings = get_metric_source_mapping_by_metric_id(self.session, metric_id)

            # 过滤掉失败的源，并按优先级排序
            available_mappings = [
                m for m in source_mappings
                if m.datasource != failed_source and m.source_level != 'AUTHORITY'
            ]
            available_mappings.sort(key=lambda x: x.priority if hasattr(x, 'priority') else 999)

            if not available_mappings:
                return {
                    'success': False,
                    'message': '无可用备用数据源'
                }

            # 选择备用源
            fallback_source = available_mappings[0]

            # 校验备用源
            is_valid = self._validate_source_field(
                fallback_source.db_table,
                fallback_source.metric_column
            )

            if not is_valid:
                return {
                    'success': False,
                    'message': f'备用源字段校验失败：{fallback_source.db_table}.{fallback_source.metric_column}'
                }

            # 生成SQL
            sql = self._generate_sql(metric_id, fallback_source)

            logger.info(f"自动降级成功：{failed_source} -> {fallback_source.datasource}")

            return {
                'success': True,
                'sql': sql,
                'source': {
                    'datasource': fallback_source.datasource,
                    'db_table': fallback_source.db_table,
                    'metric_column': fallback_source.metric_column,
                    'priority': fallback_source.priority,
                    'is_fallback': True
                },
                'original_source': failed_source
            }

        except Exception as e:
            logger.error(f"自动降级失败：{str(e)}")
            return {
                'success': False,
                'message': f'降级失败：{str(e)}'
            }

    def get_metric_sources(self, metric_id: str) -> List[Dict[str, Any]]:
        """
        获取指标的所有数据源
        
        Args:
            metric_id: 指标ID
            
        Returns:
            数据源列表
        """
        source_mappings = get_metric_source_mapping_by_metric_id(self.session, metric_id)

        sources = []
        for mapping in source_mappings:
            sources.append({
                'datasource': mapping.datasource,
                'db_table': mapping.db_table,
                'metric_column': mapping.metric_column,
                'priority': mapping.priority,
                'source_level': mapping.source_level,
                'is_valid': self._validate_source_field(mapping.db_table, mapping.metric_column)
            })

        return sources
