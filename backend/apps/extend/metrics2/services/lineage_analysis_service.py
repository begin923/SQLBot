from typing import List, Dict, Any, Optional, Set
from collections import defaultdict
import logging
from sqlalchemy.orm import Session

from apps.extend.metrics2.curd import (
    get_table_lineage_by_target,
    get_table_lineage_by_source,
    get_field_lineage_by_lineage_id,
    get_all_table_lineage,
    get_all_field_lineage,
    get_dim_field_mapping_by_dim_id,
    get_metric_lineage_by_metric_id,
    get_metric_source_mapping_by_metric_id
)

logger = logging.getLogger("LineageAnalysisService")


class LineageAnalysisService:
    """
    血缘分析服务 - 双血缘强校验核心
    
    功能：
    1. 表级血缘分析（上游/下游）
    2. 字段级血缘追踪
    3. 完整血缘链路查询
    4. 影响分析（下游依赖分析）
    """

    def __init__(self, session: Session):
        """
        初始化血缘分析服务
        
        Args:
            session: 数据库会话
        """
        self.session = session

    def get_upstream_tables(self, target_table: str, max_depth: int = 10) -> List[Dict[str, Any]]:
        """
        获取表的所有上游表（递归）
        
        Args:
            target_table: 目标表
            max_depth: 最大递归深度
            
        Returns:
            上游表列表
        """
        upstream_tables = []
        visited = set()
        self._get_upstream_recursive(target_table, upstream_tables, visited, 0, max_depth)

        return upstream_tables

    def _get_upstream_recursive(self, table: str, result: List[Dict[str, Any]],
                                visited: Set[str], depth: int, max_depth: int):
        """
        递归获取上游表
        
        Args:
            table: 当前表
            result: 结果列表
            visited: 已访问表集合（防循环）
            depth: 当前深度
            max_depth: 最大深度
        """
        if table in visited or depth >= max_depth:
            return

        visited.add(table)

        # 查询直接上游
        lineage_list = get_table_lineage_by_target(self.session, table)

        for lineage in lineage_list:
            source_table = lineage.source_table

            # 获取字段级血缘
            field_lineages = get_field_lineage_by_lineage_id(self.session, lineage.lineage_id)

            result.append({
                'table': source_table,
                'target_table': lineage.target_table,
                'depth': depth + 1,
                'field_mappings': [
                    {
                        'source_field': fl.source_field,
                        'target_field': fl.target_field
                    }
                    for fl in field_lineages
                ]
            })

            # 递归查询上游
            self._get_upstream_recursive(source_table, result, visited, depth + 1, max_depth)

    def get_downstream_tables(self, source_table: str, max_depth: int = 10) -> List[Dict[str, Any]]:
        """
        获取表的所有下游表（影响分析）
        
        Args:
            source_table: 源表
            max_depth: 最大递归深度
            
        Returns:
            下游表列表
        """
        downstream_tables = []
        visited = set()
        self._get_downstream_recursive(source_table, downstream_tables, visited, 0, max_depth)

        return downstream_tables

    def _get_downstream_recursive(self, table: str, result: List[Dict[str, Any]],
                                  visited: Set[str], depth: int, max_depth: int):
        """
        递归获取下游表
        
        Args:
            table: 当前表
            result: 结果列表
            visited: 已访问表集合
            depth: 当前深度
            max_depth: 最大深度
        """
        if table in visited or depth >= max_depth:
            return

        visited.add(table)

        # 查询直接下游
        lineage_list = get_table_lineage_by_source(self.session, table)

        for lineage in lineage_list:
            target_table = lineage.target_table

            result.append({
                'table': target_table,
                'source_table': lineage.source_table,
                'depth': depth + 1
            })

            # 递归查询下游
            self._get_downstream_recursive(target_table, result, visited, depth + 1, max_depth)

    def get_full_lineage(self, metric_id: str) -> Dict[str, Any]:
        """
        获取指标的完整血缘链路
        
        Args:
            metric_id: 指标ID
            
        Returns:
            完整血缘信息
        """
        # 1. 获取指标的映射源
        source_mappings = get_metric_source_mapping_by_metric_id(self.session, metric_id)

        # 2. 获取指标血缘
        metric_lineages = get_metric_lineage_by_metric_id(self.session, metric_id)

        # 3. 构建血缘图
        lineage_graph = {
            'metric_id': metric_id,
            'source_mappings': [],
            'table_lineage': [],
            'field_lineage': []
        }

        for mapping in source_mappings:
            lineage_graph['source_mappings'].append({
                'map_id': mapping.map_id,
                'datasource': mapping.datasource,
                'db_table': mapping.db_table,
                'metric_column': mapping.metric_column
            })

            # 查询表级血缘
            table_lineages = get_table_lineage_by_target(self.session, mapping.db_table)

            for tl in table_lineages:
                lineage_graph['table_lineage'].append({
                    'source_table': tl.source_table,
                    'target_table': tl.target_table
                })

                # 查询字段级血缘
                field_lineages = get_field_lineage_by_lineage_id(self.session, tl.lineage_id)
                for fl in field_lineages:
                    lineage_graph['field_lineage'].append({
                        'source_table': fl.source_table,
                        'source_field': fl.source_field,
                        'target_table': fl.target_table,
                        'target_field': fl.target_field
                    })

        return lineage_graph

    def get_dimension_lineage(self, dim_id: str) -> List[Dict[str, Any]]:
        """
        获取维度的血缘映射
        
        Args:
            dim_id: 维度ID
            
        Returns:
            维度血缘列表
        """
        dim_lineages = get_dim_field_mapping_by_dim_id(self.session, dim_id)

        result = []
        for lineage in dim_lineages:
            result.append({
                'dim_id': lineage.dim_id,
                'db_table': lineage.db_table,
                'dim_field': lineage.dim_field
            })

        return result

    def get_impact_analysis(self, table_name: str) -> Dict[str, Any]:
        """
        影响分析：分析表变更会影响哪些指标
        
        Args:
            table_name: 表名
            
        Returns:
            影响分析结果
        """
        # 1. 获取所有下游表
        downstream_tables = self.get_downstream_tables(table_name)

        affected_tables = [table_name] + [dt['table'] for dt in downstream_tables]

        # 2. 查询这些表对应的指标
        all_mappings = get_metric_source_mapping_by_metric_id(self.session, '')

        affected_metrics = []
        for mapping in all_mappings:
            if mapping.db_table in affected_tables:
                affected_metrics.append({
                    'metric_id': mapping.metric_id,
                    'db_table': mapping.db_table,
                    'metric_column': mapping.metric_column,
                    'affected': mapping.db_table == table_name  # 直接影响还是间接影响
                })

        return {
            'source_table': table_name,
            'affected_tables': affected_tables,
            'affected_metrics': affected_metrics,
            'total_affected_metrics': len(affected_metrics)
        }

    def build_lineage_graph(self) -> Dict[str, Any]:
        """
        构建完整血缘图
        
        Returns:
            血缘图（邻接表）
        """
        all_table_lineages = get_all_table_lineage(self.session)

        graph = defaultdict(list)
        for lineage in all_table_lineages:
            graph[lineage.source_table].append(lineage.target_table)

        return {
            'nodes': list(set(
                [lt.source_table for lt in all_table_lineages] +
                [lt.target_table for lt in all_table_lineages]
            )),
            'edges': [
                {
                    'source': lt.source_table,
                    'target': lt.target_table
                }
                for lt in all_table_lineages
            ]
        }

    def validate_lineage_completeness(self, metric_id: str) -> Dict[str, Any]:
        """
        校验血缘完整性
        
        Args:
            metric_id: 指标ID
            
        Returns:
            校验结果
        """
        issues = []

        # 1. 检查是否有映射源
        source_mappings = get_metric_source_mapping_by_metric_id(self.session, metric_id)
        if not source_mappings:
            issues.append('指标无映射源')

        # 2. 检查每个映射源的字段血缘
        for mapping in source_mappings:
            field_valid = self._check_field_lineage(mapping.db_table, mapping.metric_column)
            if not field_valid:
                issues.append(f"字段 {mapping.db_table}.{mapping.metric_column} 无血缘映射")

        return {
            'metric_id': metric_id,
            'is_complete': len(issues) == 0,
            'issues': issues
        }

    def _check_field_lineage(self, table_name: str, field_name: str) -> bool:
        """
        检查字段是否有血缘映射
        
        Args:
            table_name: 表名
            field_name: 字段名
            
        Returns:
            是否有血缘映射
        """
        all_field_lineages = get_all_field_lineage(self.session)

        for fl in all_field_lineages:
            if fl.target_table == table_name and fl.target_field == field_name:
                return True

        return False
