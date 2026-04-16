"""
批量查询工具类 - 优化数据库批量查询操作
"""

from typing import List, Dict, Any, Optional, Tuple, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text


class BatchQueryHelper:
    """
    批量查询助手，提供优化的批量查询功能

    使用示例：
    ```python
    # 批量查询维度ID
    dim_codes = ['field_id', 'parity_no']
    existing_dims = BatchQueryHelper.batch_get_ids(session, 'dim_definition', 'code', dim_codes)

    # 批量查询表血缘
    source_tables = ['source_table1', 'source_table2']
    target_tables = ['target_table1', 'target_table2']
    existing_table_lineage = BatchQueryHelper.batch_get_table_lineage(session, source_tables, target_tables)
    ```
    """

    @staticmethod
    def query_existing_ids(
        session: Session, 
        table_name: str, 
        lookup_column: str, 
        values: List[str]
    ) -> Dict[str, str]:
        """
        批量查询已存在的记录ID（通用方法）
        
        例如：查询 dim_definition 表中 code 为 ['field_id', 'parity_no'] 的记录ID

        Args:
            session: 数据库会话
            table_name: 表名
            lookup_column: 查找列名（如 'code', 'metric_en'）
            values: 要查询的值列表

        Returns:
            {查找列值: ID} 字典
        """
        if not values:
            return {}

        try:
            result = session.execute(
                text(f"SELECT {lookup_column}, id FROM {table_name} WHERE {lookup_column} IN :values"),
                {"values": tuple(values)}
            ).fetchall()

            return {row[0]: row[1] for row in result}
        except Exception as e:
            print(f"[BatchQueryHelper] 批量查询失败: {str(e)}")
            return {}

    @staticmethod
    def query_existing_table_lineage(
        session: Session,
        source_tables: List[str],
        target_tables: List[str]
    ) -> Dict[Tuple[str, str], str]:
        """
        批量查询已存在的表血缘记录
        
        例如：查询 (source_table1, target_table1) 和 (source_table2, target_table2) 的血缘ID

        Args:
            session: 数据库会话
            source_tables: 源表列表
            target_tables: 目标表列表

        Returns:
            {(source_table, target_table): lineage_id} 字典
        """
        if not source_tables or not target_tables:
            return {}

        try:
            # 去重
            unique_sources = list(set(source_tables))
            unique_targets = list(set(target_tables))

            result = session.execute(
                text("SELECT id, source_table, target_table FROM table_lineage WHERE source_table IN :sources AND target_table IN :targets"),
                {"sources": tuple(unique_sources), "targets": tuple(unique_targets)}
            ).fetchall()

            existing = {}
            for row in result:
                key = (row[1], row[2])
                existing[key] = row[0]

            return existing
        except Exception as e:
            print(f"[BatchQueryHelper] 批量表血缘查询失败: {str(e)}")
            return {}

    @staticmethod
    def query_existing_field_lineage(
        session: Session,
        table_pairs: List[Tuple[str, str]]
    ) -> Dict[Tuple[str, str, str, str], str]:
        """
        批量查询已存在的字段血缘记录
        
        例如：查询 (source_table1, source_field1, target_table1, target_field1) 的血缘ID

        Args:
            session: 数据库会话
            table_pairs: (source_table, target_table) 组合列表

        Returns:
            {(source_table, source_field, target_table, target_field): lineage_id} 字典
        """
        if not table_pairs:
            return {}

        try:
            # 构建参数和SQL
            values_list = []
            params = {}
            for idx, (src_tbl, tgt_tbl) in enumerate(table_pairs):
                param_src = f'src_{idx}'
                param_tgt = f'tgt_{idx}'
                values_list.append(f'(:{param_src}, :{param_tgt})')
                params[param_src] = src_tbl
                params[param_tgt] = tgt_tbl

            values_clause = ', '.join(values_list)
            sql = f"SELECT id, source_table, source_field, target_table, target_field FROM field_lineage WHERE (source_table, target_table) IN ({values_clause})"

            result = session.execute(text(sql), params).fetchall()

            existing = {}
            for row in result:
                key = (row[1], row[2], row[3], row[4])
                existing[key] = row[0]

            return existing
        except Exception as e:
            print(f"[BatchQueryHelper] 批量字段血缘查询失败: {str(e)}")
            return {}

    @staticmethod
    def query_existing_metric_ids(
        session: Session,
        metric_codes: List[str]
    ) -> Dict[str, str]:
        """
        批量查询已存在的指标ID（通过 code 字段）
        
        例如：查询 code 为 ['user_count', 'order_amount'] 的指标ID

        Args:
            session: 数据库会话
            metric_codes: 指标编码列表（对应数据库的 code 字段）

        Returns:
            {code: id} 字典
        """
        if not metric_codes:
            return {}

        try:
            result = session.execute(
                text("SELECT code, id FROM metric_definition WHERE code IN :codes"),  # ⚠️ 改为新字段名
                {"codes": tuple(metric_codes)}
            ).fetchall()

            existing_metrics = {row[0]: row[1] for row in result}
            return existing_metrics
        except Exception as e:
            print(f"[BatchQueryHelper] 批量查询指标ID失败: {str(e)}")
            return {}

    @staticmethod
    def query_existing_dim_ids(
        session: Session,
        dim_codes: List[str]
    ) -> Dict[str, str]:
        """
        批量查询已存在的维度ID（通过 code 字段）
        
        例如：查询 code 为 ['field_id', 'parity_no'] 的维度ID

        Args:
            session: 数据库会话
            dim_codes: 维度编码列表（对应数据库的 code 字段）

        Returns:
            {code: id} 字典
        """
        if not dim_codes:
            return {}

        try:
            result = session.execute(
                text("SELECT code, id FROM dim_definition WHERE code IN :codes"),
                {"codes": tuple(dim_codes)}
            ).fetchall()

            existing_dims = {row[0]: row[1] for row in result}
            return existing_dims
        except Exception as e:
            print(f"[BatchQueryHelper] 批量查询维度ID失败: {str(e)}")
            return {}