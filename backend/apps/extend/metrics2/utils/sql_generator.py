"""
SQL生成器工具类 - 优化批量UPSERT SQL生成逻辑
"""

import logging
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)


class SqlGenerator:
    """
    SQL生成器，提供优化的批量UPSERT SQL生成功能

    使用示例：
    ```python
    # 定义表配置
    table_config = {
        'metric_definition': {
            'columns': ['id', 'name', 'code', 'metric_type', 'biz_domain', 'cal_logic', 'unit', 'status'],
            'conflict_target': '(code)'
        }
    }

    # 生成SQL
    sql = SqlGenerator.generate_batch_upsert(table_config['metric_definition'], data_list)
    ```

    支持的表配置格式：
    {
        'columns': ['列1', '列2', ...],  # 要插入的列
        'conflict_target': '(冲突键1, 冲突键2, ...)'  # 冲突时的目标键
    }
    """

    @staticmethod
    def generate_batch_upsert(
        table_name: str,
        table_config: Dict[str, Any], 
        data_list: List[Dict]
    ) -> Optional[str]:
        """
        生成批量UPSERT SQL（INSERT ... ON CONFLICT）
        
        例如：为 metric_definition 表生成 UPSERT SQL，冲突时更新 name 和 status

        Args:
            table_name: 表名（如 'metric_definition'）
            table_config: 表配置字典，包含 columns 和 conflict_target
            data_list: 数据列表

        Returns:
            生成的SQL字符串，如果出错则返回None
        """
        if not data_list:
            return None

        try:
            columns = table_config['columns']
            conflict_target = table_config['conflict_target']

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
                    elif hasattr(value, 'strftime'):  # ⚠️ 处理 datetime 对象
                        values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
                    else:
                        values.append(str(value))
                values_list.append(f"({', '.join(values)})")

            values_str = ',\n            '.join(values_list)

            # 构建 UPDATE 子句
            conflict_columns = [col.strip() for col in conflict_target.strip('()').split(',')]
            primary_key_columns = ['id']  # ⚠️ 所有表的主键统一为 id
            update_columns = [col for col in columns if col not in conflict_columns and col not in primary_key_columns]

            # 如果所有字段都是冲突键，使用 DO NOTHING
            if not update_columns:
                return f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES {values_str}
                ON CONFLICT {conflict_target} DO NOTHING
                """
            else:
                # 否则使用 DO UPDATE SET
                update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
                return f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES {values_str}
                ON CONFLICT {conflict_target} DO UPDATE SET
                    {update_str}
                """

        except Exception as e:
            logger.error(f"[SqlGenerator] 生成{table_name}的UPSERT SQL失败: {str(e)}")
            return None

    @staticmethod
    def generate_batch_insert(table_name: str, columns: List[str], data_list: List[Dict]) -> Optional[str]:
        """
        生成批量INSERT SQL（不带UPSERT）

        Args:
            table_name: 表名
            columns: 列名列表
            data_list: 数据列表

        Returns:
            生成的SQL字符串
        """
        if not data_list:
            return None

        try:
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
                    elif hasattr(value, 'strftime'):  # ⚠️ 处理 datetime 对象
                        values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
                    else:
                        values.append(str(value))
                values_list.append(f"({', '.join(values)})")

            values_str = ',\n            '.join(values_list)
            columns_str = ', '.join(columns)

            return f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES {values_str}
            """

        except Exception as e:
            logger.error(f"[SqlGenerator] 生成{table_name}的INSERT SQL失败: {str(e)}")
            return None