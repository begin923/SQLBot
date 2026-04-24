"""
ID生成器工具类 - 统一管理各种ID的生成逻辑
"""

from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import text


class IdGenerator:
    """
    统一的ID生成器，支持各种类型的ID生成（表血缘ID、字段血缘ID、维度ID、指标ID等）

    使用示例：
    ```python
    id_gen = IdGenerator(session, 'table_lineage', 'T')
    new_id = id_gen.get_next_id()
    ```
    """

    def __init__(self, session: Session, table_name: str, prefix: str):
        """
        初始化ID生成器

        Args:
            session: 数据库会话
            table_name: 表名（用于查询最大ID）
            prefix: ID前缀（如 'T' 表示表血缘，'F' 表示字段血缘）
        """
        self.session = session
        self.table_name = table_name
        self.prefix = prefix
        self.counter = self._load_max_id()

    def _load_max_id(self) -> int:
        """
        从数据库加载当前最大ID值
        ⚠️ 从右往左截取6位数字，兼容任意前缀长度

        Returns:
            最大ID值，如果查询失败则返回0
        """
        try:
            # ⚠️ 使用 RIGHT() 函数从右往左截取6位，兼容任意前缀
            result = self.session.execute(
                text(f"SELECT MAX(CAST(RIGHT(id, 6) AS INTEGER)) FROM {self.table_name}")
            ).scalar()
            return int(result) if result else 0
        except Exception as e:
            # 记录警告但不抛出异常，确保系统可以继续运行
            print(f"[IdGenerator] 查询最大ID失败: {str(e)}，使用默认值0")
            return 0

    def get_next_id(self) -> str:
        """
        生成下一个ID

        Returns:
            新的ID字符串，格式为：前缀 + 6位数字（如 T000001）
        """
        self.counter += 1
        return f"{self.prefix}{self.counter:06d}"

    def reset(self):
        """重置计数器（主要用于测试）"""
        self.counter = self._load_max_id()