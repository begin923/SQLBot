"""
血缘数据缓存 - 避免重复数据库查询
"""

import logging
from typing import Dict, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


class LineageCache:
    """
    血缘数据全局缓存
    
    作用：
    1. 一次性加载所有已存在的 table_lineage 和 field_lineage
    2. 避免每个文件都单独查询数据库
    3. 显著提升批量处理性能
    
    使用示例：
    ```python
    cache = LineageCache()
    cache.load_all(session)  # 一次性加载
    
    # 后续查询直接从缓存获取，无需查库
    lineage_id = cache.get_table_lineage_id('source_table', 'target_table')
    ```
    """
    
    _instance = None
    
    def __new__(cls):
        """单例模式，确保全局只有一个缓存实例"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.existing_table_lineage: Dict[Tuple[str, str], str] = {}
        self.existing_field_lineage: Dict[Tuple[str, str, str, str], str] = {}
        self.is_loaded = False
        self._initialized = True
        
        logger.info("[LineageCache] 缓存实例已创建")
    
    def load_all(self, session: Session):
        """
        一次性加载所有已存在的血缘记录到缓存
        
        Args:
            session: 数据库会话
        """
        if self.is_loaded:
            logger.debug("[LineageCache] 缓存已加载，跳过")
            return
        
        try:
            logger.info("[LineageCache] 开始加载缓存...")
            
            # 1. 加载所有 table_lineage
            result = session.execute(
                text("SELECT source_table, target_table, id FROM table_lineage")
            ).fetchall()
            
            self.existing_table_lineage = {
                (row[0], row[1]): row[2] 
                for row in result
            }
            
            logger.info(f"[LineageCache] ✅ 已加载 {len(self.existing_table_lineage)} 条表血缘记录")
            
            # 2. 加载所有 field_lineage
            result = session.execute(
                text("SELECT source_table, source_field, target_table, target_field, id FROM field_lineage")
            ).fetchall()
            
            self.existing_field_lineage = {
                (row[0], row[1], row[2], row[3]): row[4] 
                for row in result
            }
            
            logger.info(f"[LineageCache] ✅ 已加载 {len(self.existing_field_lineage)} 条字段血缘记录")
            
            self.is_loaded = True
            logger.info(f"[LineageCache] 🎉 缓存加载完成！总计 {len(self.existing_table_lineage) + len(self.existing_field_lineage)} 条记录")
            
        except Exception as e:
            logger.error(f"[LineageCache] ❌ 缓存加载失败: {str(e)}", exc_info=True)
            # 不抛出异常，允许降级到直接查询
            self.is_loaded = False
    
    def get_table_lineage_id(self, source_table: str, target_table: str) -> str:
        """
        从缓存中获取表血缘 ID
        
        Args:
            source_table: 源表名
            target_table: 目标表名
            
        Returns:
            lineage_id 或 None
        """
        key = (source_table, target_table)
        return self.existing_table_lineage.get(key)
    
    def get_field_lineage_id(
        self, 
        source_table: str, 
        source_field: str, 
        target_table: str, 
        target_field: str
    ) -> str:
        """
        从缓存中获取字段血缘 ID
        
        Args:
            source_table: 源表名
            source_field: 源字段名
            target_table: 目标表名
            target_field: 目标字段名
            
        Returns:
            lineage_id 或 None
        """
        key = (source_table, source_field, target_table, target_field)
        return self.existing_field_lineage.get(key)
    
    def add_table_lineage(self, source_table: str, target_table: str, lineage_id: str):
        """
        添加新的表血缘记录到缓存
        
        Args:
            source_table: 源表名
            target_table: 目标表名
            lineage_id: 血缘ID
        """
        key = (source_table, target_table)
        self.existing_table_lineage[key] = lineage_id
        logger.debug(f"[LineageCache] 添加表血缘: {key} -> {lineage_id}")
    
    def add_field_lineage(
        self, 
        source_table: str, 
        source_field: str, 
        target_table: str, 
        target_field: str, 
        lineage_id: str
    ):
        """
        添加新的字段血缘记录到缓存
        
        Args:
            source_table: 源表名
            source_field: 源字段名
            target_table: 目标表名
            target_field: 目标字段名
            lineage_id: 血缘ID
        """
        key = (source_table, source_field, target_table, target_field)
        self.existing_field_lineage[key] = lineage_id
        logger.debug(f"[LineageCache] 添加字段血缘: {key} -> {lineage_id}")
    
    def clear(self):
        """清空缓存（用于测试或重新加载）"""
        self.existing_table_lineage.clear()
        self.existing_field_lineage.clear()
        self.is_loaded = False
        logger.warning("[LineageCache] 缓存已清空")
    
    def get_stats(self) -> Dict[str, int]:
        """获取缓存统计信息"""
        return {
            'table_lineage_count': len(self.existing_table_lineage),
            'field_lineage_count': len(self.existing_field_lineage),
            'is_loaded': self.is_loaded
        }
