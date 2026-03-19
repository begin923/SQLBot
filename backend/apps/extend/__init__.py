"""
Extend 模块
扩展功能集合，包含:
- 静态 SQL 处理器
- 下钻分析处理器
"""
from apps.extend.metric_drilldown_handler import MetricDrilldownHandler
from apps.extend.static_sql_handler import StaticSQLHandler

__all__ = [
    'StaticSQLHandler',
    'MetricDrilldownHandler',
]
