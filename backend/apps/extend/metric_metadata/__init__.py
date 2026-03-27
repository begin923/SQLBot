"""
指标元数据管理模块

提供指标元数据的增删改查功能，支持：
1. 单个/批量创建指标元数据
2. 自动计算 embedding 向量用于语义检索
3. 数仓分层管理（ODS/DWD/DWS/ADS）
4. 指标血缘关系追踪
"""

from apps.extend.metric_metadata.api.metric_metadata import router as metric_metadata_router

__all__ = ["metric_metadata_router"]
