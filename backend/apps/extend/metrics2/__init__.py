"""
养殖业务指标平台核心模块

提供指标平台的核心功能，包括：
1. 指标语义定义管理
2. 维度字典管理
3. 多源物理映射管理
4. 复合指标关系管理
5. AI 大模型客户端（复用）
6. 业务服务层（业务逻辑编排）
7. 版本管理层
8. 双血缘层
9. 核心算法服务
"""

from apps.extend.metrics2.api.dim_dict_api import router as dim_dict_router
from apps.extend.metrics2.api.metric_definition_api import router as metric_definition_router
from apps.extend.metrics2.api.metric_dim_rel_api import router as metric_dim_rel_router
from apps.extend.metrics2.api.metric_source_mapping_api import router as metric_source_mapping_router
from apps.extend.metrics2.api.metric_compound_rel_api import router as metric_compound_rel_router

# 版本管理层 API
from apps.extend.metrics2.api.version_management_api import router as version_management_router

# 血缘分析 API
from apps.extend.metrics2.api.lineage_analysis_api import router as lineage_analysis_router

from apps.extend.metrics2.services.metric_service import MetricService
from apps.extend.metrics2.services.dim_field_mapping_service import DimFieldMappingService
from apps.extend.metrics2.services.service_factory import ServiceFactory

__all__ = [
    "dim_dict_router",
    "metric_definition_router",
    "metric_dim_rel_router",
    "metric_source_mapping_router",
    "metric_compound_rel_router",
    "version_management_router",
    "lineage_analysis_router",
    "MetricService",
    "DimFieldMappingService",
    "ServiceFactory"
]