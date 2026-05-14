# ⚠️ 注意：MetricSourceMapping 表已在 apps.extend.metrics.models.metric_source_mapping_model 中定义
# 为避免 SQLAlchemy 表重复定义错误，此处直接从 metrics 模块导入
from apps.extend.metrics.models.metric_source_mapping_model import MetricSourceMapping

from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel


# 本地仅定义 Info 类用于数据传输
class MetricSourceMappingInfo(SQLModel):
    """指标源映射信息对象"""
    id: Optional[str] = None
    metric_id: Optional[str] = None
    source_type: Optional[str] = None
    datasource: Optional[str] = None
    db_table: Optional[str] = None
    metric_column: Optional[str] = None
    filter_condition: Optional[str] = None
    agg_func: Optional[str] = None
    priority: Optional[int] = None
    is_valid: Optional[bool] = True
    source_level: Optional[str] = None
    biz_domain: Optional[str] = None
    cal_logic: Optional[str] = None
    unit: Optional[str] = None
    create_time: Optional[datetime] = None
    modify_time: Optional[datetime] = None
