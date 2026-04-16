from datetime import datetime
from typing import Optional
from sqlalchemy import func
from sqlmodel import SQLModel, Field, Column, VARCHAR, DATETIME


class MetricLineage(SQLModel, table=True):
    """指标血缘表 - 指标与字段血缘关联"""
    __tablename__ = "metric_lineage"

    metric_id: str = Field(sa_column=Column(VARCHAR(32), primary_key=True, comment='指标ID'))
    field_lineage_id: str = Field(sa_column=Column(VARCHAR(32), primary_key=True, comment='字段血缘ID'))
    create_time: Optional[datetime] = Field(sa_column=Column(DATETIME, server_default=func.now(), comment='创建时间'))
    modify_time: Optional[datetime] = Field(sa_column=Column(DATETIME, server_default=func.now(), onupdate=func.now(), comment='修改时间'))

    __table_args__ = (
        {"comment": "指标-字段血缘关联表"},
    )


class MetricLineageInfo(SQLModel):
    """指标血缘信息对象"""
    metric_id: Optional[str] = None
    field_lineage_id: Optional[str] = None
    create_time: Optional[datetime] = None
    modify_time: Optional[datetime] = None
