from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field, Column, VARCHAR, DATETIME


class MetricLineage(SQLModel, table=True):
    """指标血缘表 - 指标与字段血缘关联"""
    __tablename__ = "metric_lineage"

    id: int = Field(sa_column=Column(VARCHAR(32), primary_key=True, autoincrement=True, comment='主键ID'))
    metric_id: str = Field(sa_column=Column(VARCHAR(32), nullable=False, comment='指标ID'))
    field_lineage_id: str = Field(sa_column=Column(VARCHAR(32), nullable=False, comment='字段血缘ID'))
    create_time: Optional[datetime] = Field(sa_column=Column(DATETIME, default=datetime.now, comment='创建时间'))

    __table_args__ = (
        {"comment": "指标-字段血缘关联表"},
    )


class MetricLineageInfo(SQLModel):
    """指标血缘信息对象"""
    id: Optional[int] = None
    metric_id: Optional[str] = None
    field_lineage_id: Optional[str] = None
