from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field, Column, BIGINT, VARCHAR, DATETIME, Integer
from sqlalchemy.dialects.mysql import TINYINT


class MetricDimRel(SQLModel, table=True):
    """指标-维度关联表"""
    __tablename__ = "metric_dim_rel"

    id: int = Field(sa_column=Column(BIGINT, primary_key=True, autoincrement=True, comment='自增主键'))
    metric_id: str = Field(sa_column=Column(VARCHAR(32), nullable=False, comment='关联指标ID（metric_definition主键）'))
    dim_id: str = Field(sa_column=Column(VARCHAR(32), nullable=False, comment='关联维度ID（dim_dict主键）'))
    is_required: int = Field(sa_column=Column(TINYINT, default=0, comment='是否必选维度：1必选/0可选（如胎次号、统计日期为必选）'))
    create_time: Optional[datetime] = Field(sa_column=Column(DATETIME, default=datetime.now, comment='创建时间'))

    __table_args__ = (
        {"comment": "指标维度关联表"},
    )


class MetricDimRelInfo(SQLModel):
    """指标维度关联信息对象"""
    id: Optional[int] = None
    metric_id: Optional[str] = None
    dim_id: Optional[str] = None
    is_required: Optional[bool] = False