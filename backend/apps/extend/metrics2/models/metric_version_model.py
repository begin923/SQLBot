from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field, Column, VARCHAR, TEXT, DATETIME, Integer
from sqlalchemy.dialects.mysql import TINYINT


class MetricVersion(SQLModel, table=True):
    """指标版本管理表 - 历史口径回溯"""
    __tablename__ = "metric_version"

    version_id: str = Field(sa_column=Column(VARCHAR(32), primary_key=True, comment='版本ID(V001)'))
    metric_id: str = Field(sa_column=Column(VARCHAR(32), nullable=False, comment='指标ID'))
    cal_logic: str = Field(sa_column=Column(TEXT, nullable=False, comment='历史口径'))
    version: int = Field(sa_column=Column(Integer, nullable=False, comment='版本号'))
    effective_time: datetime = Field(sa_column=Column(DATETIME, nullable=False, comment='生效时间'))
    expire_time: Optional[datetime] = Field(sa_column=Column(DATETIME, default=None, comment='失效时间'))
    is_current: int = Field(sa_column=Column(TINYINT, nullable=False, default=1, comment='当前版本'))

    __table_args__ = (
        {"comment": "指标版本管理表"},
    )


class MetricVersionInfo(SQLModel):
    """指标版本信息对象"""
    version_id: Optional[str] = None
    metric_id: Optional[str] = None
    cal_logic: Optional[str] = None
    version: Optional[int] = None
    effective_time: Optional[datetime] = None
    expire_time: Optional[datetime] = None
    is_current: Optional[bool] = True
