from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field, Column, VARCHAR, DATETIME, Integer
from sqlalchemy.dialects.mysql import TINYINT


class DimVersion(SQLModel, table=True):
    """维度版本管理表"""
    __tablename__ = "dim_version"

    version_id: str = Field(sa_column=Column(VARCHAR(32), primary_key=True, comment='版本ID'))
    dim_id: str = Field(sa_column=Column(VARCHAR(32), nullable=False, comment='维度ID'))
    dim_name: str = Field(sa_column=Column(VARCHAR(64), nullable=False, comment='维度名称'))
    version: int = Field(sa_column=Column(Integer, nullable=False, comment='版本号'))
    effective_time: datetime = Field(sa_column=Column(DATETIME, nullable=False, comment='生效时间'))
    is_current: int = Field(sa_column=Column(TINYINT, nullable=False, default=1, comment='当前版本'))

    __table_args__ = (
        {"comment": "维度版本管理表"},
    )


class DimVersionInfo(SQLModel):
    """维度版本信息对象"""
    version_id: Optional[str] = None
    dim_id: Optional[str] = None
    dim_name: Optional[str] = None
    version: Optional[int] = None
    effective_time: Optional[datetime] = None
    is_current: Optional[bool] = True
