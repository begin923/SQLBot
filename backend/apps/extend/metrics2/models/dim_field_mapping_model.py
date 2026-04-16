from datetime import datetime
from typing import Optional
from sqlalchemy import func
from sqlmodel import SQLModel, Field, Column, VARCHAR, DATETIME, Integer


class DimFieldMapping(SQLModel, table=True):
    """维度字段映射表 - 维度与物理字段映射"""
    __tablename__ = "dim_field_mapping"

    db_table: str = Field(sa_column=Column(VARCHAR(128), primary_key=True, nullable=False, comment='物理表'))
    field: str = Field(sa_column=Column(VARCHAR(64), primary_key=True, nullable=False, comment='维度字段英文名'))
    dim_id: str = Field(sa_column=Column(VARCHAR(32), nullable=False, comment='维度ID（关联dim_definition.id）'))
    field_name: str = Field(sa_column=Column(VARCHAR(128), nullable=False, comment='维度字段中文名'))
    create_time: Optional[datetime] = Field(sa_column=Column(DATETIME, server_default=func.now(), comment='创建时间'))
    modify_time: Optional[datetime] = Field(sa_column=Column(DATETIME, server_default=func.now(), onupdate=func.now(), comment='修改时间'))

    __table_args__ = (
        {"comment": "维度-物理字段映射表"},
    )


class DimFieldMappingInfo(SQLModel):
    """维度字段映射信息对象"""
    dim_id: Optional[str] = None
    db_table: Optional[str] = None
    dim_field: Optional[str] = None
    create_time: Optional[datetime] = None
    modify_time: Optional[datetime] = None
