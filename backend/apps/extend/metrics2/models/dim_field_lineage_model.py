from datetime import datetime
from typing import Optional
from sqlalchemy import func, Index
from sqlmodel import SQLModel, Field, Column, VARCHAR, DATETIME


class DimFieldLineage(SQLModel, table=True):
    """维度字段血缘表 - 维度与物理字段映射（重命名自 dim_field_mapping）"""
    __tablename__ = "dim_field_lineage"

    id: str = Field(sa_column=Column(VARCHAR(32), primary_key=True, comment='维度字段血缘ID，格式：D+6位数字（如D000001），非自增'))
    db_table: str = Field(sa_column=Column(VARCHAR(128), nullable=False, comment='物理表'))
    field: str = Field(sa_column=Column(VARCHAR(64), nullable=False, comment='维度字段英文名'))
    field_name: str = Field(sa_column=Column(VARCHAR(128), nullable=False, default='', comment='维度字段中文名'))
    dim_id: str = Field(sa_column=Column(VARCHAR(32), nullable=False, comment='维度ID（关联dim_definition.id）'))
    create_time: Optional[datetime] = Field(sa_column=Column(DATETIME, server_default=func.now(), comment='创建时间'))
    modify_time: Optional[datetime] = Field(sa_column=Column(DATETIME, server_default=func.now(), onupdate=func.now(), comment='修改时间'))

    __table_args__ = (
        Index('uk_db_table_field', 'db_table', 'field', unique=True),
        {"comment": "维度字段血缘表"},
    )


class DimFieldLineageInfo(SQLModel):
    """维度字段血缘信息对象"""
    id: Optional[str] = None
    db_table: Optional[str] = None
    field: Optional[str] = None
    field_name: Optional[str] = None
    dim_id: Optional[str] = None
    create_time: Optional[datetime] = None
    modify_time: Optional[datetime] = None
