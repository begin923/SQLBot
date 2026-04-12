from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field, Column, VARCHAR, DATETIME
from sqlalchemy.dialects.mysql import TINYINT


class TableLineage(SQLModel, table=True):
    """表级血缘表 - 下钻核心"""
    __tablename__ = "table_lineage"

    lineage_id: str = Field(sa_column=Column(VARCHAR(32), primary_key=True, comment='表血缘ID(L000001)'))
    source_table: str = Field(sa_column=Column(VARCHAR(128), nullable=False, comment='上游明细表'))
    target_table: str = Field(sa_column=Column(VARCHAR(128), nullable=False, comment='下游汇总表'))
    create_time: Optional[datetime] = Field(sa_column=Column(DATETIME, default=datetime.now, comment='创建时间'))
    modify_time: Optional[datetime] = Field(sa_column=Column(DATETIME, default=datetime.now, onupdate=datetime.now, comment='修改时间'))

    __table_args__ = (
        {"comment": "表依赖血缘表"},
    )


class TableLineageInfo(SQLModel):
    """表级血缘信息对象"""
    lineage_id: Optional[str] = None
    source_table: Optional[str] = None
    target_table: Optional[str] = None
    create_time: Optional[datetime] = None
    modify_time: Optional[datetime] = None
