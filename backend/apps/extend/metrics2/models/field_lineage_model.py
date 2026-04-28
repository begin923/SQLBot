from datetime import datetime
from typing import Optional
from sqlalchemy import func, Text
from sqlmodel import SQLModel, Field, Column, VARCHAR, DATETIME

class FieldLineage(SQLModel, table=True):
    """字段级血缘表 - 校验核心（含维度标记）"""
    __tablename__ = "field_lineage"

    id: str = Field(sa_column=Column(VARCHAR(32), primary_key=True, comment='字段血缘ID(F000001)'))
    table_lineage_id: str = Field(sa_column=Column(VARCHAR(500), nullable=False, comment='关联的表血缘ID（多个用逗号分隔）'))
    source_table: str = Field(sa_column=Column(VARCHAR(500), nullable=False, comment='上游源表（多个用逗号分隔）'))
    source_field: str = Field(sa_column=Column(Text(), nullable=False, comment='上游源字段'))
    target_table: str = Field(sa_column=Column(VARCHAR(128), nullable=False, comment='下游目标表'))
    target_field: str = Field(sa_column=Column(Text(), nullable=False, comment='下游目标字段'))
    target_field_mark: str = Field(default='normal', sa_column=Column(VARCHAR(16), nullable=False, server_default='normal', comment='目标字段标记：public_dim/private_dim/metric/normal'))
    dim_id: Optional[str] = Field(default=None, sa_column=Column(VARCHAR(32), nullable=True, comment='公共维度绑定ID'))
    formula: Optional[str] = Field(default=None, sa_column=Column(Text(), nullable=True, comment='字段计算公式/表达式（支持长SQL）'))
    create_time: Optional[datetime] = Field(sa_column=Column(DATETIME, server_default=func.now(), comment='创建时间'))
    modify_time: Optional[datetime] = Field(sa_column=Column(DATETIME, server_default=func.now(), onupdate=func.now(), comment='修改时间'))

    __table_args__ = (
        {"comment": "字段映射校验表(含指标/维度标记，区分指标/公共/私有维度)"},
    )


class FieldLineageInfo(SQLModel):
    """字段级血缘信息对象"""
    id: Optional[str] = None
    table_lineage_id: Optional[str] = None
    source_table: Optional[str] = None
    source_field: Optional[str] = None
    target_table: Optional[str] = None
    target_field: Optional[str] = None
    target_field_mark: Optional[str] = 'normal'  # public_dim/private_dim/metric/normal
    dim_id: Optional[str] = None  # 仅公共维度有值
    formula: Optional[str] = None  # 字段计算公式/表达式
    create_time: Optional[datetime] = None
    modify_time: Optional[datetime] = None
