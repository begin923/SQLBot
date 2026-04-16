from datetime import datetime
from typing import Optional
from sqlalchemy import func
from sqlmodel import SQLModel, Field, Column, BIGINT, VARCHAR, DATETIME, Integer
from sqlalchemy.dialects.mysql import TINYINT


class MetricCompoundRel(SQLModel, table=True):
    """复合指标子指标关联表"""
    __tablename__ = "metric_compound_rel"

    id: int = Field(sa_column=Column(BIGINT, primary_key=True, autoincrement=True, comment='自增主键'))
    metric_id: str = Field(sa_column=Column(VARCHAR(32), nullable=False, comment='复合指标ID（metric_definition主键，metric_type=COMPOUND）'))
    sub_metric_id: str = Field(sa_column=Column(VARCHAR(32), nullable=False, comment='子指标ID（metric_definition主键，metric_type=ATOMIC）'))
    cal_operator: str = Field(sa_column=Column(VARCHAR(8), nullable=False, comment='运算符号：+ - * /（复合指标仅支持二元运算）'))
    sort: int = Field(sa_column=Column(TINYINT, nullable=False, comment='计算顺序：1、2、3...（按运算优先级排序）'))
    create_time: Optional[datetime] = Field(sa_column=Column(DATETIME, server_default=func.now(), comment='创建时间'))
    modify_time: Optional[datetime] = Field(sa_column=Column(DATETIME, server_default=func.now(), onupdate=func.now(), comment='修改时间'))

    __table_args__ = (
        {"comment": "复合指标子指标关联表"},
    )


class MetricCompoundRelInfo(SQLModel):
    """复合指标子指标关联信息对象"""
    id: Optional[int] = None
    metric_id: Optional[str] = None
    sub_metric_id: Optional[str] = None
    cal_operator: Optional[str] = None
    sort: Optional[int] = None
    create_time: Optional[datetime] = None
    modify_time: Optional[datetime] = None