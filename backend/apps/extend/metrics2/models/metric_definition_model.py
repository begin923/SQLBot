from datetime import datetime
from typing import Optional
from sqlalchemy import func
from sqlmodel import SQLModel, Field, Column, VARCHAR, TEXT, DATETIME, SmallInteger
from sqlalchemy import Index


class MetricDefinition(SQLModel, table=True):
    """统一指标语义定义表"""
    __tablename__ = "metric_definition"

    id: str = Field(sa_column=Column(VARCHAR(32), primary_key=True, comment='指标全局唯一ID，格式：M+6位数字（如M000001），自增'))
    name: str = Field(sa_column=Column(VARCHAR(128), nullable=False, comment='指标中文名称（如胎次配种总次数）'))
    code: str = Field(sa_column=Column(VARCHAR(64), nullable=False, unique=True, comment='指标英文编码（如mating_total_cnt），唯一，用于接口/BI调用'))
    metric_type: str = Field(sa_column=Column(VARCHAR(16), nullable=False, comment='指标类型：ATOMIC（原子）/COMPOUND（复合）'))
    biz_domain: str = Field(sa_column=Column(VARCHAR(32), nullable=False, comment='业务域：养殖繁殖/养殖饲喂/养殖防疫'))
    status: Optional[int] = Field(sa_column=Column(SmallInteger, default=1, comment='状态：1启用/0禁用'))
    create_time: Optional[datetime] = Field(sa_column=Column(DATETIME, server_default=func.now(), comment='创建时间'))
    modify_time: Optional[datetime] = Field(sa_column=Column(DATETIME, server_default=func.now(), onupdate=func.now(), comment='修改时间'))

    __table_args__ = (
        Index('idx_code_unique', 'code', unique=True),  # ⚠️ code 唯一约束
        {"comment": "统一指标语义定义表"},
    )


class MetricDefinitionInfo(SQLModel):
    """指标定义信息对象"""
    id: Optional[str] = None
    name: Optional[str] = None
    code: Optional[str] = None
    metric_type: Optional[str] = None
    biz_domain: Optional[str] = None
    status: Optional[bool] = True
    create_time: Optional[datetime] = None
    modify_time: Optional[datetime] = None