from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field, Column, VARCHAR, TEXT, DATETIME, SmallInteger


class MetricDefinition(SQLModel, table=True):
    """统一指标语义定义表"""
    __tablename__ = "metric_definition"

    metric_id: str = Field(sa_column=Column(VARCHAR(32), primary_key=True, comment='指标全局唯一ID，格式：M+6位数字（如M000001），自增'))
    metric_name: str = Field(sa_column=Column(VARCHAR(128), nullable=False, comment='指标中文名称（如胎次配种总次数）'))
    metric_code: str = Field(sa_column=Column(VARCHAR(64), nullable=False, comment='指标英文编码（如mating_total_cnt），唯一，用于接口/BI调用'))
    metric_type: str = Field(sa_column=Column(VARCHAR(16), nullable=False, comment='指标类型：ATOMIC（原子）/COMPOUND（复合）'))
    biz_domain: str = Field(sa_column=Column(VARCHAR(32), nullable=False, comment='业务域：养殖繁殖/养殖饲喂/养殖防疫'))
    cal_logic: Optional[str] = Field(sa_column=Column(TEXT, default=None, comment='口径计算逻辑（纯业务描述，如"单胎次内有效配种事件总次数"）'))
    unit: Optional[str] = Field(sa_column=Column(VARCHAR(16), default=None, comment='指标单位（如次、率、头）'))
    status: Optional[int] = Field(sa_column=Column(SmallInteger, default=1, comment='状态：1启用/0禁用'))
    create_time: Optional[datetime] = Field(sa_column=Column(DATETIME, default=datetime.now, comment='创建时间'))
    modify_time: Optional[datetime] = Field(sa_column=Column(DATETIME, default=datetime.now, onupdate=datetime.now, comment='修改时间'))

    __table_args__ = (
        {"comment": "统一指标语义定义表"},
    )


class MetricDefinitionInfo(SQLModel):
    """指标定义信息对象"""
    metric_id: Optional[str] = None
    metric_name: Optional[str] = None
    metric_code: Optional[str] = None
    metric_type: Optional[str] = None
    biz_domain: Optional[str] = None
    cal_logic: Optional[str] = None
    unit: Optional[str] = None
    status: Optional[bool] = True
    create_time: Optional[datetime] = None
    modify_time: Optional[datetime] = None