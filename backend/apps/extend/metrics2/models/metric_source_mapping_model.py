from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field, Column, VARCHAR, TEXT, DATETIME, Integer
from sqlalchemy.dialects.mysql import TINYINT


class MetricSourceMapping(SQLModel, table=True):
    """指标多源物理映射表"""
    __tablename__ = "metric_source_mapping"

    map_id: str = Field(sa_column=Column(VARCHAR(32), primary_key=True, comment='映射唯一ID，格式：MAP+3位数字（如MAP001），自增'))
    metric_id: str = Field(sa_column=Column(VARCHAR(32), nullable=False, comment='关联指标ID（metric_definition主键）'))
    source_type: str = Field(sa_column=Column(VARCHAR(16), nullable=False, comment='数据源类型：OFFLINE（离线）/REAL_TIME（实时）'))
    datasource: str = Field(sa_column=Column(VARCHAR(64), nullable=False, comment='数据源标识：hive/clickhouse/mysql'))
    db_table: str = Field(sa_column=Column(VARCHAR(128), nullable=False, comment='物理库表名（格式：库名.表名，如dwd.dwd_pig_breed_event_di）'))
    metric_column: Optional[str] = Field(sa_column=Column(VARCHAR(64), default=None, comment='指标字段名（原子指标直接使用的字段，复合指标无需填写）'))
    filter_condition: Optional[str] = Field(sa_column=Column(TEXT, default=None, comment='筛选条件（WHERE后内容，如"event_type=''配种'' AND is_valid=1"）'))
    agg_func: Optional[str] = Field(sa_column=Column(VARCHAR(32), default=None, comment='聚合函数：COUNT/SUM/AVG/MAX，原子指标必填，复合指标无需填写'))
    priority: int = Field(sa_column=Column(TINYINT, nullable=False, comment='优先级：1最高，依次递减（1=权威源，2=备用源，3=废弃源）'))
    is_valid: int = Field(sa_column=Column(TINYINT, nullable=False, default=1, comment='是否启用：1启用/0禁用'))
    source_level: str = Field(sa_column=Column(VARCHAR(16), nullable=False, comment='源等级：AUTHORITY（权威）/STANDBY（备用）/DISCARD（废弃）'))
    create_time: Optional[datetime] = Field(sa_column=Column(DATETIME, default=datetime.now, comment='创建时间'))
    update_time: Optional[datetime] = Field(sa_column=Column(DATETIME, default=datetime.now, onupdate=datetime.now, comment='更新时间'))

    __table_args__ = (
        {"comment": "指标多源物理映射表"},
    )


class MetricSourceMappingInfo(SQLModel):
    """指标源映射信息对象"""
    map_id: Optional[str] = None
    metric_id: Optional[str] = None
    source_type: Optional[str] = None
    datasource: Optional[str] = None
    db_table: Optional[str] = None
    metric_column: Optional[str] = None
    filter_condition: Optional[str] = None
    agg_func: Optional[str] = None
    priority: Optional[int] = None
    is_valid: Optional[bool] = True
    source_level: Optional[str] = None