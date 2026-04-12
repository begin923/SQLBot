from datetime import datetime
from typing import Optional

from sqlmodel import SQLModel, Field, Column, VARCHAR, DATETIME, Integer


class DimDict(SQLModel, table=True):
    """维度定义表"""
    __tablename__ = "dim_definition"

    dim_id: str = Field(sa_column=Column(VARCHAR(32), primary_key=True, comment='维度唯一ID，格式：D+6位数字（如D000001），自增'))
    dim_name: str = Field(sa_column=Column(VARCHAR(64), nullable=False, comment='维度中文名称（如猪场编号、胎次号）'))
    dim_code: str = Field(sa_column=Column(VARCHAR(64), nullable=False, unique=True, comment='维度英文编码（如field_id、parity_no），唯一'))
    dim_type: str = Field(sa_column=Column(VARCHAR(16), nullable=False, comment='维度类型：普通维度/时间维度/区域维度'))
    is_valid: Optional[int] = Field(sa_column=Column(Integer, default=1, comment='是否启用：1启用/0禁用'))
    create_time: Optional[datetime] = Field(sa_column=Column(DATETIME, default=datetime.now, comment='创建时间'))
    modify_time: Optional[datetime] = Field(sa_column=Column(DATETIME, default=datetime.now, onupdate=datetime.now, comment='修改时间'))

    __table_args__ = (
        {"comment": "维度定义表"},
    )


class DimDictInfo(SQLModel):
    """维度字典信息对象"""
    dim_id: Optional[str] = None
    dim_name: Optional[str] = None
    dim_code: Optional[str] = None
    dim_type: Optional[str] = None
    is_valid: Optional[bool] = True
    create_time: Optional[datetime] = None
    modify_time: Optional[datetime] = None