from datetime import datetime
from typing import Optional
from sqlalchemy import func
from sqlmodel import SQLModel, Field, Column, VARCHAR, DATETIME


class TableMetadata(SQLModel, table=True):
    """表元数据表 - 存储物理表的元信息"""
    __tablename__ = "table_metadata"

    id: str = Field(sa_column=Column(VARCHAR(32), primary_key=True, comment='物理表唯一ID'))
    table_name: str = Field(sa_column=Column(VARCHAR(128), nullable=False, unique=True, comment='物理表名'))
    source_level: Optional[str] = Field(default=None, sa_column=Column(VARCHAR(32), nullable=True, comment='ADS/DWS/DWD 分层'))
    biz_domain: Optional[str] = Field(default=None, sa_column=Column(VARCHAR(64), nullable=True, comment='业务域'))
    table_comment: Optional[str] = Field(default=None, sa_column=Column(VARCHAR(512), nullable=True, comment='表注释'))
    create_time: Optional[datetime] = Field(sa_column=Column(DATETIME, server_default=func.now(), comment='创建时间'))
    modify_time: Optional[datetime] = Field(sa_column=Column(DATETIME, server_default=func.now(), onupdate=func.now(), comment='修改时间'))


class TableMetadataInfo(SQLModel):
    """表元数据信息对象"""
    id: Optional[str] = None
    table_name: Optional[str] = None
    source_level: Optional[str] = None
    biz_domain: Optional[str] = None
    table_comment: Optional[str] = None
    create_time: Optional[datetime] = None
    modify_time: Optional[datetime] = None
