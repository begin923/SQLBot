from datetime import datetime
from typing import List, Optional

from pgvector.sqlalchemy import VECTOR
from pydantic import BaseModel
from sqlalchemy import Column, Text, BigInteger, DateTime, Identity, Boolean, String
from sqlmodel import SQLModel, Field


class MetricMetadata(SQLModel, table=True):
    __tablename__ = "metric_metadata"
    
    id: Optional[int] = Field(sa_column=Column(BigInteger, Identity(always=True), primary_key=True))
    metric_name: str = Field(sa_column=Column(String(100), nullable=False))
    synonyms: Optional[str] = Field(sa_column=Column(Text, nullable=True))
    datasource_id: Optional[int] = Field(sa_column=Column(BigInteger, nullable=True))
    table_name: str = Field(sa_column=Column(String(100), nullable=False))
    core_fields: Optional[str] = Field(sa_column=Column(Text, nullable=True))
    calc_logic: Optional[str] = Field(sa_column=Column(Text, nullable=True))
    upstream_table: Optional[str] = Field(sa_column=Column(String(100), nullable=True))
    dw_layer: Optional[str] = Field(sa_column=Column(String(20), nullable=True))
    embedding_vector: Optional[List[float]] = Field(sa_column=Column(VECTOR(), nullable=True))
    create_time: Optional[datetime] = Field(sa_column=Column(DateTime(timezone=False), nullable=True))


class MetricMetadataInfo(BaseModel):
    """指标元数据信息对象"""
    id: Optional[int] = None
    metric_name: Optional[str] = None
    synonyms: Optional[str] = None  # 逗号分隔的同义词
    datasource_id: Optional[int] = None
    table_name: Optional[str] = None
    core_fields: Optional[str] = None  # 逗号分隔的核心字段
    calc_logic: Optional[str] = None
    upstream_table: Optional[str] = None
    dw_layer: Optional[str] = None
    enabled: Optional[bool] = True  # 是否启用
