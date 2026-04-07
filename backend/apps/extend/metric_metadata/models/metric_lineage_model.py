from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel
from sqlalchemy import Column, Text, BigInteger, DateTime, Boolean, String
from sqlmodel import SQLModel, Field


class MetricLineage(SQLModel, table=True):
    """指标血缘表模型"""
    __tablename__ = "metric_lineage"
    
    metric_column: str = Field(sa_column=Column(String(100), primary_key=True, nullable=False))
    table_name: str = Field(sa_column=Column(String(100), primary_key=True, nullable=False))
    metric_name: str = Field(sa_column=Column(String(100), nullable=False))
    synonyms: Optional[str] = Field(sa_column=Column(Text, nullable=True))
    upstream_table: Optional[str] = Field(sa_column=Column(String(100), nullable=True))
    filter: Optional[str] = Field(sa_column=Column(Text, nullable=True))
    calc_logic: Optional[str] = Field(sa_column=Column(Text, nullable=True))
    dw_layer: Optional[str] = Field(sa_column=Column(String(20), nullable=True))
    embedding_vector: Optional[List[float]] = Field(sa_column=Column('embedding_vector', nullable=True))
    create_time: Optional[datetime] = Field(sa_column=Column(DateTime(timezone=False), nullable=True, default=datetime.now))


class MetricDimension(SQLModel, table=True):
    """指标维度表模型"""
    __tablename__ = "metric_dimension"
    
    table_name: str = Field(sa_column=Column(String(100), primary_key=True, nullable=False))
    dim_column: str = Field(sa_column=Column(String(100), primary_key=True, nullable=False))
    dim_name: Optional[str] = Field(sa_column=Column(String(100), nullable=True))
    embedding_vector: Optional[List[float]] = Field(sa_column=Column('embedding_vector', nullable=True))
    create_time: Optional[datetime] = Field(sa_column=Column(DateTime(timezone=False), nullable=True, default=datetime.now))


class MetricLineageInfo(BaseModel):
    """指标血缘信息对象"""
    metric_column: str  # 指标字段名
    table_name: str  # 物理表名
    metric_name: str  # 指标名称
    synonyms: Optional[str] = None  # 同义词，逗号分隔
    upstream_table: Optional[str] = None  # 上游关联表名
    filter: Optional[str] = None  # 核心字段，逗号分隔
    calc_logic: Optional[str] = None  # 计算逻辑
    dw_layer: Optional[str] = None  # 数仓分层
    enabled: Optional[bool] = True  # 是否启用


class MetricDimensionInfo(BaseModel):
    """指标维度信息对象"""
    table_name: str  # 物理表名
    dim_column: str  # 维度字段
    dim_name: Optional[str] = None  # 维度字段名
    enabled: Optional[bool] = True  # 是否启用
