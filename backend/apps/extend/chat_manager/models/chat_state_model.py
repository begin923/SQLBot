from datetime import datetime
from typing import List, Optional, Dict, Any

from pgvector.sqlalchemy import VECTOR
from pydantic import BaseModel, Field as PydanticField
from sqlalchemy import Column, Text, BigInteger, DateTime, Identity, JSON
from sqlmodel import SQLModel, Field


class ChatState(SQLModel, table=True):
    """聊天状态表（ORM 模型）"""
    __tablename__ = "chat_state"
    
    chat_id: int = Field(sa_column=Column(BigInteger, primary_key=True, nullable=False))
    create_time: Optional[datetime] = Field(sa_column=Column(DateTime(timezone=False), nullable=True, default=datetime.now))
    update_time: Optional[datetime] = Field(sa_column=Column(DateTime(timezone=False), nullable=True, default=datetime.now, onupdate=datetime.now))
    metrics: Optional[str] = Field(sa_column=Column(Text, nullable=True))  # 最新提到的指标名称
    dimensions: Optional[List[str]] = Field(sa_column=Column(JSON, nullable=True))  # 维度名称列表
    filters: Optional[List[str]] = Field(sa_column=Column(JSON, nullable=True))  # 过滤条件列表，如 ["地区=北京", "时间=最近一个月"]
    tables: Optional[List[str]] = Field(sa_column=Column(JSON, nullable=True))  # 涉及的表名列表
    resolved_names: Optional[Dict[str, str]] = Field(sa_column=Column(JSON, nullable=True))  # 用户术语与字段名的映射
    context: Optional[Dict[str, Any]] = Field(sa_column=Column(JSON, nullable=True))  # 其他上下文信息


class ChatStateInfo(BaseModel):
    """聊天状态信息对象"""
    chat_id: int
    metrics: Optional[str] = None  # 最新提到的指标名称（只取一个），如 "销售额"
    dimensions: Optional[List[str]] = None  # 维度名称列表，如 ["日期", "地区"]
    filters: Optional[List[str]] = None  # 过滤条件列表，如 ["地区=北京", "时间=最近一个月"]
    tables: Optional[List[str]] = None  # 涉及的表名列表，如 ["orders", "users"]
    resolved_names: Optional[Dict[str, str]] = None  # 用户术语与字段名的映射，如 {"卖的钱": "amount"}
    context: Optional[Dict[str, Any]] = None  # 其他上下文信息
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
