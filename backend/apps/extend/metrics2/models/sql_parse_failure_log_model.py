"""
SQL 解析失败日志模型
"""
from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel


class SqlParseFailureLog(SQLModel, table=True):
    """SQL 解析失败日志表"""
    
    __tablename__ = "sql_parse_failure_log"
    
    id: Optional[int] = Field(default=None, primary_key=True, description="主键ID")
    file_path: str = Field(max_length=500, description="SQL文件完整路径")
    file_name: str = Field(max_length=200, description="SQL文件名称")
    layer_type: Optional[str] = Field(default=None, max_length=20, description="数据层级类型，如DIM/DWD/METRIC/AUTO")
    failure_reason: str = Field(description="解析失败具体原因描述")
    error_type: Optional[str] = Field(default=None, max_length=50, description="错误类型枚举，如SELECT_STAR/JSON_PARSE/AI_ERROR")
    sql_content: Optional[str] = Field(default=None, description="异常SQL原文内容，可为空")
    matched_pattern: Optional[str] = Field(default=None, max_length=100, description="匹配到的异常模式，如a.*")
    parse_time: datetime = Field(default_factory=datetime.utcnow, description="SQL解析执行时间")
    is_resolved: bool = Field(default=False, description="是否已处理解决，默认未解决")
    resolve_time: Optional[datetime] = Field(default=None, description="问题解决处理时间")
    retry_count: int = Field(default=0, description="自动重试执行次数")
    create_time: datetime = Field(default_factory=datetime.utcnow, description="记录创建时间")
    modify_time: datetime = Field(default_factory=datetime.utcnow, description="记录最后更新时间")
    
    class Config:
        schema_extra = {
            "example": {
                "file_path": "/path/to/dim_accounting_period.sql",
                "file_name": "dim_accounting_period.sql",
                "layer_type": "DIM",
                "failure_reason": "SQL 中存在 SELECT * 或通配符（a.*），无法准确解析字段",
                "error_type": "SELECT_STAR",
                "matched_pattern": "a.*",
                "is_resolved": False,
                "retry_count": 0
            }
        }
