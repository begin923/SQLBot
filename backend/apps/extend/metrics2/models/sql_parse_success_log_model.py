"""
SQL 解析成功记录模型
用于记录已成功解析的 SQL 文件，避免重复处理
"""
from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel


class SqlParseSuccessLog(SQLModel, table=True):
    """SQL 解析成功记录表"""
    
    __tablename__ = "sql_parse_success_log"
    
    id: Optional[int] = Field(default=None, primary_key=True, description="主键ID")
    file_path: str = Field(max_length=500, unique=True, index=True, description="SQL文件完整路径（唯一）")
    file_name: str = Field(max_length=200, description="SQL文件名称")
    layer_type: str = Field(max_length=20, description="数据层级类型，如DIM/DWD/METRIC")
    target_table: Optional[str] = Field(default=None, max_length=200, description="目标表名")
    table_stats: Optional[str] = Field(default=None, description="各表写入统计信息（JSON格式）")
    parse_time: datetime = Field(default_factory=datetime.utcnow, description="SQL解析执行时间")
    processing_duration: Optional[float] = Field(default=None, description="处理耗时（秒）")
    create_time: datetime = Field(default_factory=datetime.utcnow, description="记录创建时间")
    modify_time: datetime = Field(default_factory=datetime.utcnow, description="记录最后更新时间")
    
    class Config:
        schema_extra = {
            "example": {
                "file_path": "/path/to/dim_accounting_period.sql",
                "file_name": "dim_accounting_period.sql",
                "layer_type": "DIM",
                "target_table": "yz_datawarehouse_dim.dim_accounting_period",
                "table_stats": '{"dim_definition": 10, "dim_field_mapping": 10}',
                "processing_duration": 5.23
            }
        }
