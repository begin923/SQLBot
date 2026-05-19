"""
查询构建工具 - 根据提取的信息生成最终的查询语句
"""
from .base import ToolRegistry


@ToolRegistry.register(
    name="build_query",
    description="根据维度、指标和过滤条件，构建自然语言查询语句。用于指导 LLM 生成准确的 SQL。",
    parameters={
        "type": "object",
        "properties": {
            "dimensions": {
                "type": "array",
                "items": {"type": "string"},
                "description": "维度列表，如：['地区', '时间']"
            },
            "dim_kv": {
                "type": "array",
                "items": {"type": "string"},
                "description": "维度字段映射列表，格式：'注释:字段名'，如：['地区:region', '时间:create_time']"
            },
            "date_filter": {
                "type": "array",
                "items": {"type": "string"},
                "description": "日期过滤条件列表，如：['2024年', '最近30天']"
            },
            "metric_name": {
                "type": "string",
                "description": "指标名称，如：销售额"
            },
            "calc_logic": {
                "type": "string",
                "description": "指标计算逻辑，如：sum(amount) 或 count(*)"
            }
        },
        "required": ["dimensions", "dim_kv", "metric_name", "calc_logic"]
    }
)
def build_query(dimensions: list, dim_kv: list, date_filter: list, metric_name: str, calc_logic: str) -> dict:
    """
    构建查询语句
    
    Args:
        dimensions: 维度列表
        dim_kv: 维度字段映射
        date_filter: 日期过滤条件
        metric_name: 指标名称
        calc_logic: 计算逻辑
        
    Returns:
        dict: {'user_query': '重构后的查询语句'}
    """
    # 构建分组部分
    group_by = f"按{','.join(dimensions)}分组" if dimensions else ""
    
    # 构建字段参考部分
    field_ref = f"分组参考字段：{','.join(dim_kv)}" if dim_kv else ""
    
    # 构建过滤部分
    filter_part = f"查询{'和'.join(date_filter)}的" if date_filter else "查询"
    
    # 构建指标部分
    metric_part = f"{metric_name}，{metric_name}计算逻辑为{calc_logic}"
    
    # 组合成完整查询
    parts = [group_by, field_ref, f"{filter_part}{metric_part}"]
    user_query = "，".join([p for p in parts if p])
    
    return {
        'user_query': user_query
    }
