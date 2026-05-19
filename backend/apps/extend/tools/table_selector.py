"""
表选择工具 - 根据指标和维度选择最合适的物理表
"""
from .base import ToolRegistry


@ToolRegistry.register(
    name="select_best_table",
    description="根据指标编码和维度列表，智能选择最合适的物理表。内部会自动查询指标元数据、检查维度字段是否存在、尝试当前表和上游表。返回表名、计算逻辑和维度字段映射。",
    parameters={
        "type": "object",
        "properties": {
            "metric_code": {
                "type": "string",
                "description": "指标编码，如：sale_amount"
            },
            "dimensions": {
                "type": "array",
                "items": {"type": "string"},
                "description": "维度列表，如：['地区', '时间']"
            }
        },
        "required": ["metric_code", "dimensions"]
    }
)
def select_best_table(metric_source_mapping, column_metadata_service, metric_code: str, dimensions: list) -> dict:
    """
    选择最佳的物理表（内部自动处理所有逻辑）
    
    工作流程：
    1. 调用 metric_source_mapping.search_metrics() 查询指标元数据
    2. 优先尝试当前表 (metric.db_table)
    3. 调用 column_metadata_service.search_columns() 检查维度是否存在
    4. 如果当前表不满足，尝试上游表 (metric.upstream_table)
    5. 返回最佳表的名称、计算逻辑和字段映射
    
    Args:
        metric_source_mapping: 指标源映射服务（用于查询指标元数据）
        column_metadata_service: 列元数据服务（用于检查维度字段）
        metric_code: 指标编码
        dimensions: 维度列表
        
    Returns:
        dict: {
            'table_name': '物理表名',
            'calc_logic': '计算逻辑（聚合函数或完整SQL表达式）',
            'dim_kv': ['注释:字段名', ...]  # 维度字段映射列表
        }
        
    Raises:
        ValueError: 当找不到指标或维度无法匹配时
    """
    # === 步骤1：查询指标元数据（内部实现，LLM 无需关心）===
    metric_info_list = metric_source_mapping.search_metrics([metric_code])
    
    if not metric_info_list or len(metric_info_list) == 0:
        raise ValueError(f"未找到指标: {metric_code}，请检查指标名称是否正确")
    
    metric = metric_info_list[0]
    
    # === 步骤2：优先尝试当前表（ADS/DWS层）===
    search_texts = [{
        "table_name": metric.db_table,
        "column_comments": dimensions
    }]
    results = column_metadata_service.search_columns(search_texts)
    
    if results['found']:
        # 当前表满足，使用聚合函数
        dim_kv = [f"{dim.column_comment}:{dim.column_name}" for dim in results['columns']]
        return {
            'table_name': metric.db_table,
            'calc_logic': metric.agg_func,  # ADS/DWS层使用聚合函数
            'dim_kv': dim_kv
        }
    
    # === 步骤3：当前表不满足，尝试上游表（DWD/ODS层）===
    if metric.upstream_table:
        search_texts = [{
            "table_name": metric.upstream_table,
            "column_comments": dimensions
        }]
        results = column_metadata_service.search_columns(search_texts)
        
        if results['found']:
            # 上游表满足，使用完整计算逻辑
            dim_kv = [f"{dim.column_comment}:{dim.column_name}" for dim in results['columns']]
            return {
                'table_name': metric.upstream_table,
                'calc_logic': metric.calc_logic,  # DWD/ODS层使用完整SQL表达式
                'dim_kv': dim_kv
            }
    
    # === 步骤4：都无法满足，抛出错误 ===
    raise ValueError(
        f"维度无法继续下钻：在表 '{metric.db_table}' 和上游表 '{metric.upstream_table}' 中都未找到维度 {dimensions}，"
        f"请尝试其他维度或联系管理员确认数据模型"
    )
