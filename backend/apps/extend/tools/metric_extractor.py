"""
指标和维度提取工具
"""
from .base import ToolRegistry


@ToolRegistry.register(
    name="extract_metrics_and_dimensions",
    description="从用户的自然语言问题中提取指标、维度和过滤条件。例如：'查询2024年各地区的销售额' -> 指标:销售额, 维度:地区, 过滤:2024年",
    parameters={
        "type": "object",
        "properties": {
            "question": {
                "type": "string",
                "description": "用户的自然语言问题"
            },
            "chat_id": {
                "type": "string",
                "description": "当前聊天的ID，用于上下文关联"
            }
        },
        "required": ["question", "chat_id"]
    }
)
def extract_metrics_and_dimensions(session, chat_service, question: str, chat_id: str) -> dict:
    """
    从用户问题中提取指标、维度和过滤条件
    
    Args:
        session: 数据库会话
        chat_service: 聊天服务实例
        question: 用户问题
        chat_id: 聊天ID
        
    Returns:
        dict: {
            'metrics': ['metric_code1', ...],
            'dimensions': ['dimension1', ...],
            'filters': ['filter1', ...]
        }
        
    Raises:
        ValueError: 当没有提取到指标或维度时
    """
    result = chat_service.extract_metric_and_dim_from_question(
        session=session,
        question=question,
        chat_id=chat_id
    )
    
    metrics = result.get('metrics', [])
    dimensions = result.get('dimensions', [])
    filters = result.get('filters', [])
    
    # 校验结果
    if not metrics or len(metrics) == 0:
        raise ValueError("没有可查询指标，请明确要查询的指标名称")
    
    if not dimensions or len(dimensions) == 0:
        raise ValueError("没有可查询维度，请明确分组维度（如：按地区、按时间等）")
    
    return {
        'metrics': metrics,
        'dimensions': dimensions,
        'filters': filters
    }
