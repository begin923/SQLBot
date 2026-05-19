"""
工具系统测试
"""
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from apps.extend.tools.base import ToolRegistry


def test_tool_registration():
    """测试工具注册"""
    print("=" * 60)
    print("测试1: 工具注册")
    print("=" * 60)
    
    # 查看已注册的工具
    tools = ToolRegistry._tools
    print(f"已注册工具数量: {len(tools)}")
    print(f"工具列表: {list(tools.keys())}")
    print()
    
    for name, info in tools.items():
        print(f"工具名称: {name}")
        print(f"  描述: {info['description'][:50]}...")
        print(f"  参数: {list(info['parameters'].get('properties', {}).keys())}")
        print()


def test_tool_metadata():
    """测试获取工具元数据"""
    print("=" * 60)
    print("测试2: 工具元数据（OpenAI 格式）")
    print("=" * 60)
    
    metadata_list = ToolRegistry.get_all_tools_metadata()
    print(f"元数据数量: {len(metadata_list)}")
    print()
    
    import json
    for metadata in metadata_list:
        func_info = metadata['function']
        print(f"工具: {func_info['name']}")
        print(f"  描述: {func_info['description'][:50]}...")
        print(f"  必需参数: {func_info['parameters'].get('required', [])}")
        print()


def test_tool_calling():
    """测试工具调用（模拟）"""
    print("=" * 60)
    print("测试3: 工具调用示例")
    print("=" * 60)
    
    # 注意：这里只是演示调用方式，实际需要传入真实的服务实例
    print("工具调用需要以下依赖：")
    print("  - session: 数据库会话")
    print("  - chat_service: 聊天服务")
    print("  - metric_source_mapping: 指标源映射服务")
    print("  - column_metadata_service: 列元数据服务")
    print()
    print("实际调用示例：")
    print("""
    result = ToolRegistry.call_tool(
        "extract_metrics_and_dimensions",
        session=db_session,
        chat_service=chat_svc,
        question="查询2024年各地区的销售额",
        chat_id="chat_123"
    )
    """)


if __name__ == "__main__":
    try:
        test_tool_registration()
        test_tool_metadata()
        test_tool_calling()
        
        print("=" * 60)
        print("✅ 所有测试完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
