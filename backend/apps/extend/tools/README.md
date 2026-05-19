# SQLBot Function Calling 工具系统

## 概述

本模块提供了一套基于 Function Calling 的工具系统，允许 LLM 动态调用预定义的函数来处理用户的数据查询请求。

## 架构设计

```
用户问题 → LLM 理解意图 → 调用工具链 → 返回结果
              ↓
         1. extract_metrics_and_dimensions (提取指标和维度)
         2. select_best_table (选择最佳表)
         3. build_query (构建查询语句)
```

## 快速开始

### 1. 导入工具注册器

```python
from apps.extend.tools import ToolRegistry
```

### 2. 获取所有工具的元数据（用于初始化 LLM）

```python
# 获取 OpenAI function calling 格式的 tools 列表
tools_metadata = ToolRegistry.get_all_tools_metadata()

# 传递给 LLM
response = client.chat.completions.create(
    model="gpt-4",
    messages=[...],
    tools=tools_metadata,
    tool_choice="auto"
)
```

### 3. 执行工具调用

```python
# 当 LLM 返回 tool_calls 时
for tool_call in response.choices[0].message.tool_calls:
    tool_name = tool_call.function.name
    arguments = json.loads(tool_call.function.arguments)
    
    # 调用对应的工具
    result = ToolRegistry.call_tool(tool_name, **arguments)
    
    # 将结果返回给 LLM
    messages.append({
        "role": "tool",
        "tool_call_id": tool_call.id,
        "content": json.dumps(result)
    })
```

## 已注册的工具

### 1. extract_metrics_and_dimensions

**功能：** 从自然语言问题中提取指标、维度和过滤条件

**参数：**
- `question` (string): 用户问题
- `chat_id` (string): 聊天ID

**返回：**
```json
{
  "metrics": ["sale_amount"],
  "dimensions": ["地区"],
  "filters": ["2024年"]
}
```

**使用示例：**
```python
result = ToolRegistry.call_tool(
    "extract_metrics_and_dimensions",
    session=db_session,
    chat_service=chat_svc,
    question="查询2024年各地区的销售额",
    chat_id="chat_123"
)
```

### 2. select_best_table

**功能：** 根据指标和维度选择最合适的物理表

**参数：**
- `metric_code` (string): 指标编码
- `dimensions` (list): 维度列表

**返回：**
```json
{
  "table_name": "dws_sales_region_daily",
  "calc_logic": "sum(sale_amt)",
  "dim_kv": ["地区:region_name"]
}
```

**使用示例：**
```python
result = ToolRegistry.call_tool(
    "select_best_table",
    metric_source_mapping=msm_service,
    column_metadata_service=col_service,
    metric_code="sale_amount",
    dimensions=["地区"]
)
```

### 3. build_query

**功能：** 构建最终的查询语句

**参数：**
- `dimensions` (list): 维度列表
- `dim_kv` (list): 维度字段映射
- `date_filter` (list): 日期过滤条件
- `metric_name` (string): 指标名称
- `calc_logic` (string): 计算逻辑

**返回：**
```json
{
  "user_query": "按地区分组，分组参考字段：地区:region_name，查询2024年的销售额，销售额计算逻辑为sum(sale_amt)"
}
```

## 在 llm.py 中集成

### 修改前（硬编码流程）

```python
# 原有代码 - 80行耦合逻辑
extract_res_dict = self.chat_service.extract_metric_and_dim_from_question(...)
metrics = extract_res_dict.get('metrics', [])
dimensions = extract_res_dict.get('dimensions', [])
# ... 大量 if-else 和业务逻辑
```

### 修改后（Function Calling）

```python
from apps.extend.tools import ToolRegistry

def process_with_function_calling(self, _session):
    """使用 Function Calling 处理用户问题"""
    
    # Step 1: 提取指标和维度
    extraction_result = ToolRegistry.call_tool(
        "extract_metrics_and_dimensions",
        session=_session,
        chat_service=self.chat_service,
        question=self.chat_question.question,
        chat_id=self.chat_question.chat_id
    )
    
    # Step 2: 选择最佳表
    table_result = ToolRegistry.call_tool(
        "select_best_table",
        metric_source_mapping=self.metric_source_mapping,
        column_metadata_service=self.column_metadata_service,
        metric_code=extraction_result['metrics'][0],
        dimensions=extraction_result['dimensions']
    )
    
    # Step 3: 批量添加表到数据源
    if table_result['table_name']:
        self._batch_add_tables_to_ds(_session, [table_result['table_name']])
    
    # Step 4: 获取 schema
    table_schemes = get_table_schema(
        session=_session,
        current_user=self.current_user,
        ds=self.ds,
        question=self.chat_question.question,
        table_name_list=[table_result['table_name']]
    )
    self.chat_question.db_schema = table_schemes
    
    # Step 5: 构建查询
    query_result = ToolRegistry.call_tool(
        "build_query",
        dimensions=extraction_result['dimensions'],
        dim_kv=table_result['dim_kv'],
        date_filter=extraction_result['filters'],
        metric_name=self._get_metric_name(extraction_result['metrics'][0]),
        calc_logic=table_result['calc_logic']
    )
    
    # 更新问题
    self.chat_question.question = query_result['user_query']
    self.refresh_sql_messages_with_new_schema()
```

## 提示词配置

在 `apps/extend/yaml/function_calling.yaml` 中定义了完整的 System Prompt，包括：

1. **工作流程说明**：告诉 LLM 如何按步骤调用工具
2. **工具列表**：每个工具的名称、描述和参数
3. **错误处理**：常见错误的友好提示
4. **完整示例**：端到端的调用示例

加载提示词：

```python
import yaml

with open('apps/extend/yaml/function_calling.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

system_prompt = config['system_prompt']
```

## 扩展新工具

### 步骤1：创建工具函数

```python
# apps/extend/tools/my_new_tool.py
from .base import ToolRegistry

@ToolRegistry.register(
    name="my_custom_tool",
    description="我的自定义工具描述",
    parameters={
        "type": "object",
        "properties": {
            "param1": {"type": "string", "description": "参数1"}
        },
        "required": ["param1"]
    }
)
def my_custom_tool(param1: str) -> dict:
    """实现你的逻辑"""
    return {"result": "success"}
```

### 步骤2：在 __init__.py 中导出

```python
# apps/extend/tools/__init__.py
from .my_new_tool import my_custom_tool

__all__ = [
    ...,
    'my_custom_tool'
]
```

### 步骤3：更新提示词

在 `function_calling.yaml` 中添加新工具的说明和示例。

## 优势

✅ **模块化**：每个工具独立，易于测试和维护  
✅ **可扩展**：新增工具只需注册，无需修改主流程  
✅ **LLM 友好**：清晰的描述和 schema，LLM 容易理解  
✅ **可复用**：工具可在不同场景下重复使用  
✅ **类型安全**：JSON Schema 验证参数  

## 注意事项

1. **工具命名**：使用英文小写+下划线，见名知意
2. **描述清晰**：用中文详细描述工具的用途和使用场景
3. **参数精简**：只保留必要的参数，避免过于复杂
4. **错误处理**：抛出明确的 ValueError，方便 LLM 理解
5. **返回值规范**：统一返回 dict 格式，便于序列化

## 调试技巧

### 查看已注册的工具

```python
print("已注册工具数量:", len(ToolRegistry._tools))
print("工具列表:", list(ToolRegistry._tools.keys()))
```

### 测试单个工具

```python
result = ToolRegistry.call_tool(
    "extract_metrics_and_dimensions",
    session=test_session,
    chat_service=test_service,
    question="测试问题",
    chat_id="test_chat"
)
print(result)
```

### 查看工具元数据

```python
metadata = ToolRegistry.get_tool_metadata("extract_metrics_and_dimensions")
import json
print(json.dumps(metadata, indent=2, ensure_ascii=False))
```
