# Chat Manager - 会话状态管理模块

## 📁 目录结构

```
chat_manager/
├── models/
│   └── chat_state_model.py        # 数据模型定义（ChatState, ChatStateInfo）
├── curd/
│   └── chat_state.py              # CRUD 操作（create, update, get, delete）
├── api/
│   └── chat_state.py              # API 接口（RESTful）
├── services/
│   ├── chat_service.py            # 聊天业务服务（字段提取、历史补充）
│   └── chat_state_service.py      # 会话状态服务（从历史对话中提取状态）
├── __init__.py                    # 模块导出
└── README.md                      # 本文档
```

## 🎯 功能概述

本模块提供多轮对话的上下文管理能力，主要功能包括：

1. **会话状态管理**：创建、更新、查询、删除会话状态
2. **上下文信息维护**：
   - 指标（metrics）：如销售额、订单量等（字符串）
   - 维度（dimensions）：如日期、地区等（列表）
   - 过滤条件（filters）：如时间范围、地区限制等（列表，格式："字段=值"）
   - 表名（tables）：涉及的数据表（列表）
   - 名称映射（resolved_names）：用户术语与字段名的映射（字典）
   - 其他上下文（context）：排序规则、分组方式等（字典）
3. **智能字段提取**：使用 LLM 从用户问题中自动提取指标、维度和过滤条件
4. **历史数据补充**：当问题中无法提取时，自动从聊天历史中补充缺失的字段
5. **话题切换检测**：自动识别并处理话题切换（待实现）

## 🗄️ 数据库表结构

表名：`chat_state`

```sql
CREATE TABLE public.chat_state (
    chat_id int8 NOT NULL PRIMARY KEY,
    create_time timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    update_time timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    metrics text NULL,                          -- 最新提到的指标名称（中文）
    dimensions jsonb NULL,                      -- 维度名称列表
    filters jsonb NULL,                         -- 过滤条件列表
    tables jsonb NULL,                          -- 表名列表
    resolved_names jsonb NULL,                  -- 名称映射
    context jsonb NULL                          -- 其他上下文
);

-- 索引
CREATE INDEX idx_chat_state_chat_id ON public.chat_state (chat_id);
CREATE INDEX idx_chat_state_update_time ON public.chat_state (update_time);
```

## 📊 数据模型

### ChatState（ORM 模型）

数据库表对应的 ORM 模型，用于 SQLAlchemy 操作。

**特点：**
- 主键：`chat_id`（一个聊天只保留最新的一条记录）
- `metrics`：文本类型，存储最新的指标名称（中文）
- `dimensions`、`filters`、`tables`、`resolved_names`、`context`：JSONB 类型

### ChatStateInfo（Pydantic 模型）

用于数据传输和验证的对象，包含以下字段：

- `chat_id`: 聊天 ID（必填）
- `metrics`: str | None - 最新提到的指标名称，如 "销售额"
- `dimensions`: List[str] | None - 维度名称列表，如 ["日期", "地区"]
- `filters`: List[str] | None - 过滤条件列表，如 ["地区=北京", "时间=最近一个月"]
- `tables`: List[str] | None - 表名列表，如 ["orders", "users"]
- `resolved_names`: Dict[str, str] | None - 用户术语映射，如 {"卖的钱": "amount"}
- `context`: Dict[str, Any] | None - 其他上下文信息
- `create_time`: datetime | None
- `update_time`: datetime | None

## 🔧 CRUD 操作

### 1. 创建/更新会话状态

**策略：先删后插（Delete-Insert）**

```python
from apps.extend.chat_manager.curd.chat_state import update_chat_state
from apps.extend.chat_manager.models.chat_state_model import ChatStateInfo

info = ChatStateInfo(
    chat_id=123456,
    metrics="销售额",
    dimensions=["日期", "地区"],
    filters=["时间范围=最近一个月", "地区=北京"],
    tables=["orders", "users"],
    resolved_names={"卖的钱": "amount"},
    context={"last_question": "北京的销售额是多少？"}
)

update_chat_state(session, info)
```

**特性：**
- 同一个 `chat_id` 只保留最新的一条记录
- 更新时直接删除旧记录，然后插入新记录
- 自动更新 `update_time`

### 2. 查询最新状态

```python
from apps.extend.chat_manager.curd.chat_state import get_latest_chat_state_by_chat_id

state = get_latest_chat_state_by_chat_id(session, chat_id=123456)

if state:
    print(f"指标：{state.metrics}")
    print(f"维度：{state.dimensions}")
    print(f"过滤条件：{state.filters}")
    print(f"表名：{state.tables}")
```

### 3. 查询历史状态

```python
from apps.extend.chat_manager.curd.chat_state import get_chat_state_history

history = get_chat_state_history(session, chat_id=123456, limit=10)

for state in history:
    print(f"时间：{state.update_time}, 指标：{state.metrics}")
```

### 4. 清空会话状态

```python
from apps.extend.chat_manager.curd.chat_state import clear_chat_state_by_chat_id

clear_chat_state_by_chat_id(session, chat_id=123456)
```

### 5. 批量删除会话状态

```python
from apps.extend.chat_manager.curd.chat_state import delete_chat_state

delete_chat_state(session, chat_ids=[123, 456, 789])
```

## 🌐 API 接口

API 基础路径：`/extend/chat-manager`

### 1. 获取会话状态

```http
GET /state/{chat_id}
```

**响应示例：**
```json
{
  "success": true,
  "data": {
    "chat_id": 123456,
    "metrics": "销售额",
    "dimensions": ["日期", "地区"],
    "filters": ["时间范围=最近一个月", "地区=北京"],
    "tables": ["orders", "users"],
    "resolved_names": {"卖的钱": "amount"},
    "context": {"last_question": "北京的销售额是多少？"}
  }
}
```

### 2. 创建/更新会话状态

```http
POST /state
Content-Type: application/json

{
  "chat_id": 123456,
  "metrics": "销售额",
  "dimensions": ["日期", "地区"],
  "filters": ["时间范围=最近一个月"],
  "tables": ["orders", "users"]
}
```

**响应：**
```json
{
  "success": true,
  "chat_id": 123456,
  "action": "update"
}
```

### 3. 获取状态历史

```http
GET /state/history/{chat_id}?limit=10
```

**响应：**
```json
{
  "success": true,
  "data": [...],
  "count": 10
}
```

### 4. 清空会话状态

```http
DELETE /state/{chat_id}
```

**响应：**
```json
{
  "success": true,
  "message": "已清空会话 123456 的所有状态记录"
}
```

### 5. 批量删除会话状态

```http
DELETE /state/by-ids
Content-Type: application/json

{
  "chat_ids": [123, 456, 789]
}
```

**响应：**
```json
{
  "success": true,
  "deleted_count": 3
}
```

## 💡 使用场景

### 场景 1：多轮对话上下文保持

**用户第一轮提问：**
> "查看北京 2024 年的销售额"

**系统处理：**
```python
from apps.extend.chat_manager.models.chat_state_model import ChatStateInfo

info = ChatStateInfo(
    chat_id=chat_id,
    metrics="销售额",
    dimensions=["年份"],
    filters=["地区=北京", "时间范围=2024 年"],
    tables=["sales_fact"]
)
update_chat_state(session, info)
```

**用户第二轮提问：**
> "那上海的呢？"

**系统处理：**
```python
# 从 chat_state 获取上下文
state = get_latest_chat_state_by_chat_id(session, chat_id)

# 提取指代对象："那...呢" → 替换地区过滤条件
info = ChatStateInfo(
    chat_id=chat_id,
    metrics=state.metrics,
    dimensions=state.dimensions,
    filters=["地区=上海", "时间范围=2024 年"],  # 只更新地区
    tables=state.tables
)
update_chat_state(session, info)
```

### 场景 2：智能字段提取（新增）

**用户提问：**
> "按月下钻北京和上海地区的销售额和销售量，最近一个月的数据"

**系统处理：**
```python
from apps.extend.chat_manager.services.chat_service import ChatService

chat_service = ChatService(llm)

# 自动提取指标、维度和过滤条件
result = chat_service.extract_fields_from_question(
    session=session,
    question="按月下钻北京和上海地区的销售额和销售量，最近一个月的数据",
    chat_id=chat_id
)

print(result)
# 输出：
# {
#     'metrics': ['销售额', '销售量'],
#     'dimensions': ['月', '地区'],
#     'filters': ['地区=北京', '地区=上海', '时间范围=最近一个月'],
#     'from_history': False
# }
```

**如果问题中无法提取指标维度：**
```python
# 自动从聊天历史中补充
result = chat_service.extract_fields_from_question(
    session=session,
    question="那上海的呢？",
    chat_id=chat_id
)

# 输出：
# {
#     'metrics': ['销售额'],  # 从历史补充
#     'dimensions': ['月'],   # 从历史补充
#     'filters': ['地区=上海'],
#     'from_history': True
# }
```

## 🧪 测试接口

项目提供了多个测试接口用于本地调试：

### 1. 查看示例数据
```http
GET /test/sample-data
```

### 2. 创建示例会话
```http
POST /test/create-sample
```

### 3. 查询会话状态
```http
GET /test/query/{chat_id}
```

### 4. 更新会话状态
```http
POST /test/update/{chat_id}
```

### 5. 清空会话状态（危险操作）
```http
DELETE /test/clear/{chat_id}
```

## 📝 最佳实践

### 1. 数据格式规范

**指标（metrics）**：
- 类型：字符串（只存储最新的指标名称）
- 示例：`"销售额"`、`"订单量"`

**维度（dimensions）**：
- 类型：列表
- 示例：`["日期", "地区"]`

**过滤条件（filters）**：
- 类型：列表
- 格式：`"字段名=值"`
- 示例：`["地区=北京", "时间范围=最近一个月"]`

**表名（tables）**：
- 类型：列表
- 示例：`["orders", "users"]`

**名称映射（resolved_names）**：
- 类型：字典
- 示例：`{"卖的钱": "amount"}`

### 2. 更新策略：先删后插

```python
def update_chat_state(session, info):
    """
    更新策略：
    1. 直接删除 chat_id 对应的旧记录
    2. 插入新记录（完全覆盖，不合并）
    """
    # 删除旧记录
    delete_stmt = text(f"DELETE FROM {ChatState.__tablename__} WHERE chat_id = :chat_id")
    session.execute(delete_stmt, {"chat_id": info.chat_id})
    session.commit()
    
    # 插入新记录
    state = ChatState(...)
    session.add(state)
    session.commit()
```

**优势：**
- 简单高效，不需要复杂的合并逻辑
- 避免新旧数据混台导致的错误
- 保持数据一致性

### 3. 性能优化

- **索引优化**：已在 `chat_id` 和 `update_time` 上创建索引
- **查询优化**：只查询最新一条记录，避免全表扫描
- **缓存策略**：可在应用层添加 Redis 缓存（TTL 建议 30 分钟）

### 4. 数据清理

建议定期清理历史状态：

```python
# 清理超过 30 天的历史记录
from sqlalchemy import text
session.execute(text("""
    DELETE FROM chat_state 
    WHERE update_time < NOW() - INTERVAL '30 days'
"""))
session.commit()
```

## 🔗 相关文件

- 数据模型：`backend/apps/extend/chat_manager/models/chat_state_model.py`
- CRUD 操作：`backend/apps/extend/chat_manager/curd/chat_state.py`
- API 接口：`backend/apps/extend/chat_manager/api/chat_state.py`
- 聊天服务：`backend/apps/extend/chat_manager/services/chat_service.py`
- 状态服务：`backend/apps/extend/chat_manager/services/chat_state_service.py`

## 📋 下一步计划

1. ✅ 完成基础 CRUD 操作
2. ✅ 完成 API 接口
3. ✅ 实现智能字段提取（LLM）
4. ✅ 实现历史数据自动补充
5. ⏳ 实现话题切换检测算法
6. ⏳ 集成到 LLMService 的 SQL 生成流程
7. ⏳ 添加 Redis 缓存支持
8. ⏳ 编写单元测试
