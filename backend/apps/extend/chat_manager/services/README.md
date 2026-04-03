# ChatStateService - 会话状态提取服务

## 📖 概述

`ChatStateService` 提供了基于 LLM 的智能会话状态提取能力，能够从历史对话记录中自动提取指标、维度、过滤条件等关键信息，并将会话状态初始化到数据库中。

## 🎯 核心功能

### 1. 从历史对话提取会话状态
- 调用 `get_recent_records_by_chat_id` 获取最近 N 条对话记录
- 使用 LLM 分析对话内容，提取关键信息
- 构建结构化的 `SessionStateInfo` 对象

### 2. 初始化会话状态到数据库
- 自动调用 CRUD 将提取的状态写入 `session_state` 表
- 支持智能合并（如果已存在则更新）
- 提供完整的错误处理和日志记录

### 3. 用户意图判断
- 判断当前问题是否需要从 session_state 中提取数据
- 识别指代词、延续性话题、比较类问题
- 支持 LLM 判断和简单规则降级两种方案

---

## 💻 快速开始

### 基础用法

```python
from apps.extend.chat_manager.services.chat_services import ChatStateService
from common.core.deps import SessionDep

# 创建服务实例
service = ChatStateService(llm=llm_client)

# 从历史对话提取会话状态
session_state = service.extract_chat_state_from_history(
    session=session,
    chat_id=123456,
    datasource_id=None,
    limit=10
)

# 初始化到数据库
state_id = service.initialize_session_state_from_history(
    session=session,
    chat_id=123456,
    datasource_id=789,
    limit=10
)
```

### 判断用户意图

```python
# 判断是否需要提取 session_state
need_extract = service.should_extract_session_state(
    current_question="那上海的呢？",
    has_history=True
)

if need_extract:
    print("需要从历史对话中提取上下文信息")
else:
    print("全新问题，不需要历史上下文")
```

---

## 🔧 API 参考

### ChatStateService 类

#### `__init__(self, llm: BaseChatModel = None)`
初始化服务

**参数：**
- `llm`: 大模型客户端实例（可选）

---

#### `extract_session_state_from_history(...)`
从历史对话记录中提取会话状态

**签名：**
```python
def extract_session_state_from_history(
    self, 
    session: SessionDep, 
    chat_id: int, 
    datasource_id: int = None,
    limit: int = 10
) -> Optional[SessionStateInfo]
```

**参数：**
- `session`: 数据库会话
- `chat_id`: 聊天 ID
- `datasource_id`: 数据源 ID（可选，用于过滤）
- `limit`: 查询历史记录数量限制

**返回：**
- 成功：`SessionStateInfo` 对象
- 失败：`None`

**示例：**

```python
session_state = service.extract_chat_state_from_history(
    session=session,
    chat_id=123456,
    limit=10
)

if session_state:
    print(f"提取的指标：{session_state.metrics}")
    print(f"提取的维度：{session_state.dimensions}")
```

---

#### `initialize_session_state_from_history(...)`
从历史对话初始化会话状态并写入数据库

**签名：**
```python
def initialize_session_state_from_history(
    self, 
    session: SessionDep, 
    chat_id: int, 
    datasource_id: int = None,
    limit: int = 10
) -> Optional[int]
```

**参数：**
- `session`: 数据库会话
- `chat_id`: 聊天 ID
- `datasource_id`: 数据源 ID（可选）
- `limit`: 查询历史记录数量限制

**返回：**
- 成功：创建的记录 ID（int）
- 失败：`None`

**示例：**
```python
state_id = service.initialize_session_state_from_history(
    session=session,
    chat_id=123456,
    datasource_id=789
)

if state_id:
    print(f"成功初始化会话状态，ID={state_id}")
```

---

#### `should_extract_session_state(...)`
判断当前用户问题是否需要从 session_state 中提取数据

**签名：**
```python
def should_extract_session_state(
    self, 
    current_question: str, 
    has_history: bool = True
) -> bool
```

**参数：**
- `current_question`: 当前用户问题
- `has_history`: 是否存在历史对话

**返回：**
- `True`: 需要提取
- `False`: 不需要提取

**示例：**
```python
questions = [
    "它的销售额是多少？",  # True - 指代词
    "那上海的呢？",        # True - 延续性话题
    "和上个月相比有什么变化？",  # True - 比较类
    "查询员工人数最多的部门",   # False - 全新话题
]

for question in questions:
    need_extract = service.should_extract_session_state(
        current_question=question,
        has_history=True
    )
    print(f"'{question}' => {need_extract}")
```

---

## 📊 提取的数据结构

### SessionStateInfo 字段说明

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| `metrics` | Dict | 指标信息 | `{"销售额": {"name": "销售额", "column": "sales_amount"}}` |
| `dimensions` | Dict | 维度信息 | `{"地区": {"name": "地区", "column": "region"}}` |
| `filters` | Dict | 过滤条件 | `{"region": ["北京", "上海"]}` |
| `tables` | List[str] | 涉及的表名 | `["sales_fact", "dim_region"]` |
| `resolved_names` | Dict | 用户术语与字段名映射 | `{"销售额": "sales_amount"}` |
| `context` | Dict | 其他上下文信息 | `{"group_by": ["region"], "aggregate_functions": ["SUM"]}` |

---

## 🧠 用户意图判断规则

### 需要提取 session_state 的情况

#### 1. 指代词
- **关键词**：它、这个、那个、其、该
- **示例**：
  - "它的销售额是多少？" → 需要提取
  - "这个数据怎么样？" → 需要提取

#### 2. 延续性话题
- **句式**：那...呢、再...、还...、继续...
- **示例**：
  - "那上海的呢？" → 需要提取
  - "再看看其他维度" → 需要提取
  - "还有其他数据吗？" → 需要提取

#### 3. 比较类问题
- **关键词**：相比、对比、差异
- **示例**：
  - "和上个月相比有什么变化？" → 需要提取
  - "对比一下两个地区" → 需要提取

#### 4. 追问类
- **关键词**：为什么、怎么回事、原因
- **示例**：
  - "为什么销售额下降了？" → 需要提取
  - "这是怎么回事？" → 需要提取

---

### 不需要提取 session_state 的情况

#### 1. 全新话题
- **特征**：明确的新主题，与之前无关
- **示例**：
  - "查询员工人数最多的部门" → 不需要提取
  - "展示所有产品线" → 不需要提取

#### 2. 独立查询
- **特征**：完整的、自包含的问题
- **示例**：
  - "按月份统计 2024 年的销售额" → 不需要提取
  - "列出销售额前 10 的产品" → 不需要提取

#### 3. 打招呼/闲聊
- **示例**：
  - "你好" → 不需要提取
  - "谢谢" → 不需要提取

---

## 🔍 LLM 提示词模板

### 提取会话状态的 Prompt

```python
prompt = """
你是数据结构化分析专家。

任务：从对话历史中提取关键信息，构建会话状态（session_state）。

提取内容：
1. metrics（指标信息）
2. dimensions（维度信息）
3. filters（过滤条件）
4. tables（涉及的表）
5. resolved_names（名称映射）
6. context（其他上下文信息）

输出格式：严格的 JSON 对象
"""
```

### 判断用户意图的 Prompt

```python
prompt = """
你是对话意图分析专家。

任务：判断用户当前问题是否需要从历史对话的 session_state 中提取数据。

判断规则：
- 需要提取：指代词、延续性话题、比较类、追问类
- 不需要提取：全新话题、独立查询、闲聊

输出格式：{"need_extract": true/false, "reason": "说明"}
"""
```

---

## ⚙️ 配置选项

### 查询历史记录数量（limit）

| 场景 | 推荐值 | 说明 |
|------|--------|------|
| 短对话 | 3-5 条 | 快速但可能遗漏信息 |
| 普通对话 | 10 条 | 平衡性能和准确性 |
| 长对话 | 15-20 条 | 全面但耗时 |

### 数据源过滤（datasource_id）

- **不指定**：查询该 chat_id 下的所有记录
- **指定**：只查询特定数据源的记录（跨数据源会话场景）

---

## 🛠️ 最佳实践

### 1. 何时调用初始化

```python
# ✅ 推荐：在新对话开始时检查并初始化
async def start_new_chat(session, chat_id):
    service = ChatStateService(llm=llm)
    
    # 检查是否已有状态
    existing = get_latest_session_state_by_chat_id(session, chat_id)
    
    if not existing:
        # 首次对话，从历史初始化
        service.initialize_session_state_from_history(
            session=session,
            chat_id=chat_id
        )
```

### 2. 结合用户意图判断

```python
# ✅ 推荐：先判断意图，再决定是否提取
async def handle_user_question(question, chat_id):
    service = ChatStateService(llm=llm)
    
    # 判断是否需要提取
    need_extract = service.should_extract_session_state(
        current_question=question,
        has_history=True
    )
    
    if need_extract:
        # 需要上下文，加载 session_state
        state = get_latest_session_state_by_chat_id(session, chat_id)
        # ... 使用 state 生成 SQL
    else:
        # 全新问题，不需要历史上下文
        # ... 直接生成 SQL
```

### 3. 错误处理

```python
# ✅ 推荐：完善的错误处理
try:
    state_id = service.initialize_session_state_from_history(
        session=session,
        chat_id=chat_id
    )
    
    if state_id:
        logger.info(f"初始化成功：{state_id}")
    else:
        logger.warn("初始化失败，但不影响后续对话")
        
except Exception as e:
    logger.error(f"初始化异常：{e}")
    # 降级处理：继续使用，不使用 session_state
```

### 4. 性能优化

```python
# ✅ 推荐：根据场景调整 limit
if is_short_conversation:
    limit = 5  # 快速
else:
    limit = 10  # 标准

session_state = service.extract_chat_state_from_history(
    session=session,
    chat_id=chat_id,
    limit=limit
)
```

---

## 📝 使用示例

更多完整示例请参考：
- [`chat_service_examples.py`](chat_service_examples.py) - 包含 6 个详细的使用示例

### 示例列表：
1. **基础用法** - 提取会话状态
2. **初始化到数据库** - 提取并写入
3. **带数据源过滤** - 限定数据源范围
4. **用户意图判断** - 判断是否需要提取
5. **无 LLM 降级** - 使用简单规则
6. **自定义查询数量** - 控制历史记录范围

---

## 🔗 相关模块

- **Model**: [`session_state_model.py`](../models/session_state_model.py)
- **CURD**: [`session_state.py`](../curd/session_state.py)
- **API**: [`api/session_state.py`](../api/session_state.py)
- **用户意图**: [`user_intent.py`](../user_intent.py)

---

## 📌 注意事项

1. **LLM 依赖**：提取功能需要有效的 LLM 客户端
2. **降级处理**：无 LLM 时使用简单规则判断意图
3. **性能考虑**：limit 参数不宜过大，建议 10-20 条
4. **数据一致性**：同一个 chat_id 只保留最新的一条状态记录
5. **错误容忍**：提取失败不影响正常对话流程

---

## 🚀 下一步计划

- [ ] 支持增量更新 session_state
- [ ] 添加 Redis 缓存提高性能
- [ ] 实现话题切换自动检测
- [ ] 添加单元测试
- [ ] 集成到 LLMService 主流程
