# SQLBot 架构设计文档

## 1. 项目概述

SQLBot 是一个基于 FastAPI 的智能数据分析助手，通过自然语言交互实现数据查询、分析和可视化。系统采用前后端分离架构，支持多种数据源接入和大模型集成。

### 1.1 技术栈

**后端核心技术**
- **框架**: FastAPI + SQLAlchemy + SQLModel
- **数据库**: PostgreSQL (元数据存储)
- **ORM**: SQLModel (SQLAlchemy + Pydantic)
- **大模型**: LangChain + OpenAI API 兼容接口 (DashScope)
- **MCP**: Model Context Protocol (FastApiMCP)

**前端核心技术**
- **框架**: Vue 3 + TypeScript + Vite
- **UI 组件**: Element Plus
- **状态管理**: Pinia
- **图表**: AntV G2/G6

**基础设施**
- **容器化**: Docker + Docker Compose
- **迁移工具**: Alembic
- **缓存**: Redis
- **向量数据库**: Elasticsearch (用于语义搜索)

### 1.2 项目结构

```
backend/
├── apps/                      # 业务应用模块
│   ├── ai_model/             # AI 模型管理
│   ├── chat/                 # 聊天对话核心
│   ├── dashboard/            # 仪表盘管理
│   ├── data_training/        # 数据训练
│   ├── datasource/           # 数据源管理
│   ├── db/                   # 数据库连接与引擎
│   ├── extend/               # 扩展功能 (下钻/明细查询/执行静态sql(可选入参))
│   ├── mcp/                  # MCP 服务
│   ├── system/               # 系统管理
│   ├── template/             # 提示词模板
│   └── terminology/          # 术语管理
├── common/                    # 公共模块
│   ├── core/                 # 核心配置与中间件
│   └── utils/                # 工具函数
├── main.py                    # 应用入口
└── alembic/                   # 数据库迁移

frontend/
├── src/
│   ├── api/                  # API 接口
│   ├── components/           # 组件库
│   ├── views/                # 页面视图
│   ├── router/               # 路由配置
│   ├── stores/               # 状态管理
│   └── utils/                # 工具函数
```

---

## 2. 核心架构设计

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────┐
│                     用户层                               │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐   │
│  │ Web 前端 │  │ MCP 客户端│  │ 移动端  │  │ 第三方  │   │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘   │
└───────┼───────────┼───────────┼───────────┼───────────┘
        │           │           │           │
        └───────────┴─────┬─────┴───────────┘
                          │ HTTP/WebSocket
┌─────────────────────────┼─────────────────────────────┐
│                  API 网关层                              │
│  ┌──────────────────────┴────────────────────────┐   │
│  │  TokenMiddleware + ResponseMiddleware         │   │
│  │  - 认证鉴权                                   │   │
│  │  - 统一响应                                   │   │
│  │  - 异常处理                                   │   │
│  └────────────────────┬──────────────────────────┘   │
└───────────────────────┼───────────────────────────────┘
                        │
┌───────────────────────┼───────────────────────────────┐
│              业务逻辑层 (FastAPI Router)                │
│  ┌───────────────────┴───────────────────────────┐   │
│  │  api_router (apps/api.py)                     │   │
│  │    ├─ chat (对话查询)                         │   │
│  │    ├─ datasource (数据源管理)                  │   │
│  │    ├─ dashboard (仪表盘)                      │   │
│  │    ├─ system (系统管理)                       │   │
│  │    └─ ...                                     │   │
│  └───────────────────┬───────────────────────────┘   │
└───────────────────────┼───────────────────────────────┘
                        │
┌───────────────────────┼───────────────────────────────┐
│              服务层 (Service / CRUD)                    │
│  ┌────────────┬──────────────┬──────────────────┐    │
│  │ LLMService │ CRUD 模块     │ 扩展处理器       │    │
│  │ - 对话管理 │ - ChatCRUD   │ - StaticSQLHandler│   │
│  │ - SQL 生成 │ - Datasource │ - MetricDrilldown │   │
│  │ - 图表生成 │ - Dashboard  │ - ParseMDToJson   │   │
│  └────────────┴──────────────┴──────────────────┘    │
└───────────────────────┬───────────────────────────────┘
                        │
┌───────────────────────┼───────────────────────────────┐
│              数据访问层 (Repository)                      │
│  ┌────────────┬──────────────┬──────────────────┐    │
│  │ SQLModel   │ SQLAlchemy   │ 自定义引擎       │    │
│  │ - ORM 映射 │ - 会话管理   │ - exec_sql       │    │
│  │ - 类型安全 │ - 事务控制   │ - getFields      │    │
│  └────────────┴──────────────┴──────────────────┘    │
└───────────────────────┬───────────────────────────────┘
                        │
┌───────────────────────┼───────────────────────────────┐
│                  数据存储层                             │
│  ┌────────────┬──────────────┬──────────────────┐    │
│  │ PostgreSQL │  Redis       │ Elasticsearch   │    │
│  │ - 元数据   │ - 缓存       │ - 向量检索       │    │
│  │ - 聊天记录 │ - Session    │ - 语义搜索       │    │
│  └────────────┴──────────────┴──────────────────┘    │
└───────────────────────────────────────────────────────┘
```

### 2.2 模块化设计原则

**分层架构**
```
Controller (API) → Service (Business Logic) → Repository (Data Access) → Database
```

**职责分离**
- **API 层**: 仅负责请求处理和响应返回
- **Service 层**: 实现业务逻辑和流程编排
- **Repository 层**: 封装数据访问细节
- **Common 层**: 提供通用工具和基础设施

---

## 3. 核心业务流程

### 3.1 智能对话查询流程

#### 3.1.1 完整交互流程

```
用户提问
  │
  ├─→ [1] 创建会话记录 (create_chat)
  │
  ├─→ [2] 初始化对话上下文 (ChatQuestion)
  │     ├─ 加载术语配置 (terminologies)
  │     ├─ 加载数据训练数据 (data_training)
  │     └─ 加载自定义提示词 (custom_prompt)
  │
  ├─→ [3] 选择数据源 (select_datasource) - 可选
  │
  ├─→ [4] 获取表结构 (get_table_schema)
  │
  ├─→ [5] 下钻分析 (handle_drilldown_analysis) - 如果包含"下钻"关键词
  │     ├─ get_user_intent_and_table_scope() - LLM 判断查询范围
  │     ├─ extract_metric_blood_from_md() - 解析 MD 血缘文档
  │     └─ add_table_to_ds() - 添加依赖表到数据源
  │
  ├─→ [6] SQL 生成与执行
  │     │
  │     ├─ 模式 A: 静态 SQL 执行 (#FIXED_SQL_START#...#FIXED_SQL_END#)
  │     │   ├─ check_static_sql_mode() - 提取 SQL
  │     │   ├─ replace_parameters() - 参数替换
  │     │   └─ exe_static_sql() - 直接执行
  │     │
  │     ├─ 模式 B: 下钻查询 (is_drill_down=True)
  │     │   ├─ drill_down_sys_question() - 专用提示词
  │     │   ├─ generate_sql() - LLM 生成 SQL
  │     │   └─ execute_sql() - 执行查询
  │     │
  │     ├─ 模式 C: 明细查询 (is_view_details=True)
  │     │   ├─ view_details_sys_question() - 专用提示词
  │     │   ├─ generate_sql() - LLM 生成 SQL
  │     │   └─ execute_sql() - 执行查询
  │     │
  │     └─ 模式 D: 常规查询
  │         ├─ sql_sys_question() + sql_user_question()
  │         ├─ generate_sql() - LLM 生成 SQL
  │         ├─ check_sql() - 解析并验证
  │         ├─ generate_filter() - 行级权限过滤 - 可选
  │         └─ execute_sql() - 执行查询
  │
  ├─→ [7] 图表生成 (generate_chart)
  │     ├─ chart_sys_question() + chart_user_question()
  │     ├─ LLM 生成图表配置
  │     └─ check_save_chart() - 验证并保存
  │
  ├─→ [8] 数据格式化与返回
  │     ├─ DataFormat.convert_object_array_for_pandas()
  │     └─ 流式响应 / JSON 响应
  │
  └─→ [9] 保存聊天记录 & 推荐问题生成
```

#### 3.1.2 关键类与组件

**核心类**
```python
# llm.py
class LLMService:
    """对话查询服务"""
    
    # 属性
    - chat_question: ChatQuestion          # 对话问题对象
    - current_user: CurrentUser            # 当前用户
    - current_assistant: CurrentAssistant  # 当前助手
    - ds: CoreDatasource                   # 数据源
    - llm: OpenAI                          # LLM 客户端
    - static_sql_handler: StaticSQLHandler # 静态 SQL 处理器
    - metric_drilldown: MetricDrilldownHandler  # 下钻处理器
    
    # 核心方法
    - run_task(): 主流程执行器
    - generate_sql(): SQL 生成
    - execute_sql(): SQL 执行
    - generate_chart(): 图表生成
    - init_messages(): 初始化提示词消息
```

**数据模型**
```python
# chat_model.py
class ChatQuestion(AiModelQuestion):
    """对话问题配置"""
    question: str
    engine: str              # 数据库类型
    db_schema: str           # 表结构
    sql: str                 # SQL 语句
    terminologies: str       # 术语配置 XML
    data_training: str       # 数据训练配置
    custom_prompt: str       # 自定义提示词
    
    # 提示词生成方法
    - sql_sys_question(): 生成 SQL 系统提示词
    - drill_down_sys_question(): 下钻场景提示词
    - view_details_sys_question(): 明细查询提示词
    - chart_user_question(): 图表生成提示词
```

### 3.2 下钻分析流程

#### 3.2.1 下钻查询架构

```
用户问题："下钻指标 d7_sum"
  │
  ├─→ [1] 检测关键词 ("下钻"/"钻取"/"drill")
  │
  ├─→ [2] get_user_intent_and_table_scope(llm_client, question)
  │     ├─ LLM 分析查询意图
  │     ├─ 判断是否查询当前表 (is_current_table)
  │     ├─ 提取目标表名 (table_name)
  │     └─ 提取指标列表 (metrics)
  │
  ├─→ [3] 分支处理
  │     │
  │     ├─ 是 (is_current_table=True)
  │     │   ├─ add_table_to_ds(table_name)
  │     │   ├─ is_drill_down = True
  │     │   └─ chart_type = 'table'
  │     │
  │     └─ 否 (is_current_table=False)
  │         ├─ extract_metric_blood_from_md(table_name)
  │         │   └─ parse_md_to_json() - 解析 MD 文档
  │         ├─ 提取依赖表 (dependencies)
  │         ├─ add_table_to_ds(depend_tables)
  │         ├─ is_view_details = True
  │         └─ chart_type = 'table'
  │
  ├─→ [4] SQL 生成
  │     ├─ 使用专用提示词 (drill_down_sys_question)
  │     ├─ LLM 生成下钻 SQL
  │     └─ 保留原始条件和维度，移除聚合
  │
  └─→ [5] 执行并返回明细数据
```

#### 3.2.2 血缘关系解析

**MD 文档格式**
```markdown
# 字段清单

| field_id | field_name | field_type | source_tables | source_fields | calculation_type | formula |
|----------|------------|------------|---------------|---------------|------------------|---------|
| f001     | d7_sum     | 指标       | ads_table     | d7_estrus     | SUM              | SUM(d7_estrus) |
```

**解析器**
```python
# parse_md_to_json.py
class ParseMDToJson:
    """MD 文档解析器"""
    
    - parse_markdown_file(): 读取 MD 文件
    - extract_basic_info(): 提取基本信息
    - extract_field_list(): 提取字段清单
    - create_metric_dict(): 创建指标字典
    - build_dependencies(): 构建依赖关系
    
    # 输出格式
    {
        "success": true,
        "data": {
            "meta": {...},
            "target": {...},
            "metrics": {
                "d7_sum": {
                    "metric_id": "f001",
                    "calculation": {
                        "type": "SUM",
                        "formula": "SUM(d7_estrus)",
                        "dependencies": [
                            {"table": "ads_table", "fields": ["d7_estrus"]}
                        ]
                    }
                }
            }
        }
    }
```

### 3.3 静态 SQL 执行流程

#### 3.3.1 静态 SQL 模式

**触发方式**
```
用户问题包含：#FIXED_SQL_START#...#FIXED_SQL_END#
```

**处理流程**
```
#FIXED_SQL_START#{"sql": "SELECT...", "in_parm": {...}}#FIXED_SQL_END#
  │
  ├─→ [1] check_static_sql_mode(question)
  │     ├─ 正则提取内容
  │     ├─ JSON 解析
  │     └─ replace_parameters() - 参数替换
  │
  ├─→ [2] 设置静态 SQL 标志
  │     ├─ is_static_sql = True
  │     └─ provided_sql = extracted_sql
  │
  ├─→ [3] exe_static_sql(_session, ds, provided_sql)
  │     ├─ extract_tables_from_sql(sql) - 提取表名
  │     ├─ add_table_to_ds(tables) - 添加表到数据源
  │     └─ return full_sql_text, sql
  │
  └─→ [4] 直接执行 SQL
        ├─ execute_sql(sql)
        └─ 跳过图表生成 (chart_type='table')
```

#### 3.3.2 StaticSQLHandler 核心方法

```python
# static_sql_handler.py
class StaticSQLHandler:
    """静态 SQL 处理器"""
    
    def check_static_sql_mode(question: str) -> str:
        """检查并提取静态 SQL"""
        
    def replace_parameters(sql_template: str, parameters: dict) -> str:
        """替换 SQL 参数 (${param} 格式)"""
        
    def extract_tables_from_sql(sql_query: str) -> List[str]:
        """从 SQL 中提取表名 (使用 sqlglot 解析器)"""
        
    def add_table_to_ds(ds, table_name: str) -> bool:
        """添加表到数据源"""
        
    def exe_static_sql(_session, ds, provided_sql) -> tuple:
        """执行静态 SQL"""
```

---

## 4. 数据模型设计

### 4.1 核心数据表

#### 4.1.1 聊天相关

**chat (聊天会话表)**
```sql
CREATE TABLE chat (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    oid BIGINT DEFAULT 1,                    -- 组织 ID
    create_time TIMESTAMP,
    create_by BIGINT,
    brief VARCHAR(64),                       -- 会话摘要
    chat_type VARCHAR(20) DEFAULT 'chat',    -- 类型：chat/datasource
    datasource BIGINT,                       -- 数据源 ID
    engine_type VARCHAR(64),                 -- 数据库类型
    origin INTEGER DEFAULT 0,                -- 来源：0=页面，1=MCP, 2=助手
    brief_generate BOOLEAN DEFAULT FALSE     -- 摘要是否已生成
);
```

**chat_record (聊天记录表)**
```sql
CREATE TABLE chat_record (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    chat_id BIGINT NOT NULL,
    ai_modal_id BIGINT,
    first_chat BOOLEAN DEFAULT FALSE,
    create_time TIMESTAMP,
    finish_time TIMESTAMP,
    create_by BIGINT,
    datasource BIGINT,
    engine_type VARCHAR(64),
    question TEXT,                           -- 用户问题
    sql_answer TEXT,                         -- SQL 生成回答
    sql TEXT,                                -- 最终 SQL
    sql_exec_result TEXT,                    -- SQL 执行结果
    data TEXT,                               -- 查询数据
    chart_answer TEXT,                       -- 图表生成回答
    chart TEXT,                              -- 图表配置
    analysis TEXT,                           -- 分析结果
    predict TEXT,                            -- 预测结果
    recommended_question TEXT,               -- 推荐问题
    finish BOOLEAN DEFAULT FALSE,            -- 是否完成
    error TEXT                               -- 错误信息
);
```

**chat_log (操作日志表)**
```sql
CREATE TABLE chat_log (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    type VARCHAR(3),                         -- 类型：CHAT
    operate VARCHAR(3),                      -- 操作：生成 SQL/图表/分析等
    pid BIGINT,                              -- 父记录 ID
    ai_modal_id BIGINT,                      -- AI 模型 ID
    base_modal VARCHAR(255),                 -- 基础模型名称
    messages JSONB,                          -- 对话消息
    reasoning_content TEXT,                  -- 推理过程
    start_time TIMESTAMP,
    finish_time TIMESTAMP,
    token_usage JSONB                        -- Token 使用情况
);
```

#### 4.1.2 数据源相关

**core_datasource (数据源表)**
```sql
CREATE TABLE core_datasource (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    oid BIGINT DEFAULT 1,                    -- 组织 ID
    name VARCHAR(255),                       -- 数据源名称
    type VARCHAR(64),                        -- 类型：mysql/doris/es 等
    configuration JSONB,                     -- 连接配置 (加密)
    description TEXT,
    status INTEGER DEFAULT 1                 -- 状态：0=禁用，1=启用
);
```

**core_table (数据表配置表)**
```sql
CREATE TABLE core_table (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    ds_id BIGINT NOT NULL,                   -- 数据源 ID
    checked BOOLEAN DEFAULT TRUE,            -- 是否选中
    table_name VARCHAR(255),                 -- 表名
    table_comment TEXT,                      -- 表注释
    custom_comment TEXT,                     -- 自定义注释
    schema_name VARCHAR(255)                 -- Schema 名称
);
```

#### 4.1.3 系统与配置

**aimodel (AI 模型表)**
```sql
CREATE TABLE aimodel (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    model_name VARCHAR(255),                 -- 模型名称
    model_type VARCHAR(64),                  -- 类型：LLM/Embedding
    api_key VARCHAR(512),                    -- API 密钥 (加密)
    api_base VARCHAR(512),                   -- API 地址
    temperature DECIMAL(3,2) DEFAULT 0.7,    -- 温度参数
    max_tokens INTEGER                       -- 最大 Token 数
);
```

**terminology (术语配置表)**
```sql
CREATE TABLE terminology (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    oid BIGINT DEFAULT 1,
    ds_id BIGINT,
    term_name VARCHAR(255),                  -- 术语名称
    description TEXT,                        -- 术语描述 (含业务逻辑/计算规则)
    field_mapping JSONB,                     -- 字段映射
    priority INTEGER DEFAULT 1               -- 优先级
);
```

**data_training (数据训练表)**
```sql
CREATE TABLE data_training (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    oid BIGINT DEFAULT 1,
    ds_id BIGINT,
    training_type VARCHAR(64),               -- 训练类型：sql_example/query_pattern
    content TEXT,                            -- 训练内容
    embedding VECTOR(1536)                   -- 向量嵌入 (用于语义搜索)
);
```

---

## 5. 提示词工程体系

### 5.1 提示词模板结构

**模板文件组织**
```
apps/template/
├── generate_sql/
│   └── generator.py       # SQL 生成提示词
├── generate_chart/
│   └── generator.py       # 图表生成提示词
├── generate_analysis/
│   └── generator.py       # 数据分析提示词
├── filter/
│   └── generator.py       # 权限过滤提示词
└── template.yaml          # 全局模板配置
```

### 5.2 核心提示词模板

#### 5.2.1 SQL 生成提示词

```python
# generate_sql/generator.py
def get_sql_template():
    return {
        'system': """你是一个 SQL 专家，根据用户问题和数据库结构生成 SQL。

# 角色定位
{engine} 数据库专家，精通复杂查询和性能优化。

# 约束条件
1. 只读查询，禁止使用修改数据的语句
2. 默认限制返回条数 (除非用户明确要求更多)
3. 使用表别名提高可读性
4. 优先使用 INNER JOIN
5. 注意处理 NULL 值
6. 时间字段使用合适的格式

# 示例
## 示例 1 - 简单查询
用户问题：查询所有部门
输出：{{"sql": "SELECT * FROM department", "success": true}}

## 示例 2 - 带条件查询
用户问题：查询销售额大于 100 万的订单
输出：{{"sql": "SELECT * FROM orders WHERE amount > 1000000", "success": true}}
""",
        'user': """当前数据库类型：{engine}
表结构：{schema}
用户问题：{question}
当前时间：{current_time}

请生成符合要求的 SQL 查询。"""
    }
```

#### 5.2.2 下钻查询提示词

```python
# generate_sql/generator.py
def get_drill_down_template():
    return {
        'system': """你是下钻分析专家，擅长根据指标血缘关系生成明细查询 SQL。

# 下钻的含义
- **目标字段**: 上一个问题中查询的字段 (通常是聚合指标)
- **下钻查询**: 查询目标字段的来源，即源字段 (明细数据)
- 简单说：**下钻 = 查看目标字段从哪里来 = SELECT 源字段**

# 关键规则
1. 当用户说"下钻指标 XXX"时 = 查看该指标的明细 = SELECT 源字段 (不加聚合)
2. 保留原始 SQL 的 WHERE 条件和维度字段
3. 移除原始 SQL 的聚合函数和 GROUP BY
4. 不要添加新的聚合

# 示例
上个回答：`SELECT SUM(d7_estrus) AS d7_sum FROM ...`
下钻查询：`SELECT d7_estrus FROM ... WHERE org_id = xxx`
""",
        'user': """用户问题：{user_question}

请生成下钻查询 SQL。"""
    }
```

#### 5.2.3 明细查询提示词

```python
# generate_sql/generator.py
def get_view_details_template():
    return {
        'system': """你是明细数据查询专家，根据指标血缘查询来源字段。

# 任务
根据指标的血缘关系，查询其来源字段的详细数据。

# 要求
1. 明确计算字段 (需要追溯来源的字段)
2. 添加合适的维度字段 (如时间、组织等)
3. 保持查询简洁，避免过度聚合
4. 如果有过滤条件，应用到正确的字段上

# 输出格式
{{"sql": "SELECT ...", "success": true, "tables": [...]}}
""",
        'user': """用户问题：{user_question}
计算字段：{calculation_fields}
目标表：{table_name}

请生成明细查询 SQL。"""
    }
```

### 5.3 提示词配置外部化

**YAML 配置示例**
```yaml
# template.yaml
sql_generation:
  system_prompt: |
    你是 SQL 专家...
  
  rules:
    - 只读查询
    - 限制返回条数
    - 使用表别名
  
  examples:
    - question: "查询所有部门"
      sql: "SELECT * FROM department"
    
    - question: "统计各部门销售额"
      sql: "SELECT dept_id, SUM(amount) FROM orders GROUP BY dept_id"

drilldown:
  keywords: ["下钻", "钻取", "drill"]
  bloodline_priority: true
  remove_aggregation: true
```

---

## 6. 扩展功能设计

### 6.1 下钻分析模块

#### 6.1.1 MetricDrilldownHandler

```python
# metric_drilldown_handler.py
class MetricDrilldownHandler:
    """指标下钻分析器"""
    
    def __init__(self):
        self.parser = ParseMDToJson()
    
    @staticmethod
    def handle_drilldown_for_llm(llm_service, question: str, llm_client) -> bool:
        """处理下钻逻辑 (主入口)"""
        
    def get_user_intent_and_table_scope(self, llm_client, question: str) -> Dict[str, Any]:
        """分析用户意图，判断查询范围"""
        
    def extract_metric_blood_from_md(self, table_name: str) -> Dict[str, Any]:
        """从 MD 文档提取血缘数据"""
        
    @staticmethod
    def generate_drilldown_sql_by_llm(llm_service, field_blood: dict):
        """基于血缘信息生成下钻 SQL"""
```

#### 6.1.2 血缘关系数据结构

```json
{
  "field_blood": {
    "d7_sum": [
      {
        "源字段": "yz_datawarehouse_ads.d7_estrus",
        "源表": "yz_datawarehouse_ads.ads_pig_feed_day",
        "转换逻辑": "SUM",
        "字段类型": "指标",
        "聚合方式": "SUM"
      }
    ]
  },
  "table_name": "yz_datawarehouse_ads.ads_pig_feed_day",
  "dependencies": [
    {
      "table": "yz_datawarehouse_ads.ads_pig_feed_day",
      "fields": ["d7_estrus"]
    }
  ]
}
```

### 6.2 静态 SQL 处理器

#### 6.2.1 StaticSQLHandler

```python
# static_sql_handler.py
class StaticSQLHandler:
    """静态 SQL 处理器"""
    
    def check_static_sql_mode(self, question: str) -> str:
        """检查是否为静态 SQL 模式"""
        
    def replace_parameters(self, sql_template: str, parameters: dict) -> str:
        """替换 SQL 参数"""
        
    def extract_tables_from_sql(self, sql_query: str) -> List[str]:
        """从 SQL 中提取表名 (使用 sqlglot)"""
        
    def add_table_to_ds(self, ds, table_name: str) -> bool:
        """添加表到数据源"""
        
    def exe_static_sql(self, _session, ds, provided_sql) -> tuple:
        """执行静态 SQL"""
```

#### 6.2.2 表名提取算法

**使用 sqlglot 解析**
```python
import sqlglot
from sqlglot import exp

def extract_tables_from_sql(self, sql_query: str) -> List[str]:
    """精确提取 SQL 中的表名"""
    tables = set()
    cte_aliases = set()
    
    # 解析 SQL 语法树
    parsed_statements = sqlglot.parse(sql_query)
    
    for statement in parsed_statements:
        # 收集 CTE 别名
        for cte in statement.find_all(exp.CTE):
            cte_aliases.add(cte.alias)
        
        # 提取表名引用
        for table_exp in statement.find_all(exp.Table):
            db = table_exp.args.get('db')
            table = table_exp.args.get('this')
            
            # 排除 CTE 别名
            if table and str(table) not in cte_aliases:
                if db:
                    full_name = f"{db}.{table}"
                    tables.add(full_name.lower())
                else:
                    tables.add(str(table).lower())
    
    return sorted(list(tables))
```

---

## 7. MCP 集成

### 7.1 MCP Server 架构

**配置**
```python
# main.py
mcp = FastApiMCP(
    app,
    name="SQLBot MCP Server",
    description="SQLBot MCP Server",
    include_operations=[
        "get_datasource_list",
        "get_model_list",
        "mcp_question",
        "mcp_start",
        "mcp_assistant"
    ]
)
```

### 7.2 MCP 接口

**mcp_question (MCP 问答接口)**
```python
@app.post("/mcp/mcp_question")
async def mcp_question(params: McpQuestion):
    """MCP 客户端问答接口"""
    # 复用 LLMService 处理逻辑
    llm_service = LLMService(...)
    async for chunk in llm_service.run_task_async():
        yield chunk
```

---

## 8. 安全与权限

### 8.1 认证机制

**TokenMiddleware**
```python
# auth.py
class TokenMiddleware:
    """Token 认证中间件"""
    
    async def __call__(self, request: Request, call_next):
        # 1. 检查请求头中的 Token
        token = request.headers.get("Authorization")
        
        # 2. 验证 Token 有效性
        user = verify_token(token)
        
        # 3. 设置用户上下文
        request.state.current_user = user
        
        # 4. 继续处理请求
        response = await call_next(request)
        return response
```

### 8.2 行级权限控制

**权限过滤流程**
```
生成 SQL → 检查权限 → 添加过滤条件 → 保存最终 SQL
  │
  ├─ generate_filter(_session, sql, tables)
  │   ├─ 获取用户权限配置
  │   ├─ 解析 SQL 中的表
  │   └─ 添加 WHERE 条件
  │
  └─ check_save_sql(session, filtered_sql)
```

**权限配置示例**
```xml
<permissions>
    <table name="sales">
        <row-filter>org_id = '709347917181313024'</row-filter>
        <column-filter>
            <exclude>salary, bonus</exclude>
        </column-filter>
    </table>
</permissions>
```

---

## 9. 性能优化

### 9.1 缓存策略

**Redis 缓存**
```python
# sqlbot_cache.py
class SQLBotCache:
    """SQLBot 缓存管理器"""
    
    @staticmethod
    def cache_chat_result(chat_id: str, result: dict, ttl: int = 3600):
        """缓存聊天结果"""
        
    @staticmethod
    def get_cached_result(chat_id: str) -> Optional[dict]:
        """获取缓存结果"""
        
    @staticmethod
    def invalidate_cache(pattern: str):
        """批量清除缓存"""
```

### 9.2 向量检索优化

**术语语义搜索**
```python
# terminology crud
def get_terminology_template(session, question, oid, ds_id):
    """获取相关术语配置"""
    
    # 1. 将问题转换为向量
    question_embedding = get_embedding(question)
    
    # 2. ES 向量相似度搜索
    search_body = {
        "knn": {
            "field": "embedding",
            "query_vector": question_embedding,
            "k": 10,
            "filter": {"term": {"oid": oid}}
        }
    }
    
    # 3. 返回最相关的术语
    results = es.search(index="terminology", body=search_body)
    return format_results(results)
```

### 9.3 数据库查询优化

**连接池配置**
```python
# config.py
class Settings:
    # 数据库连接池
    DATABASE_POOL_SIZE = 10
    DATABASE_MAX_OVERFLOW = 20
    POOL_TIMEOUT = 30
    POOL_RECYCLE = 1800
    
    # Elasticsearch
    ES_HOST = "localhost"
    ES_PORT = 9200
    ES_INDEX_PREFIX = "sqlbot_"
```

---

## 10. 监控与日志

### 10.1 日志系统

**日志级别与格式**
```python
# utils.py
class SQLBotLogUtil:
    """SQLBot 日志工具"""
    
    @staticmethod
    def info(message: str):
        logging.info(f"[INFO] {message}")
    
    @staticmethod
    def error(message: str):
        logging.error(f"[ERROR] {message}")
    
    @staticmethod
    def warning(message: str):
        logging.warning(f"[WARNING] {message}")
    
    @staticmethod
    def debug(message: str):
        logging.debug(f"[DEBUG] {message}")
```

### 10.2 操作日志追踪

**chat_log 表记录**
```python
# chat/crud/chat.py
def start_log(session, ai_modal_id, operate, record_id, full_message):
    """开始记录操作日志"""
    log = ChatLog(
        type=TypeEnum.CHAT,
        operate=operate,
        pid=record_id,
        ai_modal_id=ai_modal_id,
        messages=full_message,
        start_time=datetime.now()
    )
    session.add(log)
    session.commit()
    return log

def end_log(session, log, full_message, reasoning_content, token_usage):
    """结束记录操作日志"""
    log.finish_time = datetime.now()
    log.messages = full_message
    log.reasoning_content = reasoning_content
    log.token_usage = token_usage
    session.commit()
```

---

## 11. 部署架构

### 11.1 Docker Compose 部署

```yaml
# docker-compose.yaml
version: '3.8'

services:
  backend:
    build: ./backend
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://user:pass@postgres:5432/sqlbot
      - REDIS_URL=redis://redis:6379
      - ES_HOST=elasticsearch
    depends_on:
      - postgres
      - redis
      - elasticsearch
  
  frontend:
    build: ./frontend
    ports:
      - "80:80"
    depends_on:
      - backend
  
  postgres:
    image: postgres:15
    volumes:
      - pgdata:/var/lib/postgresql/data
  
  redis:
    image: redis:7
    volumes:
      - redisdata:/data
  
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
    volumes:
      - esdata:/usr/share/elasticsearch/data

volumes:
  pgdata:
  redisdata:
  esdata:
```

### 11.2 环境变量配置

```bash
# .env
# 数据库
DATABASE_URL=postgresql://user:password@localhost:5432/sqlbot

# Redis
REDIS_URL=redis://localhost:6379

# Elasticsearch
ES_HOST=localhost
ES_PORT=9200

# AI 模型
DASHSCOPE_API_KEY=your_api_key
DASHSCOPE_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
DASHSCOPE_MODEL=qwen-plus

# JWT 配置
JWT_SECRET_KEY=your_secret_key
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
```

---

## 12. 开发规范

### 12.1 代码风格

**命名规范**
- **类名**: PascalCase (如 `LLMService`, `StaticSQLHandler`)
- **函数/方法**: snake_case (如 `generate_sql`, `extract_tables`)
- **常量**: UPPER_SNAKE_CASE (如 `CHAT_FINISH_STEP`)
- **私有方法**: 单下划线前缀 (如 `_build_prompt`)

**类型注解**
```python
from typing import Dict, List, Optional, Any

def process_data(
    items: List[Dict[str, Any]],
    limit: Optional[int] = None
) -> Dict[str, Any]:
    """处理数据"""
```

### 12.2 错误处理

**统一异常类**
```python
# error.py
class SingleMessageError(Exception):
    """单条错误消息"""

class SQLBotDBConnectionError(Exception):
    """数据库连接错误"""

class SQLBotDBError(Exception):
    """数据库执行错误"""
```

**异常处理模式**
```python
try:
    result = execute_sql(sql)
except SingleMessageError as e:
    # 业务逻辑错误
    raise e
except Exception as e:
    # 系统错误
    SQLBotLogUtil.error(f"Execute SQL failed: {str(e)}")
    raise SQLBotDBError(str(e))
```

---

## 13. 未来规划

### 13.1 功能增强

- [ ] 多轮对话上下文优化
- [ ] 复杂查询自动分解
- [ ] 查询结果自动解释
- [ ] 智能图表推荐
- [ ] 数据预测与趋势分析

### 13.2 性能提升

- [ ] 查询缓存命中率优化
- [ ] 大模型响应时间优化
- [ ] 批量查询并行处理
- [ ] 增量数据同步

### 13.3 生态扩展

- [ ] 更多数据源类型支持
- [ ] 第三方 BI 工具集成
- [ ] 自定义插件系统
- [ ] 开放 API 市场

---

## 附录

### A. 关键依赖版本

```toml
# pyproject.toml
[tool.poetry.dependencies]
python = "^3.11"
fastapi = "^0.109.0"
sqlmodel = "^0.0.14"
sqlalchemy = "^2.0.25"
langchain = "^0.1.0"
openai = "^1.10.0"
psycopg2-binary = "^2.9.9"
redis = "^5.0.1"
elasticsearch = "^8.11.0"
sqlglot = "^19.0.0"
```

### B. 常用命令

```bash
# 启动开发服务器
cd backend
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# 数据库迁移
cd backend
alembic upgrade head

# 运行测试
cd backend
pytest

# 构建 Docker 镜像
docker-compose build

# 启动所有服务
docker-compose up -d
```

### C. 联系方式

- **项目仓库**: [内部仓库]
- **文档地址**: [内部 Wiki]
- **技术支持**: tech-team@company.com

---

**文档版本**: v1.0  
**最后更新**: 2026-03-19  
**维护团队**: SQLBot 开发团队
