# SQLBot Extend 扩展模块功能详细说明

## 📋 目录

1. [模块概览](#模块概览)
2. [Chat Manager - 会话状态管理](#1-chat-manager---会话状态管理)
3. [Metric Metadata - 指标元数据管理](#2-metric-metadata---指标元数据管理)
4. [Drilldown - 下钻分析引擎](#3-drilldown---下钻分析引擎)
5. [SQL Engine - SQL 校验引擎](#4-sql-engine---sql-校验引擎)
6. [Static SQL Handler - 静态 SQL 处理器](#5-static-sql-handler---静态-sql-处理器)
7. [Format - SQL 格式化工具](#6-format---sql-格式化工具)
8. [Metric Blood - 指标血缘关系](#7-metric-blood---指标血缘关系)
9. [Utils & YAML - 工具与配置](#8-utils--yaml---工具与配置)
10. [架构设计原则](#架构设计原则)

---

## 模块概览

`apps/extend` 是 SQLBot 的**扩展功能模块**，提供核心业务逻辑之外的增强功能。采用模块化设计，每个子模块独立负责特定领域的功能。

### 模块清单

| 模块名 | 职责 | 核心技术 |
|--------|------|----------|
| **chat_manager** | 多轮对话上下文管理 | LLM 意图识别、历史补充 |
| **metric_metadata** | 指标元数据 CRUD + 向量检索 | PostgreSQL Vector、Embedding |
| **drilldown** | 智能下钻分析 | 规则引擎、血缘解析 |
| **sql_engine** | SQL 合法性校验与修复 | 正则匹配、LLM 自动修复 |
| **static** | 静态 SQL 直接执行 | sqlglot 解析、参数替换 |
| **format** | SQL 转 Markdown 文档 | MD 表格解析、结构化输出 |
| **metric_blood** | 指标血缘关系文档 | MD 文档存储、字段级血缘 |
| **utils** | 通用工具函数 | 日志、数据库辅助 |
| **yaml** | 提示词模板配置 | YAML 配置化管理 |

---

## 1. Chat Manager - 会话状态管理

### 1.1 功能定位

解决**多轮对话中的上下文保持问题**，实现：
- ✅ 从用户问题中智能提取指标、维度、过滤条件
- ✅ 当问题信息不足时，自动从聊天历史中补充
- ✅ 维护会话级别的上下文状态（metrics/dimensions/filters）

### 1.2 核心组件

#### 1.2.1 数据模型

**表名：** `chat_state`

**主要字段：**
- `chat_id`：会话 ID（主键）
- `metrics`：最新指标名称（中文）
- `dimensions`：维度列表（JSONB）
- `filters`：过滤条件列表（JSONB）
- `tables`：涉及表名列表（JSONB）
- `resolved_names`：术语映射字典（JSONB）
- `context`：其他上下文信息（JSONB）

**设计特点：**
- 一个 `chat_id` 只保留**最新一条记录**（先删后插策略）
- 使用 JSONB 类型存储复杂结构，支持灵活扩展

#### 1.2.2 聊天服务

**核心功能：**

从用户问题中提取指标、维度和过滤条件。

**处理流程：**
1. 调用 LLM 提取字段
2. 如果提取失败，从 chat_state 历史中补充
3. 返回结构化结果

**返回格式：**
```json
{
    "metrics": ["销售额"],
    "dimensions": ["日期", "地区"],
    "filters": ["时间范围=最近一个月"],
    "from_history": false
}
```

**实现细节：**
- 使用专门的 Prompt 让 LLM 识别指标/维度/过滤条件
- 降级策略：LLM 失败时使用规则匹配或历史数据
- 支持指代消解（如"那上海的呢？" → 替换地区过滤条件）

#### 1.2.3 状态服务

提供历史状态查询和合并功能：
- 从最近 N 轮对话中提取状态
- 合并当前状态和历史状态

### 1.3 典型应用场景

#### 场景 1：多轮追问

```
用户第一轮："查看北京 2024 年的销售额"
→ 系统提取：metrics="销售额", filters=["地区=北京", "年份=2024"]

用户第二轮："那上海的呢？"
→ 系统识别指代：从历史获取 metrics="销售额", filters=["地区=上海", "年份=2024"]
```

#### 场景 2：省略主语

```
用户第一轮："按月统计订单量"
→ metrics="订单量", dimensions=["月"]

用户第二轮："按地区呢？"
→ 从历史补充 metrics="订单量", dimensions=["地区"]
```

### 1.4 API 接口

```
GET    /extend/chat-manager/state/{chat_id}           # 查询状态
POST   /extend/chat-manager/state                     # 创建/更新状态
GET    /extend/chat-manager/state/history/{chat_id}   # 查询历史
DELETE /extend/chat-manager/state/{chat_id}           # 清空状态
```

---

## 2. Metric Metadata - 指标元数据管理

### 2.1 功能定位

管理**业务指标的元数据信息**，支持：
- ✅ 指标定义（名称、计算逻辑、数据来源）
- ✅ 数仓分层（ODS/DWD/DWS/ADS）
- ✅ 向量化语义检索（相似度匹配）
- ✅ 批量导入/导出

### 2.2 数据模型

**表名：** `metric_metadata`

**主要字段：**
- `id`：主键 ID
- `metric_name`：标准指标名称（如：销售额）
- `synonyms`：同义词（逗号分隔）
- `datasource_id`：数据源 ID
- `table_name`：物理表名
- `core_fields`：核心字段（逗号分隔）
- `calc_logic`：计算逻辑（如：SUM(amount)）
- `upstream_table`：上游表（用于下钻）
- `dw_layer`：数仓分层（ODS/DWD/DWS/ADS）
- `embedding_vector`：向量嵌入（语义检索）

**唯一性约束：** `(metric_name, table_name, datasource_id)`

### 2.3 核心功能

#### 2.3.1 单个/批量创建

**功能特性：**
- 自动计算 embedding_vector（如果启用）
- 检查重复（metric_name + table_name + datasource_id）
- 失败不影响主流程（embedding 错误仅记录日志）

**批量创建返回统计：**
```json
{
    "success_count": 10,
    "failed_records": [],
    "duplicate_count": 2,
    "update_count": 3
}
```

#### 2.3.2 向量检索

**三级混合搜索模式：**

1. **hybrid**（默认）：精准 → 模糊 → 向量，逐级匹配未命中文本
2. **exact**：仅精准匹配（dim_name == search_text）
3. **fuzzy**：仅模糊匹配（LIKE %search_text%）
4. **vector**：仅向量语义搜索（余弦相似度）

**返回结果：**
- `results`：匹配到的维度列表
- `all_matched`：是否所有搜索文本都成功匹配（布尔值）

**Hybrid 模式工作流程：**

```
输入：['断奶时间', '耳号', '配种时间']

第 1 级：精准匹配
  ├─ 匹配到：['母猪耳号'] ← '耳号'
  └─ 剩余：['断奶时间', '配种时间']

第 2 级：模糊匹配
  ├─ 匹配到：['最佳配种时间'] ← '配种时间'
  └─ 剩余：['断奶时间']

第 3 级：向量搜索
  └─ 匹配到：['断奶日期'] ← '断奶时间'（相似度 0.8857）

输出：
  results = [母猪耳号, 最佳配种时间, 断奶日期]
  all_matched = True  # 全部匹配成功
```

**性能优化：**
- 精准匹配：使用 `IN` 子句批量查询
- 模糊匹配：使用 `OR` 连接多个 `LIKE` 条件
- 向量搜索：使用 PostgreSQL `<=>` 运算符计算余弦距离

### 2.4 数仓分层路由

**优先 ADS 层策略：**

```python
# llm.py 中的路由逻辑
for metric in metric_info_list:
    # 尝试在 ADS 表中查找维度
    results, all_matched = search_metric_dimensions(
        session, dimensions, embedding_model, metric.table_name
    )
    
    if all_matched:
        # ADS 表满足需求，直接使用（高性能）
        table_name_list.append(metric.table_name)
    else:
        # ADS 表缺少维度，降级到 DWS 上游表
        results, all_matched = search_metric_dimensions(
            session, dimensions, embedding_model, metric.upstream_table
        )
        if all_matched:
            table_name_list.append(metric.upstream_table)
```

**⚠️ 重要规则：calc_logic 禁止使用表别名**

```python
# ❌ 错误示例
calc_logic="COUNT(DISTINCT a.order_id)"  # LLM 无法识别 'a' 是哪个表

# ✅ 正确示例
calc_logic="COUNT(DISTINCT order_id)"    # 直接使用字段名
```

**原因：**
1. LLM 生成下钻 SQL 时参考 `calc_logic`
2. 如果包含别名，LLM 无法确定别名对应的表
3. 导致生成的 SQL 出现语法错误或逻辑错误

### 2.5 API 接口

```
PUT    /extend/metric-metadata                    # 创建/更新单个指标
POST   /extend/metric-metadata/batch              # 批量创建
GET    /extend/metric-metadata/{id}               # 查询详情
GET    /extend/metric-metadata/page/{page}/{size} # 分页查询
GET    /extend/metric-metadata/list               # 列表查询
DELETE /extend/metric-metadata?ids=1,2,3         # 删除
POST   /extend/metric-metadata/fill-embeddings    # 填充向量
```

---

## 3. Drilldown - 下钻分析引擎

### 3.1 功能定位

实现**智能下钻分析**，支持：
- ✅ 粒度下钻：按维度拆分汇总数据（如"按月下钻销售额"）
- ✅ 穿透下钻：查看明细原始数据（如"查看 d7_sum 的明细"）
- ✅ 自动判断表范围：当前表 vs 上游表
- ✅ 规则引擎约束：严格遵守数仓分层聚合规则

### 3.2 核心组件

#### 3.2.1 聚合规则引擎

**优先级顺序：**
1. 最高：关键词判断（"明细" > "汇总"）
2. 次高：下钻类型（is_granular / is_raw）
3. 兜底：数仓分层（DWS/ADS 强制聚合，DWD/ODS 禁止聚合）

**核心功能：**
- `judge_drill_type()`：判断下钻类型（粒度下钻 / 穿透下钻 / 不下钻）
- `get_agg_rule()`：计算聚合规则（是否需要聚合函数）
- `build_final_prompt()`：构建最终 Prompt（注入到 custom_prompt）

**示例输出：**
```
【系统强制聚合规则（不可违反）】
1. 汇总层 (ADS/DWS) 必须使用聚合函数+GROUP BY；
2. 明细层 (DWD/ODS) 禁止任何聚合函数；
3. 严格按指令生成 SQL，禁止自主推断；
4. 规则冲突时，优先执行【不聚合】策略。

【当前指令】不聚合，查原始明细，分层：ADS
```

**关键词库（可配置在 YAML 中）：**
- **粒度下钻**：下钻、钻取、拆分、细分
- **穿透下钻**：明细、原始数据、详细、为什么
- **聚合**：汇总、统计、合计、总计

#### 3.2.2 指标下钻处理器

**核心功能：**

1. **extract_metrics_and_intent()**：同时提取指标名称和用户意图
   - 返回指标列表和意图字典（包含下钻类型、是否需要聚合等）

2. **judge_table_scope()**：判断表范围（当前表还是上游表）
   - 从 MD 文档提取字段血缘
   - 构建血缘描述
   - LLM 根据血缘和意图判断
   - 返回目标表列表和判断理由

#### 3.2.3 MD 文档解析器

**功能：** 解析 Markdown 格式的指标血缘文档

**输入格式：** Markdown 表格（包含字段ID、名称、源表、源字段、计算公式等）

**输出格式：**
```json
{
    "field_blood": {
        "d7_sum": [{
            "源字段": "ads_table.d7_estrus",
            "源表": "ads_table",
            "转换逻辑": "SUM",
            "字段类型": "指标"
        }]
    },
    "dependencies": [
        {"table": "ads_table", "fields": ["d7_estrus"]}
    ]
}
```

### 3.3 完整执行流程

#### 场景 1：粒度下钻（汇总）

```
用户问题："按月下钻指标 d7_sum"

1. extract_metrics_and_intent()
   ├─ 提取指标：["d7_sum"]
   └─ 判断意图：is_granular=True, need_agg=True

2. 获取元数据：dw_layer="ADS"

3. build_final_prompt()
   └─ 注入规则："聚合，按维度拆分，分层：ADS"

4. judge_table_scope()
   └─ 输出：is_current_table=True（查询汇总表）

5. LLM 生成 SQL（受规则约束）：
   SELECT dt_month, SUM(d7_sum) 
   FROM ads_algo_female_batch_production
   GROUP BY dt_month
```

#### 场景 2：穿透下钻（明细）

```
用户问题："查看 d7_sum 的明细数据"

1. extract_metrics_and_intent()
   └─ 判断意图：is_raw=True, need_agg=False

2. 获取元数据：dw_layer="ADS"

3. build_final_prompt()
   └─ 注入规则："不聚合，查原始明细，分层：ADS"
      （规则 4：冲突时优先"不聚合"）

4. judge_table_scope()
   ├─ 解析 MD 血缘：
   │   d7_sum ← feed_count (SUM) [源表：dwd_feed_detail]
   └─ 输出：is_current_table=False
           target_tables=["dwd_feed_detail"]

5. 添加上游表到数据源

6. LLM 生成 SQL（无聚合）：
   SELECT feed_count, dt_date
   FROM dwd_feed_detail
   WHERE dt_month = '2024-01'
```

### 3.4 集成到 LLMService

**修改位置：** `apps/chat/task/llm.py`

```python
# 第 1169-1170 行：提取指标和意图
metrics, user_intent = self.metric_drilldown.extract_metrics_and_intent(
    self.llm, self.chat_question.question
)

# 第 1196-1213 行：注入聚合规则
if metric_info_list:
    curr_layer = metric_info_list[0].dw_layer or "unknown"
    agg_rule_prompt = self.metric_drilldown.rule_engine.build_final_prompt(
        question=self.chat_question.question,
        curr_layer=curr_layer
    )
    self.chat_question.custom_prompt += f"\n\n{agg_rule_prompt}"

# 第 1215-1235 行：判断表范围并添加上游表
table_scope = self.metric_drilldown.judge_table_scope(...)
if not table_scope['is_current_table']:
    self._batch_add_tables_to_ds(_session, table_scope['target_tables'])
    # 重新获取 schema（包含上游表）
```

---

## 4. SQL Engine - SQL 校验引擎

### 4.1 功能定位

**自动校验和修复 SQL**，确保符合数仓规范：
- ✅ 拦截 DWD/ODS 层查询（数据安全）
- ✅ 强制 ADS/DWS 层聚合（性能优化）
- ✅ LLM 自动修复错误 SQL

### 4.2 核心类

**主要方法：**

1. **validate_sql()**：校验 SQL 是否符合规则
   - DWD/ODS 层拦截 → LAYER_VIOLATION（不调用 LLM）
   - ADS/DWS 层聚合检查 → MISSING_CLAUSE（调用 LLM 修复）
   - 返回：(是否通过, 错误类型, 错误信息)

2. **retry_generate()**：调用 LLM 修复 SQL
   - 输入：原始 SQL + 错误信息
   - 输出：修复后的 SQL（JSON 格式）

### 4.3 错误类型分类

| 错误类型 | 含义 | 是否调用 LLM | 处理方式 |
|---------|------|------------|---------|
| `LAYER_VIOLATION` | 查询 DWD/ODS 明细层 | ❌ 否 | 重新选择表 |
| `MISSING_CLAUSE` | 缺少 GROUP BY 或聚合函数 | ✅ 是 | LLM 自动修复 |
| `EMPTY_SQL` | SQL 为空 | ❌ 否 | 直接报错 |

### 4.4 校验规则详解

#### 规则 1：DWD/ODS 层拦截

**匹配方式：** 正则表达式 `(DWD|ODS)[_\.]`

**原因：**
- DWD/ODS 是明细层，数据量大
- 直接查询会导致性能问题
- 应使用 ADS/DWS 汇总层

#### 规则 2：ADS/DWS 层聚合检查

**检查项：**
1. 是否有 GROUP BY 子句
2. 是否有聚合函数（SUM, COUNT, AVG, MAX, MIN 等）

**严格校验：** 必须同时包含 GROUP BY 和聚合函数

**支持的聚合函数：**
- `SUM`, `COUNT`, `AVG`, `MAX`, `MIN`
- `GROUP_CONCAT` (MySQL)
- `ARRAY_AGG`, `STRING_AGG` (PostgreSQL)

### 4.5 集成流程

在 LLMService 中集成 SQLValidator：

1. 生成 SQL 后调用 `validate_sql()` 校验
2. 如果校验失败：
   - `LAYER_VIOLATION`：不调用 LLM，直接返回错误
   - `MISSING_CLAUSE`：调用 `retry_generate()` 修复 SQL
3. 如果修复成功，使用修复后的 SQL

---

## 5. Static SQL Handler - 静态 SQL 处理器

### 5.1 功能定位

支持**直接执行预定义 SQL**，绕过 LLM 生成：
- ✅ 参数化 SQL 模板（`${param}` 占位符）
- ✅ 自动提取表名并添加到数据源
- ✅ 精确解析 SQL 语法树（sqlglot）

### 5.2 触发方式

**用户问题格式：**

```
#FIXED_SQL_START#{"sql": "SELECT...", "in_parm": {...}}#FIXED_SQL_END#
```

**示例：**

```json
{
  "sql": "SELECT * FROM orders WHERE org_id = '${org_id}' AND dt >= '${start_date}'",
  "in_parm": {
    "org_id": "709347917181313024",
    "start_date": "2024-01-01"
  }
}
```

### 5.3 核心方法

1. **check_static_sql_mode()**：检查并提取静态 SQL
   - 正则提取 `#FIXED_SQL_START#...#FIXED_SQL_END#` 之间的内容
   - JSON 解析
   - 参数替换（如果有 in_parm）
   - 返回：替换参数后的 SQL 字符串

2. **replace_parameters()**：替换 SQL 参数
   - 支持 `${param}` 格式占位符
   - 示例：`SELECT * FROM t WHERE id = '${id}'` + `{"id": "123"}` → `SELECT * FROM t WHERE id = '123'`

3. **extract_tables_from_sql()**：从 SQL 中提取完整表名
   - 使用 sqlglot 精确解析
   - 排除 CTE 别名
   - 返回：schema.table 格式的表名列表

4. **exe_static_sql()**：执行静态 SQL
   - 提取表名
   - 添加表到数据源
   - 返回完整 SQL 文本

### 5.4 sqlglot 解析优势

**对比正则表达式：**

| 特性 | 正则表达式 | sqlglot |
|------|-----------|---------|
| CTE 别名处理 | ❌ 无法区分 | ✅ 自动排除 |
| 嵌套子查询 | ❌ 容易误判 | ✅ 精确解析 |
| Schema 分离 | ❌ 可能提取单独 schema | ✅ 只提取完整表名 |
| 多数据库方言 | ❌ 需要自定义规则 | ✅ 内置支持 |

**示例：**

```sql
-- 复杂 SQL
WITH cte AS (SELECT * FROM orders)
SELECT * FROM cte JOIN yz_datawarehouse_dws.dws_user ON ...

-- sqlglot 提取结果
['yz_datawarehouse_dws.dws_user']  # ✅ 排除 CTE 'cte'

-- 正则可能误提取
['cte', 'orders', 'yz_datawarehouse_dws.dws_user']  # ❌ 包含 CTE
```

---

## 6. Format - SQL 格式化工具

### 6.1 功能定位

将 **SQL 转换为结构化 Markdown 文档**，用于：
- ✅ 指标血缘关系文档化
- ✅ 字段级血缘追踪
- ✅ 自动生成技术文档

### 6.2 核心类

**SQLToMDAnalyzer**：SQL 转 Markdown 分析器

**输出格式：**
- 表基本信息（数仓分层、更新时间等）
- 字段清单表格（字段ID、名称、类型、源表、计算公式等）

**分析步骤：**
1. 解析 SELECT 子句中的所有字段
2. 分类为维度/指标
3. 提取字段血缘（源表.字段 → 目标表.字段）
4. 推断计算公式
5. 标识跨表计算（is_cross_table）

### 6.3 字段分类规则

**维度（dimension）：**
- 时间字段（dt, date, month, year）
- 组织字段（org_id, org_name）
- 分类字段（category, type）
- 直接映射字段（无聚合函数）

**指标（metric）：**
- 聚合函数字段（SUM, COUNT, AVG）
- 计算字段（(revenue - cost) / revenue）
- 衍生指标（转化率、增长率）

**指标类型：**
- `atomic`：原子指标（直接来自源表）
- `derived`：衍生指标（单表聚合）
- `composite`：复合指标（多表计算）

### 6.4 使用场景

```python
# 1. 解析 ADS 层 SQL
analyzer = SQLToMDAnalyzer()
result = analyzer.analyze_sql(
    sql="SELECT dt_month, SUM(amount) FROM ads_order GROUP BY dt_month",
    table_name="yz_datawarehouse_ads.ads_order_summary"
)

# 2. 保存为 MD 文档
save_to_markdown(result, "yz_datawarehouse_ads.ads_order_summary.md")

# 3. 后续用于下钻分析的血缘解析
```

---

## 7. Metric Blood - 指标血缘关系

### 7.1 功能定位

存储和管理**字段级血缘关系文档**，支持：
- ✅ ADS/DWS 层指标的来源追溯
- ✅ 下钻分析时的表范围判断
- ✅ 影响分析（上游变更影响下游）

### 7.2 文档结构

**目录组织：**

```
metric_blood/
├── yz_datawarehouse_ads/          # ADS 层
│   ├── ads_order_summary.md
│   ├── ads_user_profile.md
│   └── ...
├── yz_datawarehouse_dws/          # DWS 层
│   ├── dws_order_detail.md
│   └── ...
├── yz_datawarehouse_dwd/          # DWD 层
│   └── ...
└── yz_datawarehouse_dim/          # DIM 层
    └── ...
```

**MD 文档示例：**

```markdown
# 表名：yz_datawarehouse_ads.ads_order_summary

## 基本信息
- 数仓分层：ADS
- 上游表：yz_datawarehouse_dws.dws_order_detail
- 更新时间：2024-01-01

## 字段清单

| field_id | field_name | field_type | calculation_type | formula | source_tables | source_fields |
|----------|------------|------------|------------------|---------|---------------|---------------|
| f001     | order_cnt  | 指标       | derived          | COUNT(*) | dws_order     | order_id      |
| f002     | total_amt  | 指标       | derived          | SUM(amount) | dws_order  | amount        |
| f003     | dt_month   | 维度       | direct           | DATE_FORMAT(dt, '%Y-%m') | dws_order | dt |

## 依赖关系
- yz_datawarehouse_dws.dws_order_detail
```

### 7.3 血缘数据结构

```json
{
  "field_blood": {
    "order_cnt": [{
      "源字段": "dws_order.order_id",
      "源表": "yz_datawarehouse_dws.dws_order_detail",
      "转换逻辑": "COUNT",
      "字段类型": "指标",
      "聚合方式": "COUNT"
    }],
    "total_amt": [{
      "源字段": "dws_order.amount",
      "源表": "yz_datawarehouse_dws.dws_order_detail",
      "转换逻辑": "SUM",
      "字段类型": "指标",
      "聚合方式": "SUM"
    }]
  },
  "table_name": "yz_datawarehouse_ads.ads_order_summary",
  "dependencies": [
    {"table": "yz_datawarehouse_dws.dws_order_detail", "fields": ["order_id", "amount"]}
  ]
}
```

### 7.4 应用场景

**下钻分析时的血缘查询：**

```python
# 1. 用户问："查看 order_cnt 的明细"
question = "查看 order_cnt 的明细"

# 2. 解析 MD 文档获取血缘
blood_data = parse_md_to_json("yz_datawarehouse_ads.ads_order_summary.md")

# 3. 提取依赖表
dependencies = blood_data['dependencies']
# → ['yz_datawarehouse_dws.dws_order_detail']

# 4. 添加上游表到数据源
add_table_to_ds(ds, dependencies)

# 5. LLM 生成明细查询 SQL（不使用聚合）
```

---

## 8. Utils & YAML - 工具与配置

### 8.1 工具函数 (`utils/utils.py`)

```python
class Utils:
    @staticmethod
    def create_local_session():
        """创建本地数据库会话（测试用）"""
        
    @staticmethod
    def format_time(timestamp):
        """格式化时间戳"""
        
    @staticmethod
    def safe_json_loads(json_str):
        """安全的 JSON 解析（容错处理）"""
```

### 8.2 日志工具 (`common/utils/utils.py`)

```python
class SQLBotLogUtil:
    @staticmethod
    def info(message: str):
        logging.info(f"[INFO] {message}")
        
    @staticmethod
    def error(message: str):
        logging.error(f"[ERROR] {message}")
        
    @staticmethod
    def warning(message: str):
        logging.warning(f"[WARNING] {message}")
```

### 8.3 YAML 配置 (`yaml/`)

#### 8.3.1 提示词模板 (`prompt.yaml`)

```yaml
sql_generation:
  system_prompt: |
    你是 SQL 专家，根据用户问题和数据库结构生成 SQL。
    
  rules:
    - 只读查询，禁止修改数据
    - 默认限制返回条数
    - 使用表别名提高可读性
    
  examples:
    - question: "查询所有部门"
      sql: "SELECT * FROM department"
```

#### 8.3.2 规则关键词 (`rule_keyword.yaml`)

```yaml
drilldown:
  granular_keywords: ["下钻", "钻取", "拆分", "细分"]
  detail_keywords: ["明细", "原始数据", "详细", "为什么"]
  aggregate_keywords: ["汇总", "统计", "合计", "总计"]
```

#### 8.3.3 SQL 分析模板 (`sql_analysis.yaml`)

```yaml
system: |
  你是一名数据治理专家，精通 SQL 解析、指标血缘分析。
  
metric_blood: |
  ## 【任务目标】
  1. 识别 SQL 中的所有目标字段
  2. 分类每个字段为维度或指标
  3. 提取字段级血缘
  4. 推断字段口径
```

---

## 架构设计原则

### 1. 模块化设计

**职责分离：**
- 每个子模块独立负责特定领域
- 模块间通过明确接口通信
- 避免循环依赖

**示例：**
```
chat_manager → 管理会话状态
metric_metadata → 管理指标元数据
drilldown → 处理下钻逻辑
sql_engine → 校验 SQL
static → 执行静态 SQL
```

### 2. 依赖注入

**统一依赖：**
```python
from common.core.deps import SessionDep, Trans, CurrentUser

def some_function(session: SessionDep, trans: Trans):
    """自动注入数据库会话和翻译器"""
```

**好处：**
- 便于测试（Mock 依赖）
- 统一管理资源生命周期
- 减少全局变量

### 3. 降级策略

**多层降级：**
```python
# 1. 优先 LLM 提取
result = llm_extract(question)

# 2. LLM 失败 → 规则匹配
if not result:
    result = rule_based_extract(question)

# 3. 规则失败 → 历史补充
if not result:
    result = get_from_history(chat_id)
```

### 4. 配置外部化

**YAML 管理：**
- 提示词模板 → `yaml/prompt.yaml`
- 规则关键词 → `yaml/rule_keyword.yaml`
- 分析模板 → `yaml/sql_analysis.yaml`

**好处：**
- 无需修改代码即可调整行为
- 便于 A/B 测试
- 支持动态加载

### 5. 性能优化

**关键优化点：**

1. **批量操作：**
   ```python
   # ❌ 逐个添加
   for table in tables:
       add_table_to_ds(table)
   
   # ✅ 批量添加
   batch_add_tables_to_ds(tables)
   ```

2. **向量检索优化：**
   - 精准匹配：`IN` 子句
   - 模糊匹配：`OR` 连接 `LIKE`
   - 向量搜索：PostgreSQL `<=>` 运算符

3. **缓存策略：**
   - Redis 缓存会话状态
   - 内存缓存 Embedding 模型
   - 数据库索引优化

### 6. 错误处理

**统一异常处理：**
```python
try:
    result = process()
except Exception as e:
    SQLBotLogUtil.error(f"处理失败：{e}")
    traceback.print_exc()
    return None  # 降级返回
```

**分级错误：**
- `LAYER_VIOLATION`：不调用 LLM，直接报错
- `MISSING_CLAUSE`：调用 LLM 修复
- `EMBEDDING_ERROR`：记录日志，不影响主流程

---

## 总结

`apps/extend` 模块是 SQLBot 的**核心扩展能力**，提供：

1. **智能对话管理**：多轮上下文保持、指代消解
2. **指标元数据管理**：CRUD + 向量检索 + 数仓分层
3. **下钻分析引擎**：规则约束 + 血缘解析 + 表范围判断
4. **SQL 校验修复**：自动拦截违规 SQL + LLM 修复
5. **静态 SQL 执行**：参数化模板 + 精确表名提取
6. **文档化工具**：SQL 转 MD + 血缘关系存储

**核心价值：**
- ✅ 提升 SQL 生成准确性（规则约束）
- ✅ 改善用户体验（多轮对话、智能下钻）
- ✅ 保障数据安全（层级拦截、权限控制）
- ✅ 提高开发效率（配置化、模块化）

**未来优化方向：**
1. 多指标合并（自动 JOIN 不同表的指标）
2. 血缘缓存（避免重复解析 MD 文档）
3. 规则自学习（根据反馈优化关键词库）
4. 性能监控（统计规则命中率和 SQL 准确率）
