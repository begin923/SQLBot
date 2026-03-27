# Metric Metadata - 指标元数据管理模块

## 📋 功能概述

基于 `terminology` 模块的设计模式，为 `metric_metadata` 表提供完整的数据写入和管理功能。

### 核心特性

- ✅ **单个/批量创建**：支持单条插入和批量导入
- ✅ **自动 Embedding**：自动计算指标向量用于语义检索
- ✅ **数仓分层**：支持 ODS/DWD/DWS/ADS 分层管理
- ✅ **血缘关系**：记录上游表关联关系
- ✅ **同义词支持**：支持指标同义词配置
- ✅ **本地测试**：提供完整的测试接口

---

## 🗂️ 目录结构

```
backend/apps/extend/metric_metadata/
├── __init__.py              # 模块初始化
├── models/
│   └── metric_metadata_model.py  # 数据模型定义
├── curd/
│   └── metric_metadata.py        # CRUD 操作逻辑
├── api/
│   └── metric_metadata.py        # API 接口定义
├── sample_data.json         # 示例数据
└── README.md               # 使用说明
```

---

## 🚀 快速开始

### 1. 启动服务后访问测试接口

```bash
# 查看示例数据
curl http://localhost:8000/extend/metric-metadata/test/sample-data

# 插入示例数据到数据库
curl -X POST http://localhost:8000/extend/metric-metadata/test/insert-sample

# 查询所有数据
curl http://localhost:8000/extend/metric-metadata/list

# 分页查询
curl http://localhost:8000/extend/metric-metadata/page/1/10

# 按名称模糊查询
curl "http://localhost:8000/extend/metric-metadata/test/query?metric_name=销售"

# 填充 embedding（需要先配置 EMBEDDING_ENABLED=true）
curl -X POST http://localhost:8000/extend/metric-metadata/fill-embeddings

# 清空测试数据（危险操作！）
curl -X DELETE http://localhost:8000/extend/metric-metadata/test/clear-all
```

---

## 📊 数据模型

### MetricMetadata 表结构

| 字段名 | 类型 | 说明 |
|--------|------|------|
| id | BIGSERIAL | 主键 ID |
| metric_name | VARCHAR(100) | 标准指标名称（如：销售额） |
| synonyms | TEXT | 指标同义词，逗号分隔 |
| datasource_id | BIGINT | 对应 SQLBot 的数据源 id |
| table_name | VARCHAR(100) | 指标所属的物理表名 |
| core_fields | TEXT | 指标核心字段，逗号分隔 |
| calc_logic | TEXT | 指标计算逻辑（如：sum(amt)/count(id)） |
| upstream_table | VARCHAR(100) | 上游关联表名（数仓分层下钻使用） |
| dw_layer | VARCHAR(20) | 数仓分层（ODS/DWD/DWS/ADS） |
| embedding_vector | VECTOR | 指标向量值，用于语义相似度检索 |
| create_time | TIMESTAMP | 记录创建时间 |

---

## 💡 使用示例

### 单个创建

```python
import requests

data = {
    "metric_name": "销售额",
    "synonyms": "营收，销售收入，卖钱额",
    "datasource_id": 1,
    "table_name": "orders",
    "core_fields": "order_id, amount, create_time",
    "calc_logic": "SUM(amount)",
    "upstream_table": "order_items",
    "dw_layer": "DWS"
}

response = requests.put(
    "http://localhost:8000/extend/metric-metadata",
    json=data
)
print(response.json())
```

### 批量创建

```python
import requests

data_list = [
    {
        "metric_name": "销售额",
        "synonyms": "营收，销售收入",
        "datasource_id": 1,
        "table_name": "orders",
        "core_fields": "order_id, amount",
        "calc_logic": "SUM(amount)",
        "dw_layer": "DWS"
    },
    {
        "metric_name": "订单量",
        "synonyms": "订单数",
        "datasource_id": 1,
        "table_name": "orders",
        "core_fields": "order_id",
        "calc_logic": "COUNT(DISTINCT order_id)",
        "dw_layer": "DWS"
    }
]

response = requests.post(
    "http://localhost:8000/extend/metric-metadata/batch",
    json=data_list
)
print(response.json())
# 返回：
# {
#   "success_count": 2,
#   "failed_records": [],
#   "duplicate_count": 0,
#   "original_count": 2,
#   "deduplicated_count": 2
# }
```

### 更新指标

```python
import requests

data = {
    "id": 1,
    "metric_name": "销售额",
    "synonyms": "营收，销售收入，GMV",  # 更新同义词
    "datasource_id": 1,
    "table_name": "orders",
    "core_fields": "order_id, amount, pay_time",
    "calc_logic": "SUM(amount) WHERE pay_time IS NOT NULL",  # 优化计算逻辑
    "dw_layer": "DWS"
}

response = requests.put(
    "http://localhost:8000/extend/metric-metadata",
    json=data
)
print(response.json())
```

### 删除指标

```python
import requests

# 删除单个
response = requests.delete(
    "http://localhost:8000/extend/metric-metadata",
    params={"ids": [1]}
)

# 删除多个
response = requests.delete(
    "http://localhost:8000/extend/metric-metadata",
    params={"ids": [1, 2, 3]}
)
```

---

## 🔍 查询功能

### 分页查询

```python
import requests

# 基础分页
response = requests.get(
    "http://localhost:8000/extend/metric-metadata/page/1/10"
)

# 带条件查询
response = requests.get(
    "http://localhost:8000/extend/metric-metadata/page/1/10",
    params={
        "metric_name": "销售",  # 模糊匹配
        "datasource_id": 1
    }
)
```

### 列表查询（不分页）

```python
import requests

response = requests.get(
    "http://localhost:8000/extend/metric-metadata/list",
    params={
        "metric_name": "率",  # 查询所有包含"率"的指标
        "datasource_id": 1
    }
)
```

### 查询详情

```python
import requests

response = requests.get(
    "http://localhost:8000/extend/metric-metadata/1"
)
print(response.json())
```

---

## 🧪 本地测试接口

### 1. 获取示例数据

```bash
curl http://localhost:8000/extend/metric-metadata/test/sample-data
```

返回 10 个精心设计的示例指标，包括：
- 销售额、订单量、客单价（电商基础指标）
- 毛利率、退货率（财务指标）
- DAU、MAU、转化率、复购率（用户指标）
- 库存周转率（供应链指标）

### 2. 一键插入测试数据

```bash
curl -X POST http://localhost:8000/extend/metric-metadata/test/insert-sample
```

### 3. 测试查询

```bash
# 查询所有
curl http://localhost:8000/extend/metric-metadata/test/query

# 按名称查询
curl "http://localhost:8000/extend/metric-metadata/test/query?metric_name=销售"
```

### 4. 清空测试数据

```bash
# ⚠️ 警告：这将删除所有数据！
curl -X DELETE http://localhost:8000/extend/metric-metadata/test/clear-all
```

---

## 📝 数据样例

### 基础指标样例

```json
{
  "metric_name": "销售额",
  "synonyms": "营收，销售收入，卖钱额，GMV",
  "datasource_id": 1,
  "table_name": "orders",
  "core_fields": "order_id, user_id, amount, pay_time, create_time",
  "calc_logic": "SUM(amount) WHERE pay_time IS NOT NULL",
  "upstream_table": "order_items, cart",
  "dw_layer": "DWS"
}
```

### 派生指标样例

```json
{
  "metric_name": "客单价",
  "synonyms": "人均消费，ARPU",
  "datasource_id": 1,
  "table_name": "orders",
  "core_fields": "user_id, amount",
  "calc_logic": "SUM(amount) / COUNT(DISTINCT user_id)",
  "upstream_table": "orders",
  "dw_layer": "ADS"
}
```

### 数仓分层说明

| 分层 | 说明 | 示例指标 |
|------|------|----------|
| ODS | 原始数据层 | 订单明细表 |
| DWD | 明细数据层 | 清洗后的订单表 |
| DWS | 汇总数据层 | 销售额、订单量 |
| ADS | 应用数据层 | 客单价、毛利率、转化率 |

---

## ⚙️ 配置说明

### Embedding 配置

在 `backend/common/core/config.py` 中配置：

```python
EMBEDDING_ENABLED = True  # 启用 embedding 计算
EMBEDDING_MODEL = "text-embedding-ada-002"  # embedding 模型
```

Embedding 会在以下场景自动触发：
1. 创建新指标时
2. 批量插入成功后
3. 更新指标信息时
4. 手动调用 `/fill-embeddings` 接口

---

## 🎯 设计特点

### 1. 复用 Terminology 模式

- ✅ 相同的代码组织结构
- ✅ 相似的 CRUD 操作逻辑
- ✅ 统一的错误处理机制
- ✅ 一致的 API 设计风格

### 2. 最小化改造

- ✅ 直接复用现有的 SessionDep、Trans 等依赖注入
- ✅ 使用相同的数据库连接池
- ✅ 共享 EmbeddingModelCache

### 3. 扩展性设计

- ✅ 支持自定义字段扩展
- ✅ 支持多种数仓分层
- ✅ 灵活的查询条件组合
- ✅ 预留未来扩展空间

---

## 🔧 常见问题

### Q1: Embedding 失败会影响数据插入吗？

不会。Embedding 处理在 try-catch 块中，失败只会打印日志，不影响主流程。

### Q2: 如何修改唯一性约束？

当前唯一性约束：`(metric_name, table_name, datasource_id)`

修改位置：`curd/metric_metadata.py` 第 27-32 行

### Q3: 如何禁用自动 Embedding？

方法 1：临时禁用
```python
create_metric_metadata(session, info, skip_embedding=True)
```

方法 2：配置禁用
```python
# config.py
EMBEDDING_ENABLED = False
```

### Q4: 支持哪些数仓分层？

理论上支持任意分层标识，常用的有：
- ODS (Operational Data Store)
- DWD (Data Warehouse Detail)
- DWS (Data Warehouse Service)
- ADS (Application Data Service)
- DIM (Dimension)

---

## 📞 API 端点汇总

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/test/sample-data` | 查看示例数据 |
| POST | `/test/insert-sample` | 插入示例数据 |
| GET | `/test/query` | 测试查询 |
| DELETE | `/test/clear-all` | 清空测试数据 |
| PUT | `/` | 创建或更新 |
| POST | `/batch` | 批量创建 |
| GET | `/{id}` | 查询详情 |
| GET | `/page/{current_page}/{page_size}` | 分页查询 |
| GET | `/list` | 列表查询（不分页） |
| DELETE | `/` | 删除 |
| POST | `/fill-embeddings` | 填充 embedding |

---

## 📚 参考资料

- [Terminology 模块实现](../terminology/)
- [SQLBot 项目文档](../../../README.md)
- [FastAPI 官方文档](https://fastapi.tiangolo.com/)

---

## 🎉 下一步计划

1. ✅ 基础 CRUD 功能完成
2. ✅ 本地测试接口完成
3. ⏳ 集成到智能问数流程
4. ⏳ 语义检索功能增强
5. ⏳ 指标血缘关系可视化
6. ⏳ 自动化指标推荐
