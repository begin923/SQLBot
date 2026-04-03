# SQLBot - 智能 SQL 生成与分析系统

## 📋 项目简介

SQLBot 是一个基于大语言模型（LLM）的智能 SQL 生成与分析系统，支持自然语言查询、自动 SQL 生成、数据可视化等功能。系统采用 FastAPI 框架，支持多种数据库类型（MySQL、PostgreSQL、Oracle、ClickHouse 等）。

### 核心功能

- **自然语言查询**：用户通过中文或英文提问，自动生成 SQL
- **多轮对话**：支持上下文理解的连续问答
- **SQL 校验引擎**：自动校验和修复 SQL，确保符合数仓规范
- **数据可视化**：自动生成图表配置
- **指标元数据管理**：支持指标、维度的定义和管理
- **术语管理**：支持业务术语与数据库字段的映射
- **数据训练**：通过示例 SQL 训练模型
- **MCP 服务**：支持 Model Context Protocol 集成

---

## 🏗️ 技术架构

### 后端技术栈

| 技术 | 版本 | 用途 |
|------|------|------|
| **Python** | 3.11 | 开发语言 |
| **FastAPI** | 0.115+ | Web 框架 |
| **Pydantic** | 2.0+ | 数据验证 |
| **SQLModel/SQLAlchemy** | 0.0.21+ | ORM 框架 |
| **Alembic** | 1.12+ | 数据库迁移 |
| **LangChain** | 0.3+ | LLM 应用框架 |
| **Sentence Transformers** | 4.0.2+ | 文本嵌入 |
| **PGVector** | 0.4.1+ | 向量相似度搜索 |
| **Redis** | 6.2+ | 缓存 |
| **PostgreSQL** | 14+ | 主数据库 |

### 支持的数据库

- MySQL / PostgreSQL / Oracle / SQL Server
- ClickHouse / Doris / StarRocks / Redshift
- Elasticsearch / Kingbase / Dameng (DM)

---

## 📁 目录结构

```
backend/
├── apps/                          # 应用模块
│   ├── ai_model/                  # AI 模型管理
│   │   ├── model_factory.py       # LLM 工厂类
│   │   ├── embedding.py           # 嵌入模型
│   │   └── llm.py                 # LLM 接口
│   ├── chat/                      # 聊天模块
│   │   ├── api/chat.py            # 聊天 API
│   │   ├── curd/chat.py           # 聊天 CRUD
│   │   ├── models/chat_model.py   # 聊天数据模型
│   │   └── task/llm.py            # LLM 任务处理（核心）
│   ├── datasource/                # 数据源管理
│   │   ├── api/datasource.py      # 数据源 API
│   │   ├── embedding/             # 数据源向量化
│   │   └── crud/                  # 数据源 CRUD
│   ├── extend/                    # 扩展功能
│   │   ├── chat_manager/          # 会话状态管理
│   │   ├── sql_engine/            # SQL 校验引擎
│   │   ├── metric_metadata/       # 指标元数据
│   │   └── drilldown/             # 下钻分析
│   ├── system/                    # 系统管理
│   │   ├── api/                   # 系统 API
│   │   ├── crud/                  # 系统 CRUD
│   │   └── models/                # 系统数据模型
│   ├── template/                  # 提示词模板
│   └── terminology/               # 术语管理
├── common/                        # 公共模块
│   ├── core/                      # 核心组件
│   │   ├── config.py              # 配置管理
│   │   ├── deps.py                # 依赖注入
│   │   ├── db.py                  # 数据库连接
│   │   └── security.py            # 安全认证
│   └── utils/                     # 工具函数
├── alembic/                       # 数据库迁移
├── models/                        # AI 模型文件
└── templates/                     # YAML 模板
```

---

## 🚀 快速开始

### 环境要求

- Python 3.11
- PostgreSQL 14+
- Redis（可选，用于缓存）

### 安装步骤

#### 1. 克隆项目

```bash
git clone https://github.com/your-org/MySQLBot.git
cd MySQLBot/backend
```

#### 2. 创建虚拟环境

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或
venv\Scripts\activate     # Windows
```

#### 3. 安装依赖

```bash
# 使用 uv（推荐）
uv pip install -e .

# 或使用 pip
pip install -r requirements.txt
```

#### 4. 配置环境变量

复制 `.env.example` 为 `.env` 并修改配置：

```bash
cp ../.env.example ../.env
```

关键配置项：

```ini
# 数据库配置
POSTGRES_SERVER=localhost
POSTGRES_PORT=5432
POSTGRES_USER=root
POSTGRES_PASSWORD=Password123@pg
POSTGRES_DB=sqlbot

# LLM 配置（在系统中配置）
DEFAULT_LLM_MODEL=qwen-plus
DEFAULT_EMBEDDING_MODEL=shibing624/text2vec-base-chinese

# 缓存配置
CACHE_TYPE=memory  # 或 redis
CACHE_REDIS_URL=redis://localhost:6379/0
```

#### 5. 初始化数据库

```bash
# 运行数据库迁移
alembic upgrade head
```

#### 6. 启动服务

```bash
# 开发模式
python main.py

# 生产模式
uvicorn main:app --host 0.0.0.0 --port 8000
```

访问 http://localhost:8000/docs 查看 API 文档。

---

## 🔧 核心功能说明

### 1. SQL 生成流程

```python
# apps/chat/task/llm.py - LLMService 类

class LLMService:
    def generate_sql(self):
        """
        SQL 生成完整流程：
        1. 分析用户意图（ChatService）
        2. 提取指标和维度
        3. 查询指标元数据
        4. 获取表结构
        5. 构建 Prompt
        6. 调用 LLM 生成 SQL
        7. SQL 校验和修复（SQLValidator）
        8. 执行 SQL 并返回结果
        """
```

### 2. SQL 校验引擎

```python
# apps/extend/sql_engine/sql_validator.py

class SQLValidator:
    """
    SQL 校验引擎，支持：
    1. DWD/ODS 层拦截（LAYER_VIOLATION）
    2. ADS/DWS 层聚合检查（MISSING_CLAUSE）
    3. 自动修复（调用 LLM）
    """
    
    def validate_and_fix(self, sql: str):
        # 返回：(是否通过，最终 SQL, 错误信息)
        pass
```

**错误类型分类：**

| 错误类型 | 含义 | 是否调用 LLM | 处理方式 |
|---------|------|------------|---------|
| `LAYER_VIOLATION` | 查询 DWD/ODS 明细层 | ❌ 否 | 重新选择表 |
| `MISSING_CLAUSE` | 缺少 GROUP BY 或聚合函数 | ✅ 是 | LLM 自动修复 |
| `EMPTY_SQL` | SQL 为空 | ❌ 否 | 直接报错 |

### 3. 会话状态管理

```python
# apps/extend/chat_manager/services/chat_service.py

class ChatService:
    """
    会话管理服务：
    1. 从用户问题中提取字段（extract_fields_from_question）
    2. 从聊天历史中补充缺失字段
    3. 维护多轮对话上下文
    """
```

### 4. 指标元数据管理

```python
# apps/extend/metric_metadata/

# 支持：
# - 指标定义（名称、计算逻辑、数据来源）
# - 维度定义（时间、组织、分类等）
# - 血缘关系（上游表、下游表）
# - 向量化检索
```

---

## 📊 数据库设计

### 核心表

| 表名 | 用途 |
|------|------|
| `chat` | 聊天会话 |
| `chat_question` | 用户问题 |
| `chat_record` | 聊天记录 |
| `chat_state` | 会话状态（上下文） |
| `core_datasource` | 数据源配置 |
| `core_table` | 表结构 |
| `core_field` | 字段信息 |
| `metric_metadata` | 指标元数据 |
| `metric_dimension` | 维度元数据 |
| `metric_lineage` | 指标血缘 |
| `terminology` | 业务术语 |
| `data_training` | 数据训练样本 |
| `assistant` | 智能助手配置 |

---

## 🔐 认证与授权

### Token 认证

```python
# apps/system/middleware/auth.py

class TokenMiddleware:
    """
    JWT Token 认证中间件
    - 从请求头获取 Token: X-SQLBOT-TOKEN
    - 验证用户身份
    - 注入当前用户到请求上下文
    """
```

### 行级权限

```python
# apps/datasource/crud/permission.py

def get_row_permission_filters():
    """
    根据用户角色和数据权限，动态添加 SQL 过滤条件
    - 部门数据隔离
    - 区域数据隔离
    - 自定义权限规则
    """
```

---

## 🤖 AI 模型集成

### 支持的 LLM

- **通义千问**：qwen-turbo, qwen-plus, qwen-max
- **OpenAI**：gpt-3.5-turbo, gpt-4
- **智谱 AI**：glm-3-turbo, glm-4
- **本地部署**：通过 LangChain 集成

### 嵌入模型

默认使用 `shibing624/text2vec-base-chinese`，支持：

- 术语相似度检索
- 指标元数据匹配
- 数据训练样本推荐
- 表结构语义理解

---

## 🛠️ 开发与测试

### 代码规范

```bash
# 代码格式化
ruff format .

# 代码检查
ruff check .

# 类型检查
mypy .

# 运行测试
pytest
```

### Git 工作流

```bash
# 提交前检查
pre-commit run --all-files

# 提交信息规范
git commit -m "feat: 添加新功能"
git commit -m "fix: 修复 bug"
git commit -m "refactor: 代码重构"
```

---

## 📝 API 文档

启动服务后访问：

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/api/v1/openapi.json

### 主要 API 端点

```
# 认证
POST   /api/v1/login/access-token
POST   /api/v1/users

# 聊天
POST   /api/v1/chat
POST   /api/v1/chat/{chat_id}/question
GET    /api/v1/chat/{chat_id}/records

# 数据源
GET    /api/v1/datasource/list
POST   /api/v1/datasource/create
PUT    /api/v1/datasource/update/{ds_id}

# 指标元数据
GET    /api/v1/metric-metadata/list
POST   /api/v1/metric-metadata/create

# 术语
GET    /api/v1/terminology/list
POST   /api/v1/terminology/create
```

---

## 🔄 数据库迁移

### 创建新迁移

```bash
cd backend
alembic revision --autogenerate -m "描述变更内容"
```

### 应用迁移

```bash
alembic upgrade head
```

### 回滚迁移

```bash
alembic downgrade -1  # 回滚一个版本
alembic downgrade base  # 回滚到初始状态
```

---

## 📦 部署

### Docker 部署

```bash
# 构建镜像
docker build -t sqlbot:latest .

# 运行容器
docker-compose up -d
```

### 生产环境配置

编辑 `docker-compose.yaml`：

```yaml
services:
  backend:
    image: sqlbot:latest
    environment:
      - POSTGRES_SERVER=postgres
      - REDIS_HOST=redis
    depends_on:
      - postgres
      - redis
  
  postgres:
    image: postgres:14
    volumes:
      - pgdata:/var/lib/postgresql/data
  
  redis:
    image: redis:7-alpine
```

---

## 🧩 扩展开发

### 添加新的 LLM 支持

```python
# apps/ai_model/model_factory.py

class LLMFactory:
    @staticmethod
    def create_llm(config: LLMConfig):
        if config.model_type == 'your-llm':
            return YourLLM(config)
```

### 添加新的数据库支持

```python
# apps/db/db.py

def get_db_engine(db_type: str):
    if db_type == 'your-db':
        return YourDBEngine()
```

### 自定义提示词模板

```yaml
# templates/template.yaml

generate_sql:
  system_prompt: |
    你是一个 SQL 专家...
  user_prompt_template: |
    请根据以下表结构生成 SQL...
```

---

## 📄 License

本项目采用 [MIT License](LICENSE)

---

## 👥 贡献指南

欢迎提交 Issue 和 Pull Request！

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

---

## 📞 联系方式

- 项目地址：https://github.com/your-org/MySQLBot
- 问题反馈：https://github.com/your-org/MySQLBot/issues
- 邮箱：support@sqlbot.com

---

## 🙏 致谢

感谢以下开源项目：

- [FastAPI](https://fastapi.tiangolo.com/)
- [LangChain](https://www.langchain.com/)
- [SQLModel](https://sqlmodel.tiangolo.com/)
- [Sentence Transformers](https://www.sbert.net/)

---

**Version**: 1.4.0  
**Last Updated**: 2026-04-03  
**Maintainer**: SQLBot Team
