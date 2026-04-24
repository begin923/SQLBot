import os
import sys
import logging
import traceback
from pathlib import Path
from typing import Dict

import yaml
from openai import OpenAI
from apps.extend.format.config import config

logger = logging.getLogger(__name__)


class Utils:
    """工具类"""

    @staticmethod
    def load_prompt_template_static(template_name: str) -> Dict[str, str]:
        """静态方法：加载提示词模板（可被 ModelClient 复用）"""
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        yaml_path = os.path.join(current_dir, "yaml")
        yaml_file = os.path.join(yaml_path, f"{template_name}.yaml")

        if os.path.exists(yaml_file):
            with open(yaml_file, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        else:
            raise FileNotFoundError(f"提示词文件不存在：{yaml_file}")


class ModelClient:
    """AI 模型客户端 - 简化版"""

    def __init__(self):
        # 统一从配置文件读取配置
        try:
            self.api_key = config.dashscope_api_key or os.getenv("DASHSCOPE_API_KEY")
            self.base_url = config.dashscope_base_url or os.getenv("DASHSCOPE_BASE_URL")
            self.model = config.dashscope_code_model or os.getenv("DASHSCOPE_CODE_MODEL")
        except ImportError:
            # 如果 config 模块不存在，直接使用环境变量
            self.api_key = os.getenv("DASHSCOPE_API_KEY")
            self.base_url = os.getenv("DASHSCOPE_BASE_URL")
            self.model = os.getenv("DASHSCOPE_CODE_MODEL", "qwen3-coder-plus")

        # 初始化 OpenAI 客户端
        if self.api_key and self.base_url:
            self.client = OpenAI(
                api_key=self.api_key,
                base_url=self.base_url
            )
        else:
            logger.warning(f"⚠️  [ModelClient] AI 配置缺失 - API Key: {'✅' if self.api_key else '❌'}, Base URL: {'✅' if self.base_url else '❌'}")
            self.client = None

    def call_ai(self, template_name: str, sql_content: str, sql_file: str = "", layer_type: str = "METRIC") -> str:
        """
        调用 AI 大模型，返回 MD 格式

        Args:
            template_name: 模板名称
            sql_content: SQL 内容
            sql_file: SQL 文件名
            layer_type: 数仓层级类型（DIM/DWD/METRIC），用于选择不同的提示词

        Returns:
            MD 格式的字符串
        """
        try:
            # 加载提示词配置（使用静态方法）
            prompt_config = Utils.load_prompt_template_static(template_name)

            # 获取 system 提示词
            system_prompt = prompt_config.get('system', '')

            # ⚠️ 根据层级类型选择不同的 system + user 提示词组合
            if layer_type == "DIM":
                # DIM 层：使用 dim_layer.system + dim_layer.user
                layer_config = prompt_config.get('dim_layer', {})
                system_prompt = layer_config.get('system', '')
                user_prompt_template = layer_config.get('user', '')
                        
            elif layer_type == "DWD":
                # DWD 层：使用 dwd_layer.system + dwd_layer.user
                layer_config = prompt_config.get('dwd_layer', {})
                system_prompt = layer_config.get('system', '')
                user_prompt_template = layer_config.get('user', '')
                        
            else:
                # METRIC/DWS/ADS 层（默认）：使用 dws_ads_layer.system + dws_ads_layer.user
                layer_config = prompt_config.get('dws_ads_layer', {})
                system_prompt = layer_config.get('system', '')
                user_prompt_template = layer_config.get('user', '')

            if not user_prompt_template:
                raise ValueError("找不到任何可用的提示词模板")

            # 替换占位符（只替换 sql_content）
            user_prompt = user_prompt_template.replace("{sql_content}", sql_content)

            # 构建消息列表
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            else:
                messages.append({"role": "system",
                                 "content": "You are a helpful data assistant that outputs Markdown format."})

            messages.append({"role": "user", "content": user_prompt})

            completion = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0.1
            )

            result = completion.choices[0].message.content
            logger.info(f"[AI] 收到 AI 响应，长度：{len(result) if result else 0} 字符")

            if result:
                logger.debug(f"[AI] AI 返回内容预览：{result[:200]}...")

            if not result or not result.strip():
                logger.error("[AI] AI 返回空内容")
                return None

            return result

        except Exception as e:
            # 获取详细的错误位置信息
            exc_type, exc_obj, exc_tb = sys.exc_info()
            file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            line_number = exc_tb.tb_lineno
            function_name = exc_tb.tb_frame.f_code.co_name
            module_name = exc_tb.tb_frame.f_globals.get('__name__', 'unknown')
            
            # 构建详细错误日志
            error_msg = f"""
{'='*80}
❌ [AI] AI 调用失败
{'='*80}
📍 错误位置：
   • 模块：{module_name}
   • 文件：{file_name}
   • 函数：{function_name}
   • 行号：{line_number}

🔍 调用上下文：
   • 模板名称：{template_name}
   • SQL 内容长度：{len(sql_content)} 字符
   • SQL 文件名：{sql_file or 'N/A'}

⚠️  客户端状态：
   • API Key：{'✅ 已设置' if self.api_key else '❌ 未设置'}
   • Base URL：{'✅ 已设置' if self.base_url else '❌ 未设置'}
   • Model：{self.model or '❌ 未设置'}
   • Client：{'✅ 已初始化' if self.client else '❌ None'}

💥 异常信息：
   • 类型：{type(e).__name__}
   • 消息：{str(e)}

📋 完整堆栈跟踪：
{traceback.format_exc()}
{'='*80}
"""
            
            # 输出到日志和控制台
            logger.error(error_msg)
            print(error_msg)
            
            raise


class DBUtils:
    """数据库工具类"""

    @staticmethod
    def create_local_session(echo_sql: bool = False):
        """
        创建一个独立的数据库会话（用于本地测试或后台任务）
        不依赖 SessionDep，直接从.env 文件读取配置

        Args:
            echo_sql: 是否输出 SQL 日志（默认 False）

        Returns:
            session: SQLAlchemy 会话对象
        """
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from dotenv import load_dotenv

        # 加载根目录的.env 文件
        env_path = Path(__file__).parent.parent.parent.parent.parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)

        # 从环境变量读取数据库配置
        db_host = os.getenv("POSTGRES_SERVER", "localhost")
        db_port = os.getenv("POSTGRES_PORT", "5432")
        db_user = os.getenv("POSTGRES_USER", "sqlbot")
        db_password = os.getenv("POSTGRES_PASSWORD", "sqlbot")
        db_name = os.getenv("POSTGRES_DB", "sqlbot")

        database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

        # 创建引擎和会话（默认关闭 SQL 日志输出）
        engine = create_engine(database_url, echo=echo_sql)
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()

        return session

    @staticmethod
    def create_llm_client():
        # 创建会话状态服务实例
        from apps.ai_model.model_factory import LLMConfig, LLMFactory

        # 使用简化的测试配置（需要根据实际环境修改）
        config = LLMConfig(
            model_type="tongyi",  # 或 "vllm"
            model_name="qwen3-coder-plus",  # 或其他可用模型
            api_key="sk-4d2c4271c8734e7998c1ea9853c6ec00",  # 替换为实际 API Key
            api_base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",  # 替换为实际 endpoint
        )

        llm_instance = LLMFactory.create_llm(config)
        return llm_instance.llm


if __name__ == "__main__":
    print(Utils.load_prompt_template_static("rule_keyword"))
