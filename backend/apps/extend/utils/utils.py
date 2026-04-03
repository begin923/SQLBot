import os
from pathlib import Path
from typing import Dict

import yaml


class Utils:
    """工具类"""

    @staticmethod
    def load_prompt_template_static(template_name: str) -> Dict[str, str]:
        """静态方法：加载提示词模板（可被 ModelClient 复用）"""
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        yaml_path = os.path.join(current_dir, "yaml")
        yaml_file = os.path.join(yaml_path, f"{template_name}.yaml")
        print(yaml_file)

        if os.path.exists(yaml_file):
            with open(yaml_file, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        else:
            raise FileNotFoundError(f"提示词文件不存在：{yaml_file}")


    @staticmethod
    def create_local_session():
        """
        创建一个独立的数据库会话（用于本地测试或后台任务）
        不依赖 SessionDep，直接从.env 文件读取配置

        Returns:
            session: SQLAlchemy 会话对象
        """
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        import os
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

        # 创建引擎和会话
        engine = create_engine(database_url)
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