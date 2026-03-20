# 项目配置文件（统一配置管理）
"""
配置模块提供统一的配置管理能力，支持多环境配置。

使用示例:
    from config.config import config
    
    # 直接访问配置项
    doris_host = config.doris_config["host"]
    api_key = config.dashscope_api_key
"""

import os
from dotenv import load_dotenv
from typing import Dict, Any


class Config:
    """配置类 - 统一管理所有环境配置"""
    
    def __init__(self):
        """初始化配置"""
        self._load_env_files()
        
        # Git 配置
        self.git_local_path = os.getenv("GIT_LOCAL_PATH", "D:/codes/AIDataEasy/data_governance_agent/sql_files")
        self.git_repo_url = os.getenv("GIT_REPO_URL", "")
        
        # DashScope 配置
        self.dashscope_base_url = os.getenv("DASHSCOPE_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
        self.dashscope_api_key = os.getenv("DASHSCOPE_API_KEY")
        self.dashscope_code_model = os.getenv("DASHSCOPE_CODE_MODEL", "qwen3-coder-plus")
        self.dashscope_vector_model = os.getenv("DASHSCOPE_VECTOR_MODEL", "text-embedding-v4")
        self.dashscope_rerank_model = os.getenv("DASHSCOPE_RERANK_MODEL", "qwen3-rerank")
        
        # Doris 配置
        self.doris_config = {
            "host": os.getenv("DORIS_HOST"),
            "port": int(os.getenv("DORIS_PORT", 9030)),
            "user": os.getenv("DORIS_USER", "root"),
            "password": os.getenv("DORIS_PASSWORD"),
            "db": os.getenv("DORIS_DB")
        }
        
        # Redis 配置
        self.redis_config = {
            "host": os.getenv("REDIS_HOST"),
            "port": int(os.getenv("REDIS_PORT", 6379)),
            "password": os.getenv("REDIS_PASSWORD"),
            "db": int(os.getenv("REDIS_DB", 0))
        }
        
        # PostgreSQL 配置
        self.pg_config = {
            "host": os.getenv("PG_HOST"),
            "port": int(os.getenv("PG_PORT", 5432)),
            "user": os.getenv("PG_USER", "postgres"),
            "password": os.getenv("PG_PASSWORD"),
            "database": os.getenv("PG_DATABASE")
        }
    
    def _load_env_files(self):
        """加载环境变量文件"""
        # 获取当前脚本所在目录
        base_dir = os.path.dirname(os.path.abspath(__file__))
        env_path = os.path.join(base_dir, '.env')
        template_path = os.path.join(base_dir, '.env.template')
        
        # 优先加载.env，如果不存在则加载.env.template
        if os.path.exists(env_path):
            load_dotenv(env_path)
        elif os.path.exists(template_path):
            load_dotenv(template_path)
    
    def __repr__(self):
        """配置的字符串表示"""
        return (f"<Config "
                f"git_repo={self.git_repo_url or 'local'}, "
                f"doris={self.doris_config.get('host', 'N/A')}, "
                f"model={self.dashscope_code_model}>")


# ==================== 全局配置实例 ====================
config = Config()

if __name__ == "__main__":
    print(config.doris_config)
