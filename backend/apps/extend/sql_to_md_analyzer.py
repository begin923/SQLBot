import os
import sys
import yaml
import logging
from typing import Dict, Any
from datetime import datetime
from openai import OpenAI

# 添加 data_governance_agent 到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import config

logger = logging.getLogger("SQLToMDAnalyzer")
logging.basicConfig(level=logging.INFO)


class ModelClient:
    """AI 模型客户端 - 简化版"""
    
    def __init__(self):
        # 统一从配置文件读取配置
        self.api_key = config.dashscope_api_key or os.getenv("DASHSCOPE_API_KEY")
        self.base_url = config.dashscope_base_url or os.getenv("DASHSCOPE_BASE_URL")
        self.model = config.dashscope_code_model or os.getenv("DASHSCOPE_CODE_MODEL")
        
        # 初始化 OpenAI 客户端
        if self.api_key and self.base_url:
            self.client = OpenAI(
                api_key=self.api_key,
                base_url=self.base_url
            )
        else:
            logger.warning("未设置 API 密钥或 base_url，AI 功能可能失败")
            self.client = None
    
    def call_ai(self, template_name: str, sql_content: str, sql_file: str = "") -> str:
        """
        调用 AI 大模型，返回 MD 格式
        
        Args:
            template_name: 模板名称
            sql_content: SQL 内容
            sql_file: SQL 文件名
            
        Returns:
            MD 格式的字符串
        """
        try:
            # 加载提示词配置（使用静态方法）
            prompt_config = SQLToMDAnalyzer.load_prompt_template_static(template_name)
            
            # 获取 system 提示词
            system_prompt = prompt_config.get('system', '')
            
            # 获取 metric_blood 提示词
            user_prompt_template = prompt_config.get('metric_blood', '')
            
            if not user_prompt_template:
                raise ValueError("找不到 metric_blood 类型的提示词模板")
            
            # 替换占位符
            user_prompt = user_prompt_template.replace("{sql_content}", sql_content)
            if sql_file:
                user_prompt = user_prompt.replace("{sql_file}", sql_file)
            
            logger.info(f"[AI] 调用 AI 模型：{self.model}, 模板：{template_name}")
            logger.debug(f"[AI] System 提示词长度：{len(system_prompt)} 字符")
            logger.debug(f"[AI] User Prompt 长度：{len(user_prompt)} 字符")
            
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
            logger.error(f"AI 调用失败：{e}")
            logger.error(f"[AI] 调用详情 - 模板：{template_name}, SQL 内容长度：{len(sql_content)}")
            logger.error(f"[AI] 完整错误信息：{str(e)}")
            raise


class SQLToMDAnalyzer:
    """SQL 到 MD文档分析器 - 简化版"""

    def __init__(self, md_output_dir: str = 'metric_blood'):
        """
        初始化分析器
            
        Args:
            md_output_dir: MD 文档输出根目录（默认：当前脚本目录/metric_blood）
        """
        # 简化输出目录：当前脚本目录/metric_blood
        self.md_output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), md_output_dir)
        os.makedirs(self.md_output_path, exist_ok=True)
        self.model_client = ModelClient()

    @staticmethod
    def load_prompt_template_static(template_name: str) -> Dict[str, str]:
        """静态方法：加载提示词模板（可被 ModelClient 复用）"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        yaml_path = os.path.join(current_dir, f"{template_name}_prompt.yaml")
        
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        prompt_yaml_path = os.path.join(base_dir, "prompt", f"{template_name}_prompt.yaml")
        
        if os.path.exists(yaml_path):
            with open(yaml_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        elif os.path.exists(prompt_yaml_path):
            with open(prompt_yaml_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        else:
            raise FileNotFoundError(f"提示词文件不存在：{template_name}")

    def analyze_sql(self, sql_content: str, sql_file: str = "custom.sql") -> str:
        """
        分析 SQL 内容，返回 MD 格式结果
        
        Args:
            sql_content: SQL 语句内容
            sql_file: SQL 文件名（用于提示词）
            
        Returns:
            MD 格式的字符串
        """
        try:
            md_content = self.model_client.call_ai(
                template_name="sql_analysis",
                sql_content=sql_content,
                sql_file=sql_file
            )
            logger.info(f"成功分析 SQL")
            return md_content or ""
        except Exception as e:
            logger.error(f"分析 SQL 失败：{str(e)}")
            return ""

    def analyze_sql_file(self, sql_path: str) -> Dict[str, Any]:
        """
        分析单个 SQL 文件
        
        Args:
            sql_path: SQL 文件路径
            
        Returns:
            解析结果字典 {file_name, file_path, table_name, md_content}
        """
        result = {
            "file_name": os.path.basename(sql_path),
            "file_path": sql_path,
            "table_name": "",
            "md_content": ""
        }
        
        try:
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()
            
            result["md_content"] = self.analyze_sql(sql_content, result["file_name"])
            
            # 从 MD 内容中提取表名（新格式：横向表格，第二列是 target_table_id）
            if result["md_content"]:
                for line in result["md_content"].split('\n'):
                    # 匹配基本信息表格的数据行（第二列是 target_table_id）
                    if line.startswith("|") and "target_table_id" not in line and "file_name" not in line:
                        parts = [p.strip() for p in line.split("|") if p.strip()]
                        # 横向表格：第一部分应该是 file_name，第二部分是 target_table_id
                        if len(parts) >= 2 and parts[0] != "-----------":
                            # 跳过表头行，取数据行的第二个字段
                            if not any(keyword in parts[0] for keyword in ["file_name", "target_table_id", "warehouse_layer"]):
                                result["table_name"] = parts[1]
                                break
            
            logger.info(f"成功分析文件：{sql_path}")
        except Exception as e:
            logger.error(f"分析文件失败 {sql_path}: {str(e)}")
        
        return result

    def generate_md_file(self, analysis_result: Dict[str, Any]) -> str:
        """
        生成 MD 文件
        
        Args:
            analysis_result: 分析结果字典
            
        Returns:
            MD 文件路径
        """
        md_content = analysis_result.get("md_content", "")
        if not md_content:
            return ""
        
        table_name = analysis_result.get("table_name", "")
        file_name = analysis_result.get("file_name", "unknown.sql").replace(".sql", "")
        
        # 使用表名作为文件名
        md_file_name = f"{table_name}.md" if table_name else f"{file_name}.md"
        
        # MD 文档输出路径：当前脚本路径/metric_blood
        md_file_path = os.path.join(self.md_output_path, md_file_name)
        
        # 智能更新检测
        is_new_file = not os.path.exists(md_file_path)
        update_type = "新建" if is_new_file else "更新"
        
        # 生成版本信息头
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        version_info = f"<!-- 版本信息：最后更新时间={timestamp}, 操作类型={update_type} -->\n"
        
        if not is_new_file:
            version_info += f"<!-- 变更日志：文档于 {timestamp} 更新 -->\n"
        
        # 写入文件
        with open(md_file_path, "w", encoding="utf-8") as f:
            f.write(version_info + md_content)
        
        logger.info(f"{update_type} MD文档：{md_file_path}")
        return md_file_path

    def analyze_directory(self, sql_dir: str) -> Dict[str, str]:
        """
        分析整个目录中的 SQL 文件
        
        Args:
            sql_dir: SQL 文件目录路径
            
        Returns:
            分析结果字典 {sql_path: md_path}
        """
        results = {}
        
        if not os.path.exists(sql_dir):
            logger.error(f"SQL 目录不存在：{sql_dir}")
            return results
        
        sql_file_count = 0
        for root, _, files in os.walk(sql_dir):
            for file in files:
                if file.endswith(".sql"):
                    sql_file_count += 1
                    sql_path = os.path.join(root, file)
                    logger.info(f"\n分析第{sql_file_count}个文件：{sql_path}")
                    
                    analysis_result = self.analyze_sql_file(sql_path)
                    md_path = self.generate_md_file(analysis_result)
                    
                    if md_path:
                        results[sql_path] = md_path
        
        logger.info(f"\n分析完成！共处理{sql_file_count}个 SQL 文件")
        return results
    
    def analyze(self, dir_path: str,file_name: str = None) -> Dict[str, str]:
        """
        统一分析入口：自动判断是文件还是目录
        
        Args:
            dir_path: SQL 文件路径或目录路径
            
        Returns:
            分析结果字典 {sql_path: md_path}
        """
        if not os.path.exists(dir_path):
            logger.error(f"路径不存在：{dir_path}")
            return {}

        file_path = dir_path.join(file_name)
        if file_name and os.path.isfile(file_path):
            # 单个文件
            logger.info(f"开始分析文件：{file_path}")
            analysis_result = self.analyze_sql_file(file_path)
            md_path = self.generate_md_file(analysis_result)
            return {dir_path: md_path} if md_path else {}
        
        elif os.path.isdir(dir_path):
            # 目录
            logger.info(f"开始批量分析目录：{dir_path}")
            return self.analyze_directory(dir_path)
        
        else:
            logger.error(f"不支持的路径类型：{dir_path}")
            return {}


def main():
    """主函数 - 测试用"""
    analyzer = SQLToMDAnalyzer()
    
    # 测试单个文件
    # target_file = "D://codes//yingzi-data-datawarehouse-release//source//sql//doris//fpf//hour//ads//ads_pig_feed_sum_month.sql"
    # if os.path.exists(target_file):
    #     print(f"\n=== 测试单个文件 ===")
    #     results = analyzer.analyze(target_file)
    #     print(f"MD 文档：{list(results.values())[0]}")
    # else:
    #     print(f"错误：文件不存在 {target_file}")
    
    # 测试目录批量处理
    target_dir = "D://codes//yingzi-data-datawarehouse-release//source//sql//doris//fpf//hour//ads"
    if os.path.exists(target_dir):
        print(f"\n=== 测试目录批量处理 ===")
        results = analyzer.analyze(target_dir)
        print(f"\n批量处理完成！共生成 {len(results)} 个 MD 文档")
        for sql_path, md_path in results.items():
            print(f"  {os.path.basename(sql_path)} -> {os.path.basename(md_path)}")


if __name__ == "__main__":
    main()
