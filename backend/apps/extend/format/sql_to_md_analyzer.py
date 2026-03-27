import os
import sys
import yaml
import logging
import time
from typing import Dict, Any, List
from datetime import datetime
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed

# 添加 data_governance_agent 到路径
from apps.extend.utils.utils import Utils

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import config

logger = logging.getLogger("SQLToMDAnalyzer")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


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
            prompt_config = Utils.load_prompt_template_static(template_name)
            
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
    """SQL 到 MD 文档分析器 - 简化版"""

    def __init__(self, max_workers: int = 5, task_interval: int = 5, batch_interval: int = 30):
        """
        初始化分析器
            
        Args:
            max_workers: 最大并发线程数（默认：5）
            task_interval: 任务提交间隔时间，单位秒（默认：5 秒）
            batch_interval: 批次间等待时间，单位秒（默认：30 秒，根据负载动态调整）
        """
        # 简化输出目录：当前脚本目录/metric_blood
        self.model_client = ModelClient()
        # 创建线程池，控制最大并发数
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        # 任务提交间隔
        self.task_interval = task_interval
        # 批次间等待时间（动态调整）
        self.batch_interval = batch_interval
        # 任务耗时统计
        self.task_durations = {}
        logger.info(f"SQLToMDAnalyzer 初始化完成，最大并发线程数：{max_workers}, 任务间隔：{task_interval}秒，批次间隔：{batch_interval}秒")

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

    def generate_md_file(self, analysis_result: Dict[str, Any], md_output_dir: str = 'metric_blood') -> str:
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
        md_output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), md_output_dir)
        os.makedirs(md_output_path, exist_ok=True)
        md_file_path = os.path.join(md_output_path, md_file_name)
        
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

    def analyze_directory(self, sql_dir: str) -> list[str]:
        """
        分析整个目录中的 SQL 文件（使用线程池并发处理）
        
        Args:
            sql_dir: SQL 文件目录路径
            
        Returns:
            分析结果列表 [md_path, ...]
        """
        results = []
        
        if not os.path.exists(sql_dir):
            logger.error(f"SQL 目录不存在：{sql_dir}")
            return results
        
        # 收集所有 SQL 文件
        sql_files = []
        for root, _, files in os.walk(sql_dir):
            for file in files:
                if file.endswith(".sql"):
                    sql_files.append(os.path.join(root, file))
        
        if not sql_files:
            logger.warning(f"目录中未找到 SQL 文件：{sql_dir}")
            return results
        
        total_count = len(sql_files)
        logger.info(f"开始批量分析，共发现 {total_count} 个 SQL 文件")
        
        # 使用线程池并发处理
        future_to_file = {}
        batch_start_time = time.time()
        last_submit_time = 0  # 上次提交任务的时间
        
        for idx, sql_path in enumerate(sql_files, 1):
            # 控制任务提交间隔（每个任务间隔 5 秒）
            elapsed_since_last = time.time() - last_submit_time
            if idx > 1 and elapsed_since_last < self.task_interval:
                wait_time = self.task_interval - elapsed_since_last
                logger.info(f"等待 {wait_time:.1f}秒后提交下一个任务...")
                time.sleep(wait_time)
            
            logger.info(f"提交任务 {idx}/{total_count}: {os.path.basename(sql_path)}")
            future = self.executor.submit(self._process_single_file, sql_path)
            future_to_file[future] = sql_path
            last_submit_time = time.time()  # 记录本次提交时间
        
        # 等待所有任务完成
        logger.info(f"所有任务已提交，等待处理完成...")
        
        # 收集结果
        success_count = 0
        fail_count = 0
        for future in as_completed(future_to_file):
            sql_path = future_to_file[future]
            try:
                md_path = future.result()
                if md_path:
                    results.append(md_path)
                    success_count += 1
                    logger.info(f"✓ 处理成功：{os.path.basename(sql_path)} -> {os.path.basename(md_path)}")
                else:
                    fail_count += 1
                    logger.warning(f"✗ 处理失败（无输出）: {os.path.basename(sql_path)}")
            except Exception as e:
                fail_count += 1
                logger.error(f"✗ 处理异常：{os.path.basename(sql_path)} - {str(e)}")
        
        # 所有任务完成后，检查总耗时
        batch_elapsed = time.time() - batch_start_time
        
        # 检查是否有任务耗时超过 2 分钟
        slow_tasks = []
        for task_path, duration in self.task_durations.items():
            if duration > 120:  # 2 分钟 = 120 秒
                slow_tasks.append((os.path.basename(task_path), duration))
        
        # 如果有慢任务，增加下次分批等待时间
        if slow_tasks:
            increase_time = len(slow_tasks) * 5
            old_batch_interval = self.batch_interval
            self.batch_interval += increase_time
            logger.warning(f"⚠️  检测到 {len(slow_tasks)} 个慢任务 (耗时>2 分钟):")
            for task_name, duration in slow_tasks:
                logger.warning(f"   - {task_name}: {duration:.2f}秒")
            logger.warning(f"下次分批等待时间调整为：{old_batch_interval}秒 + {increase_time}秒 = {self.batch_interval}秒")
        
        # 如果总耗时 < 30 秒，立即继续，不等待
        if batch_elapsed < 30:
            logger.info(f"本批次总耗时 {batch_elapsed:.2f}秒 < 30 秒，立即继续下一批")
        else:
            # 总耗时 >= 30 秒，检查是否需要额外等待
            if batch_elapsed < self.batch_interval:
                wait_time = self.batch_interval - batch_elapsed
                logger.info(f"本批次总耗时 {batch_elapsed:.2f}秒，等待 {wait_time:.1f}秒后继续...")
                time.sleep(wait_time)
            else:
                logger.info(f"本批次总耗时 {batch_elapsed:.2f}秒，已满足分批间隔要求")
        
        logger.info(f"分析完成！共处理 {total_count} 个文件，成功 {success_count} 个，失败 {fail_count} 个")
        return results
    
    def _process_single_file(self, sql_path: str) -> str:
        """
        处理单个 SQL 文件（在线程中执行）
        
        Args:
            sql_path: SQL 文件路径
            
        Returns:
            MD 文件路径
        """
        start_time = datetime.now()
        logger.info(f"开始处理：{os.path.basename(sql_path)}")
        
        try:
            analysis_result = self.analyze_sql_file(sql_path)
            md_path = self.generate_md_file(analysis_result)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # 记录任务耗时
            self.task_durations[sql_path] = duration
            
            logger.info(f"完成处理：{os.path.basename(sql_path)} (耗时：{duration:.2f}秒)")
            
            return md_path
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            self.task_durations[sql_path] = duration
            logger.error(f"处理失败：{os.path.basename(sql_path)} (耗时：{duration:.2f}秒) - {str(e)}")
            raise
    
    def analyze(self, dir_path: str,file_name: str = None) -> List[str]:
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

        if file_name and os.path.isdir(dir_path):
            file_path = os.path.join(dir_path, file_name)
            logger.info(f"开始分析文件：{file_path}")
            if os.path.isfile(file_path):
                # 单个文件
                analysis_result = self.analyze_sql_file(file_path)
                md_path = self.generate_md_file(analysis_result)
                return [md_path] if md_path else []
        
        elif os.path.isdir(dir_path):
            # 目录
            logger.info(f"开始批量分析目录：{dir_path}")
            return self.analyze_directory(dir_path)
        
        else:
            logger.error(f"不支持的路径类型：{dir_path}")
            return {}


def main():
    """主函数 - 测试用"""
    # max_workers=5 表示最多同时处理 5 个文件
    analyzer = SQLToMDAnalyzer(max_workers=5)


    target_dir = "D://codes//yingzi-data-datawarehouse-release//source//sql//doris//fpf//hour//minute"
    # 测试单个文件
    # file_name = "ads_pig_feed_sum_month.sql"
    # print(f"\n=== 测试单个文件 ===")
    # results = analyzer.analyze(target_dir,file_name)
    # print(f"\n分析完成！MD 文档：{results}")

    # 测试目录批量处理
    print(f"\n=== 测试目录批量处理 ===")
    results = analyzer.analyze(target_dir)
    print(f"\n批量处理完成！共生成 {len(results)} 个 MD 文档")


if __name__ == "__main__":
    main()
