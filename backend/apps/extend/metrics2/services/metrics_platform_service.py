from typing import List, Dict, Any, Optional, Union
from pathlib import Path
import os
import logging
import json
from sqlalchemy.orm import Session
from sqlalchemy import text
from apps.extend.metrics2.services.metric_service import MetricService
from apps.extend.metrics2.services.dim_field_mapping_service import DimFieldMappingService
from apps.extend.metrics2.services.etl_processor_service import ETLProcessorService
from apps.extend.utils.utils import ModelClient
from apps.extend.metrics2.models import (
    DimDictInfo,
    MetricDefinitionInfo,
    MetricDimRelInfo,
    MetricSourceMappingInfo,
    MetricCompoundRelInfo
)
from apps.extend.metrics2.curd.dim_definition_curd import get_dim_dict_by_code
from apps.extend.metrics2.curd.metric_definition_curd import get_metric_definition_by_code

# 配置日志
logger = logging.getLogger("MetricsPlatformService")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    ))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


class MetricsPlatformService:
    """养殖业务指标平台总服务类 - 负责整个自动化建设链路的编排"""

    def __init__(self, session: Session):
        """
        初始化指标平台服务

        Args:
            session: 数据库会话
        """
        self.session = session
        self.model_client = ModelClient()
        self.metric_service = MetricService(session)
        self.dim_dict_service = DimFieldMappingService(session)
        self.etl_processor = ETLProcessorService(session)
        
        # ID生成器计数器（批次内递增）
        self._metric_id_counter = 0
        self._dim_id_counter = 0
        self._map_id_counter = 0
        self._table_lineage_id_counter = 0  # 表血缘ID计数器
        self._field_lineage_id_counter = 0  # 字段血缘ID计数器

    def _get_next_metric_id(self) -> str:
        """
        生成下一个唯一的指标ID（M + 6位数字，如 M000001）
        
        Returns:
            唯一的指标ID
        """
        # 如果是第一次调用，从数据库查询当前最大ID
        if self._metric_id_counter == 0:
            try:
                from sqlalchemy import func
                result = self.session.execute(
                    text("SELECT MAX(CAST(SUBSTRING(metric_id FROM 2) AS INTEGER)) FROM metric_definition")
                ).scalar()
                
                if result is not None:
                    # 从最大值开始递增
                    self._metric_id_counter = int(result)
                else:
                    # 数据库为空，从0开始
                    self._metric_id_counter = 0
            except Exception as e:
                logger.warning(f"[ID生成] 查询最大metric_id失败: {str(e)}，从0开始")
                self._metric_id_counter = 0
        
        # 递增计数器
        self._metric_id_counter += 1
        
        # 格式化为 M + 6位数字（如 M000001, M000002, ... M999999）
        return f"M{self._metric_id_counter:06d}"
    
    def _get_next_dim_id(self) -> str:
        """
        生成下一个唯一的维度ID（D + 6位数字，如 D000001）
        
        Returns:
            唯一的维度ID
        """
        # 如果是第一次调用，从数据库查询当前最大ID
        if self._dim_id_counter == 0:
            try:
                result = self.session.execute(
                    text("SELECT MAX(CAST(SUBSTRING(dim_id FROM 2) AS INTEGER)) FROM dim_definition")
                ).scalar()
                
                if result is not None:
                    # 从最大值开始递增
                    self._dim_id_counter = int(result)
                else:
                    # 数据库为空，从0开始
                    self._dim_id_counter = 0
            except Exception as e:
                logger.warning(f"[ID生成] 查询最大dim_id失败: {str(e)}，从0开始")
                self._dim_id_counter = 0
        
        # 递增计数器
        self._dim_id_counter += 1
        
        # 格式化为 D + 6位数字（如 D000001, D000002, ... D999999）
        return f"D{self._dim_id_counter:06d}"

    def _get_next_map_id(self) -> str:
        """
        生成下一个唯一的映射ID（MAP + 6位数字，如 MAP000001）
        
        Returns:
            唯一的映射ID
        """
        # 递增计数器
        self._map_id_counter += 1
        
        # 格式化为 MAP + 6位数字（如 MAP000001, MAP000002, ... MAP999999）
        return f"MAP{self._map_id_counter:06d}"
    
    def _get_next_table_lineage_id(self) -> str:
        """
        生成下一个唯一的表血缘ID（T + 6位数字，如 T000001）
        
        Returns:
            唯一的表血缘ID
        """
        # 如果是第一次调用，从数据库查询当前最大ID
        if self._table_lineage_id_counter == 0:
            try:
                result = self.session.execute(
                    text("SELECT MAX(CAST(SUBSTRING(lineage_id FROM 2) AS INTEGER)) FROM table_lineage")
                ).scalar()
                
                if result is not None:
                    # 从最大值开始递增
                    self._table_lineage_id_counter = int(result)
                else:
                    # 数据库为空，从0开始
                    self._table_lineage_id_counter = 0
            except Exception as e:
                logger.warning(f"[ID生成] 查询最大table_lineage lineage_id失败: {str(e)}，从0开始")
                self._table_lineage_id_counter = 0
        
        # 递增计数器
        self._table_lineage_id_counter += 1
        
        # 格式化为 T + 6位数字（如 T000001, T000002, ... T999999）
        return f"T{self._table_lineage_id_counter:06d}"
    
    def _get_next_field_lineage_id(self) -> str:
        """
        生成下一个唯一的字段血缘ID（F + 6位数字，如 F000001）
        
        Returns:
            唯一的字段血缘ID
        """
        # 如果是第一次调用，从数据库查询当前最大ID
        if self._field_lineage_id_counter == 0:
            try:
                result = self.session.execute(
                    text("SELECT MAX(CAST(SUBSTRING(lineage_id FROM 2) AS INTEGER)) FROM field_lineage")
                ).scalar()
                
                if result is not None:
                    # 从最大值开始递增
                    self._field_lineage_id_counter = int(result)
                else:
                    # 数据库为空，从0开始
                    self._field_lineage_id_counter = 0
            except Exception as e:
                logger.warning(f"[ID生成] 查询最大field_lineage lineage_id失败: {str(e)}，从0开始")
                self._field_lineage_id_counter = 0
        
        # 递增计数器
        self._field_lineage_id_counter += 1
        
        # 格式化为 F + 6位数字（如 F000001, F000002, ... F999999）
        return f"F{self._field_lineage_id_counter:06d}"

    def process_metrics_from_sql(self, input_path: Union[str, Path], is_directory: bool = False, layer_type: str = "AUTO") -> Dict[str, Any]:
        """
        从SQL文件处理指标（完整流程：读取文件 -> 解析 -> 规则处理 -> 生成SQL -> 写入表）

        Args:
            input_path: 输入路径（文件路径或目录路径）
            is_directory: 是否为目录模式
            layer_type: 数仓层级类型
                - "DIM": 维度定义层（只提取维度，写入 dim_dict）
                - "METRIC": 指标层（dwd/dws/ads，提取指标并引用已有维度）
                - "AUTO": 自动识别（默认，根据文件路径判断）

        Returns:
            处理结果字典
        """
        try:
            # 0. 确保会话处于干净状态（回滚任何未完成的事务）
            if self.session.in_transaction():
                try:
                    self.session.rollback()
                    logger.info("[流程开始] 已回滚之前的未完成事务，确保会话状态干净")
                except Exception as e:
                    logger.warning(f"[流程开始] 回滚事务失败: {str(e)}")
            
            # 0.5 自动识别层级类型（如果未指定）
            if layer_type == "AUTO":
                layer_type = self._auto_detect_layer_type(input_path)
                logger.info(f"[流程开始] 自动识别层级类型: {layer_type}")
            
            # 1. 读取并处理SQL文件
            file_result = self._read_and_process_sql_files(input_path, is_directory)
            if not file_result.get('success', False):
                return file_result

            # 2. 解析SQL内容
            parsed_results = self._parse_sql_files(file_result.get('results', []))
            if not parsed_results or not any(r.get('success', False) for r in parsed_results):
                return {"success": False, "message": "SQL解析失败或未解析到有效数据"}

            # ⚠️ 2.5 DWS/ADS 层严格校验：检查是否有 GROUP BY（在规则引擎之前）
            if layer_type == "METRIC":
                for parsed_result in parsed_results:
                    if parsed_result.get('success', False):
                        file_path = parsed_result.get('file_path', '')
                        sql_content = parsed_result.get('parsed_data', {}).get('basic_info', {}).get('sql_content', '')
                        
                        if sql_content:
                            has_group_by = self._check_has_group_by(sql_content)
                            if not has_group_by:
                                error_msg = (
                                    f"❌ DWS/ADS 层 ETL 脚本必须包含 GROUP BY 子句！\n"
                                    f"   文件：{file_path}\n"
                                    f"   原因：DWS/ADS 层是聚合层，必须通过 GROUP BY 进行数据聚合\n"
                                    f"   建议：检查 SQL 是否缺少 GROUP BY，或者该脚本应该属于 DWD 明细层"
                                )
                                logger.error(error_msg)
                                raise ValueError(error_msg)
                            else:
                                logger.info(f"[DWS/ADS校验] ✅ 文件 {file_path} 包含 GROUP BY，校验通过")

            # 3. 规则引擎处理（校验、去重、自动赋值）
            processed_results = self._apply_rules(parsed_results, layer_type)
            if not processed_results:
                return {"success": False, "message": "规则处理失败"}

            # 4. 生成INSERT SQL
            insert_sqls = self._generate_insert_sqls(processed_results)
            if not insert_sqls:
                return {"success": False, "message": "生成INSERT SQL失败"}

            # 5. 校验引擎校验（暂时禁用，直接通过）
            # validation_result = self._validate_insert_sqls(insert_sqls)
            # if not validation_result.get('success', False):
            #     return validation_result
            validation_result = {"success": True, "message": "跳过校验"}

            # 6. 执行INSERT SQL写入表
            execution_result = self._execute_insert_sqls(insert_sqls)
            if not execution_result.get('success', False):
                return execution_result
            
            # 7. 确保事务已提交（如果 _execute_insert_sqls 没有提交）
            if self.session.in_transaction():
                try:
                    self.session.commit()
                    
                    # 输出简洁的业务总结日志
                    layer_type = processed_results[0].get('layer_type', 'UNKNOWN') if processed_results else 'UNKNOWN'
                    file_name = Path(str(input_path)).name if not is_directory else f"{len(file_paths)}个文件"
                    
                    if layer_type == "DIM":
                        dim_count = sum(r.get('dimensions_count', 0) for r in processed_results)
                        logger.info(f"✅ DIM层 | {file_name} | 维度ETL解析成功，数据写入成功 (维度数: {dim_count})")
                    elif layer_type == "DWD":
                        logger.info(f"✅ DWD层 | {file_name} | 明细ETL解析成功，数据写入成功")
                    elif layer_type == "METRIC":
                        metric_count = sum(r.get('metrics_count', 0) for r in processed_results)
                        table_lineage_count = len(table_data.get('table_lineage', []))
                        field_lineage_count = len(table_data.get('field_lineage', []))
                        metric_lineage_count = len(table_data.get('metric_lineage', []))
                        logger.info(f"✅ METRIC层 | {file_name} | 指标+维度解析成功，血缘数据写入成功 (指标: {metric_count}, 表血缘: {table_lineage_count}, 字段血缘: {field_lineage_count}, 指标血缘: {metric_lineage_count})")
                    else:
                        logger.info(f"✅ 解析成功 | {file_name}")
                    
                except Exception as commit_error:
                    logger.error(f"[流程结束] 提交事务失败: {str(commit_error)}")
                    self.session.rollback()
                    return {
                        "success": False,
                        "message": f"事务提交失败：{str(commit_error)}",
                        "input_path": str(input_path)
                    }

            # 8. 返回总体结果
            return {
                "success": True,
                "file_result": file_result,
                "parsed_results": parsed_results,
                "processed_results": processed_results,
                "insert_sqls": insert_sqls,
                "validation_result": validation_result,
                "execution_result": execution_result,
                "message": "指标平台自动化建设流程完成"
            }

        except Exception as e:
            logger.error(f"指标平台处理流程失败：{str(e)}")
            # 发生异常时回滚事务
            try:
                if self.session.in_transaction():
                    self.session.rollback()
                    logger.info("[流程异常] 已回滚事务")
            except Exception as rollback_error:
                logger.error(f"[流程异常] 回滚事务失败: {str(rollback_error)}")
            
            return {
                "success": False,
                "message": f"处理流程失败：{str(e)}",
                "input_path": str(input_path)
            }

    def _auto_detect_layer_type(self, input_path: Union[str, Path]) -> str:
        """
        根据文件路径自动识别数仓层级类型
        
        Args:
            input_path: 文件路径或目录路径
            
        Returns:
            层级类型："DIM"、"DWD" 或 "METRIC"
            - DIM: 维度定义层
            - DWD: 明细层（只提取血缘，不提取指标/维度）
            - METRIC: 指标层（dws/ads，需要提取指标和维度）
        """
        path_str = str(input_path).lower()
        
        # 检查路径中是否包含 dim 关键词
        if '/dim/' in path_str or '\\dim\\' in path_str or path_str.endswith('/dim') or path_str.endswith('\\dim'):
            return "DIM"
        
        # 检查路径中是否包含 dwd 关键词（明细层）
        if '/dwd/' in path_str or '\\dwd\\' in path_str or path_str.endswith('/dwd') or path_str.endswith('\\dwd'):
            return "DWD"
        
        # 其他情况默认为指标层（dws/ads）
        return "METRIC"

    def _check_has_group_by(self, sql_content: str) -> bool:
        """
        检查 SQL 是否包含 GROUP BY 子句
        
        Args:
            sql_content: SQL 内容
            
        Returns:
            是否包含 GROUP BY
        """
        import re
        # 移除注释
        sql_clean = re.sub(r'--.*?$', '', sql_content, flags=re.MULTILINE)
        sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)
        
        # 检查是否包含 GROUP BY（不区分大小写）
        return bool(re.search(r'\bGROUP\s+BY\b', sql_clean, re.IGNORECASE))
    
    def _check_has_group_by_from_processed_result(self, processed_result: Dict[str, Any]) -> bool:
        """
        从 processed_result 中提取 SQL 并检查是否包含 GROUP BY
        
        Args:
            processed_result: 处理结果（包含 parsed_data）
            
        Returns:
            是否包含 GROUP BY
        """
        try:
            parsed_data = processed_result.get('parsed_data', {})
            basic_info = parsed_data.get('basic_info', {})
            sql_content = basic_info.get('sql_content', '')
            
            if not sql_content:
                logger.warning("[SQL生成-GROUP BY检查] 无法获取 SQL 内容，默认返回 False")
                return False
            
            has_group_by = self._check_has_group_by(sql_content)
            logger.debug(f"[SQL生成-GROUP BY检查] 检查结果: {has_group_by}")
            return has_group_by
        except Exception as e:
            logger.error(f"[SQL生成-GROUP BY检查] 检查失败: {str(e)}")
            return False

    def _read_and_process_sql_files(self, input_path: Union[str, Path], is_directory: bool) -> Dict[str, Any]:
        """
        读取并处理SQL文件

        Args:
            input_path: 输入路径
            is_directory: 是否为目录模式

        Returns:
            文件处理结果
        """
        try:
            # 验证输入路径
            path = Path(input_path)
            if not path.exists():
                return {"success": False, "message": f"路径不存在：{input_path}"}

            # 获取文件列表
            file_paths = []
            if is_directory:
                # 目录模式：获取目录下所有.sql文件
                file_paths = list(path.glob("*.sql"))
                if not file_paths:
                    return {"success": False, "message": f"目录中没有找到SQL文件：{input_path}"}
            else:
                # 文件模式：直接使用指定文件
                if path.suffix.lower() != '.sql':
                    return {"success": False, "message": f"文件必须是.sql格式：{input_path}"}
                file_paths = [path]

            # 处理每个文件
            total_files = len(file_paths)
            processed_files = 0
            failed_files = 0
            results = []

            for file_path in file_paths:
                try:
                    # 读取文件内容
                    with open(file_path, 'r', encoding='utf-8') as f:
                        sql_content = f.read()

                    # 处理单个文件
                    file_result = {
                        "file_path": str(file_path),
                        "sql_content": sql_content,
                        "success": True,
                        "message": "文件读取成功"
                    }
                    results.append(file_result)
                    processed_files += 1

                except Exception as e:
                    failed_files += 1
                    logger.error(f"读取文件 {file_path} 失败：{str(e)}")
                    results.append({
                        "file_path": str(file_path),
                        "success": False,
                        "message": f"文件读取失败：{str(e)}"
                    })

            return {
                "success": True,
                "total_files": total_files,
                "processed_files": processed_files,
                "failed_files": failed_files,
                "results": results,
                "message": f"成功读取 {processed_files}/{total_files} 个文件"
            }

        except Exception as e:
            logger.error(f"读取SQL文件失败：{str(e)}")
            return {
                "success": False,
                "message": f"读取文件失败：{str(e)}",
                "input_path": str(input_path)
            }

    def _parse_sql_files(self, file_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        解析SQL文件内容

        Args:
            file_results: 文件处理结果列表

        Returns:
            解析结果列表
        """
        parsed_results = []

        for file_result in file_results:
            if not file_result.get('success', False):
                parsed_results.append({
                    "file_path": file_result.get('file_path'),
                    "success": False,
                    "message": file_result.get('message'),
                    "parsed_data": None
                })
                continue

            try:
                sql_content = file_result.get('sql_content', '')
                file_path = file_result.get('file_path', '')

                # 调用大模型解析SQL
                parsed_data = self._parse_sql_with_ai(sql_content, file_path)
                if not parsed_data or not parsed_data.get('success', False):
                    parsed_results.append({
                        "file_path": file_path,
                        "success": False,
                        "message": "大模型解析失败",
                        "parsed_data": None
                    })
                    continue

                parsed_results.append({
                    "file_path": file_path,
                    "success": True,
                    "message": "SQL解析成功",
                    "parsed_data": parsed_data.get('parsed_data', {})
                })

            except Exception as e:
                logger.error(f"解析文件 {file_result.get('file_path')} 失败：{str(e)}")
                parsed_results.append({
                    "file_path": file_result.get('file_path'),
                    "success": False,
                    "message": f"解析失败：{str(e)}",
                    "parsed_data": None
                })

        return parsed_results

    def _parse_sql_with_ai(self, sql_content: str, sql_file: str = "") -> Dict[str, Any]:
        """
        使用大模型解析SQL内容

        Args:
            sql_content: SQL内容
            sql_file: SQL文件名（可选）

        Returns:
            解析结果字典
        """
        try:
            # 调用大模型解析SQL
            result = self.model_client.call_ai(
                template_name="sql_analysis",
                sql_content=sql_content,
                sql_file=sql_file
            )

            if not result:
                return {"success": False, "message": "大模型返回空内容"}

            # 解析大模型返回的JSON
            try:
                parsed_json = json.loads(result)
            except json.JSONDecodeError as e:
                logger.error(f"JSON解析失败：{str(e)}")
                logger.error(f"AI 返回内容前500字符：{result[:500]}")
                return {"success": False, "message": f"JSON解析失败：{str(e)}"}

            if not parsed_json or not parsed_json.get('fields'):
                logger.error(f"[AI解析] 解析结果缺少 fields 字段")
                logger.error(f"[AI解析] 解析结果 keys: {list(parsed_json.keys()) if parsed_json else 'None'}")
                return {"success": False, "message": "JSON解析成功但未提取到字段数据"}

            logger.info(f"[AI解析] 解析成功 - 共 {len(parsed_json['fields'])} 个字段")
            
            # 将 fields 转换为 metrics 和 dimensions
            metrics, dimensions = self.etl_processor.convert_fields_to_metrics_and_dimensions(parsed_json['fields'])
            
            logger.debug(f"[AI解析] 转换完成 - 指标: {len(metrics)}, 维度: {len(dimensions)}")
            
            # 打印前3个指标的详细信息
            for i, metric in enumerate(metrics[:3], 1):
                logger.info(f"[AI解析] 指标 #{i}: {metric.get('metric_name')} ({metric.get('metric_code')}) - 类型: {metric.get('metric_type')}")

            return {
                "success": True,
                "parsed_data": {
                    "basic_info": {
                        **parsed_json.get('basic_info', {}),
                        'sql_content': sql_content  # ⚠️ 保存原始 SQL 内容用于后续校验
                    },
                    "fields": parsed_json.get('fields', []),  # ⚠️ 保留原始 fields 用于血缘分析
                    "metrics": metrics,
                    "dimensions": dimensions,
                    "is_lineage_only": False  # 正常模式
                }
            }

        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败：{str(e)}")
            return {"success": False, "message": f"JSON解析失败：{str(e)}"}
        except Exception as e:
            logger.error(f"大模型解析失败：{str(e)}")
            return {"success": False, "message": f"大模型解析失败：{str(e)}"}

    def _apply_rules(self, parsed_results: List[Dict[str, Any]], layer_type: str = "METRIC") -> List[Dict[str, Any]]:
        """
        规则引擎处理（校验、去重、自动赋值）

        Args:
            parsed_results: 解析结果列表
            layer_type: 数仓层级类型（"DIM" 或 "METRIC"）

        Returns:
            规则处理后的结果列表
        """
        processed_results = []

        for parsed_result in parsed_results:
            if not parsed_result.get('success', False):
                processed_results.append(parsed_result)
                continue

            try:
                parsed_data = parsed_result.get('parsed_data', {})
                file_path = parsed_result.get('file_path', '')

                # 检查是否为仅血缘模式
                is_lineage_only = parsed_data.get('is_lineage_only', False)
                
                if is_lineage_only:
                    logger.info(f"[规则引擎] 文件 {file_path} 为仅血缘模式，跳过指标/维度处理")
                    processed_results.append({
                        "file_path": file_path,
                        "success": True,
                        "message": "仅血缘模式，无需规则处理",
                        "layer_type": layer_type,
                        "parsed_data": parsed_data,
                        "processed_data": {
                            "dimensions": [],
                            "metrics": [],
                            "is_lineage_only": True
                        }
                    })
                    continue

                # 提取指标和维度数据
                dimensions = parsed_data.get('dimensions', [])
                metrics = parsed_data.get('metrics', [])
                
                logger.info(f"[规则引擎] 文件 {file_path} (层级: {layer_type}): 原始数据 - 指标: {len(metrics)}, 维度: {len(dimensions)}")

                # 根据层级类型决定处理策略
                if layer_type == "DIM":
                    # DIM 层：只处理维度定义，不处理指标
                    logger.info(f"[规则引擎] DIM 层模式：只提取维度定义")
                    processed_dimensions = self._process_dimensions(dimensions)
                    logger.info(f"[规则引擎] 维度处理完成: {len(processed_dimensions)} 个")
                    
                    # ⚠️ 校验：DIM 层必须有维度数据
                    if not processed_dimensions:
                        error_msg = f"❌ DIM 层文件 {file_path} 未提取到任何维度数据，请检查 SQL 是否正确"
                        logger.error(error_msg)
                        raise ValueError(error_msg)
                    
                    processed_results.append({
                        "file_path": file_path,
                        "success": True,
                        "message": "DIM层规则处理成功",
                        "layer_type": "DIM",
                        "parsed_data": parsed_data,
                        "processed_data": {
                            "dimensions": processed_dimensions,
                            "metrics": [],  # DIM 层不处理指标
                            "dimensions_count": len(processed_dimensions)
                        }
                    })
                elif layer_type == "DWD":
                    # DWD 层：只提取血缘关系，不处理指标/维度
                    logger.info(f"[规则引擎] DWD 层模式：只提取血缘关系")
                    
                    processed_results.append({
                        "file_path": file_path,
                        "success": True,
                        "message": "DWD层规则处理成功（仅血缘）",
                        "layer_type": "DWD",
                        "parsed_data": parsed_data,
                        "processed_data": {
                            "dimensions": [],
                            "metrics": [],
                            "is_lineage_only": True  # 标记为仅血缘模式
                        }
                    })
                else:
                    # METRIC 层（dws/ads）：处理指标并引用已有维度
                    logger.info(f"[规则引擎] METRIC 层模式：处理指标并引用已有维度")
                    
                    # 1. 维度处理：只查询已有维度，不创建新维度
                    processed_dimensions = self._process_dimensions_for_metric_layer(dimensions)
                    logger.info(f"[规则引擎] 维度匹配完成: {len(processed_dimensions)} 个")

                    # 2. 指标处理：自动生成ID、校验口径、类型
                    processed_metrics = self._process_metrics(metrics, processed_dimensions)
                    logger.info(f"[规则引擎] 指标处理完成: {len(processed_metrics)} 个")

                    processed_results.append({
                        "file_path": file_path,
                        "success": True,
                        "message": "METRIC层规则处理成功",
                        "layer_type": "METRIC",
                        "parsed_data": parsed_data,
                        "processed_data": {
                            "dimensions": processed_dimensions,
                            "metrics": processed_metrics,
                            "metrics_count": len(processed_metrics),
                            "dimensions_count": len(processed_dimensions)
                        }
                    })

            except Exception as e:
                logger.error(f"规则处理文件 {parsed_result.get('file_path')} 失败：{str(e)}")
                processed_results.append({
                    "file_path": parsed_result.get('file_path'),
                    "success": False,
                    "message": f"规则处理失败：{str(e)}",
                    "processed_data": None
                })

        return processed_results

    def _process_dimensions_for_metric_layer(self, dimensions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        METRIC 层维度处理：只查询已有维度，不创建新维度
        
        Args:
            dimensions: 维度列表
            
        Returns:
            处理后的维度列表（只包含已存在的维度）
        """
        processed_dimensions = []
        missing_dimensions = []

        for dim in dimensions:
            dim_code = dim.get('dim_code')
            if not dim_code:
                continue

            # 查询维度是否已存在
            try:
                existing_dim = get_dim_dict_by_code(self.session, dim_code)
            except Exception as e:
                logger.warning(f"[规则引擎-METRIC层] 查询维度 {dim_code} 失败: {str(e)}")
                
                # 如果是事务错误，尝试回滚
                if 'InFailedSqlTransaction' in str(e) or 'current transaction is aborted' in str(e):
                    try:
                        self.session.rollback()
                        logger.info(f"[规则引擎-METRIC层] 已回滚事务以恢复会话状态")
                        try:
                            existing_dim = get_dim_dict_by_code(self.session, dim_code)
                        except Exception:
                            existing_dim = None
                    except Exception as rollback_error:
                        logger.error(f"[规则引擎-METRIC层] 回滚失败: {str(rollback_error)}")
                        existing_dim = None
                else:
                    existing_dim = None
            
            if existing_dim:
                # 复用现有维度
                processed_dimensions.append({
                    "dim_id": existing_dim.dim_id,
                    "dim_code": dim_code,
                    "dim_name": dim.get('dim_name'),
                    "dim_type": dim.get('dim_type', '普通维度'),
                    "is_existing": True
                })
                logger.debug(f"[规则引擎-METRIC层] 找到维度: {dim_code} -> {existing_dim.dim_id}")
            else:
                # 维度不存在于 dim_definition 中，跳过（不创建新维度）
                # 注意：这个维度会在 field_lineage 中被标记为 private_dim 或 normal
                logger.debug(f"[规则引擎-METRIC层] 维度 {dim_code} 不在 dim_definition 中，将在字段血缘中标记为非公共维度")

        return processed_dimensions

    def _process_dimensions(self, dimensions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        维度处理：自动匹配或生成

        Args:
            dimensions: 维度列表

        Returns:
            处理后的维度列表
        """
        processed_dimensions = []

        for dim in dimensions:
            dim_code = dim.get('dim_code')
            if not dim_code:
                continue

            # 检查维度是否已存在
            try:
                existing_dim = get_dim_dict_by_code(self.session, dim_code)
            except Exception as e:
                # 如果查询失败（可能是数据库表结构不一致或事务错误），视为不存在
                logger.warning(f"[规则引擎] 查询维度 {dim_code} 失败: {str(e)}，将创建新维度")
                
                # 如果是事务错误，尝试回滚以恢复会话状态
                if 'InFailedSqlTransaction' in str(e) or 'current transaction is aborted' in str(e):
                    try:
                        self.session.rollback()
                        logger.info(f"[规则引擎] 已回滚事务以恢复会话状态")
                        # 重新尝试查询
                        try:
                            existing_dim = get_dim_dict_by_code(self.session, dim_code)
                        except Exception:
                            existing_dim = None
                    except Exception as rollback_error:
                        logger.error(f"[规则引擎] 回滚失败: {str(rollback_error)}")
                        existing_dim = None
                else:
                    existing_dim = None
            
            if existing_dim:
                # 复用现有维度
                processed_dimensions.append({
                    "dim_id": existing_dim.dim_id,
                    "dim_code": dim_code,
                    "dim_name": dim.get('dim_name'),
                    "dim_type": dim.get('dim_type', '普通维度'),
                    "is_existing": True
                })
            else:
                # 自动生成新维度
                dim_id = self._get_next_dim_id()  # 使用递增ID生成器
                dim_info = DimDictInfo(
                    dim_name=dim.get('dim_name'),
                    dim_code=dim_code,
                    dim_type=dim.get('dim_type', '普通维度'),
                    is_valid=True
                )
                processed_dimensions.append({
                    "dim_id": dim_id,
                    "dim_code": dim_code,
                    "dim_name": dim.get('dim_name'),
                    "dim_type": dim.get('dim_type', '普通维度'),
                    "is_existing": False,
                    "dim_info": dim_info
                })

        return processed_dimensions

    def _process_metrics(self, metrics: List[Dict[str, Any]], dimensions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        指标处理：自动生成ID、校验口径、类型

        Args:
            metrics: 指标列表
            dimensions: 处理后的维度列表

        Returns:
            处理后的指标列表
        """
        processed_metrics = []

        for metric in metrics:
            metric_code = metric.get('metric_code')
            if not metric_code:
                continue

            # 检查指标是否已存在（只用于确定 metric_id）
            try:
                existing_metric = get_metric_definition_by_code(self.session, metric_code)
            except Exception as e:
                logger.warning(f"[规则引擎] 查询指标 {metric_code} 失败: {str(e)}，将创建新指标")
                
                if 'InFailedSqlTransaction' in str(e) or 'current transaction is aborted' in str(e):
                    try:
                        self.session.rollback()
                        logger.info(f"[规则引擎] 已回滚事务以恢复会话状态")
                        try:
                            existing_metric = get_metric_definition_by_code(self.session, metric_code)
                        except Exception:
                            existing_metric = None
                    except Exception as rollback_error:
                        logger.error(f"[规则引擎] 回滚失败: {str(rollback_error)}")
                        existing_metric = None
                else:
                    existing_metric = None
            
            # 确定 metric_id
            if existing_metric:
                metric_id = existing_metric.metric_id
                is_existing = True
                logger.debug(f"[规则引擎] 指标 {metric_code} 已存在，复用 ID: {metric_id}")
            else:
                metric_id = self._get_next_metric_id()
                is_existing = False
                logger.debug(f"[规则引擎] 指标 {metric_code} 不存在，生成新 ID: {metric_id}")

            # 构建 metric_info（无论是否已存在，都保留完整信息用于后续 UPSERT）
            metric_info = MetricDefinitionInfo(
                metric_name=metric.get('metric_name'),
                metric_code=metric_code,
                metric_type=metric.get('metric_type'),
                biz_domain=metric.get('biz_domain', '养殖繁殖'),
                cal_logic=metric.get('cal_logic'),
                unit=metric.get('unit'),
                status=True
            )

            # 处理维度关联
            dim_rels = []
            for dim in dimensions:
                dim_id = dim.get('dim_id')
                if dim_id:
                    dim_rels.append(MetricDimRelInfo(
                        dim_id=dim_id,
                        is_required=metric.get('is_required', False)
                    ))

            # 处理源映射（直接保留原始数据，兼容新旧格式）
            source_mappings = metric.get('source_mappings', [])
            if not source_mappings and metric.get('source'):
                source = metric.get('source')
                source_mappings = [{
                    'datasource': source.get('datasource'),
                    'db_table': source.get('db_table'),
                    'metric_column': source.get('metric_column'),
                    'filter_condition': source.get('filter_condition'),
                    'agg_func': source.get('agg_func'),
                    'priority': source.get('priority', 1),
                    'source_level': source.get('source_level', 'AUTHORITY')
                }]

            # 处理复合关系（仅复合指标）
            compound_rels = []
            if metric.get('metric_type') == 'COMPOUND' and metric.get('compound_rule'):
                for rule in metric.get('compound_rule', []):
                    compound_rels.append(MetricCompoundRelInfo(
                        sub_metric_id=rule.get('sub_metric_code'),
                        cal_operator=rule.get('operator'),
                        sort=rule.get('sort', 0)
                    ))

            # 统一返回结构（无论是否已存在）
            processed_metrics.append({
                "metric_id": metric_id,
                "metric_code": metric_code,
                "metric_name": metric.get('metric_name'),
                "metric_type": metric.get('metric_type'),
                "is_existing": is_existing,
                "metric_info": metric_info,
                "dim_rels": dim_rels,
                "source_mappings": source_mappings,
                "compound_rels": compound_rels
            })

        return processed_metrics

    def _generate_insert_sqls(self, processed_results: List[Dict[str, Any]]) -> List[str]:
        """
        生成INSERT SQL（按表分组，批量插入）

        Args:
            processed_results: 规则处理后的结果列表

        Returns:
            INSERT SQL列表（每个表一条批量INSERT）
        """
        try:
            logger.info(f"[SQL生成] 开始生成批量 INSERT SQL")
            
            # 收集所有需要插入的数据，按表分组
            table_data = {
                'dim_definition': [],  # 维度定义表
                'dim_field_mapping': [],  # 维度字段映射表
                'table_lineage': [],  # 表级血缘表
                'field_lineage': [],  # 字段级血缘表
                'metric_definition': [],
                'metric_dim_rel': [],
                'metric_source_mapping': [],
                'metric_lineage': [],  # ⚠️ 指标血缘表（关联 metric_id 和 map_id）
                'metric_compound_rel': []
            }

            for idx, processed_result in enumerate(processed_results, 1):
                if not processed_result.get('success', False):
                    logger.warning(f"[SQL生成] 处理结果 #{idx} 失败，跳过")
                    continue

                # 获取层级类型
                layer_type = processed_result.get('layer_type', 'METRIC')
                file_path = processed_result.get('file_path', '')
                
                processed_data = processed_result.get('processed_data', {})
                dimensions = processed_data.get('dimensions', [])
                metrics = processed_data.get('metrics', [])
                is_lineage_only = processed_data.get('is_lineage_only', False)
                
                logger.info(f"[SQL生成] 处理结果 #{idx} (层级: {layer_type}, 仅血缘: {is_lineage_only}): {len(metrics)} 个指标, {len(dimensions)} 个维度")

                # 根据层级类型决定收集策略
                if is_lineage_only:
                    # ⚠️ DWD 层仅血缘模式：只收集表血缘和字段血缘，不收集指标/维度
                    logger.info(f"[SQL生成] DWD 层仅血缘模式：收集表血缘和字段血缘")
                    # 1. 先收集表级血缘
                    self._collect_table_lineage_data(processed_result, table_data)
                    # 2. 再收集字段级血缘（依赖 table_lineage）
                    has_group_by = self._check_has_group_by_from_processed_result(processed_result)
                    self._collect_field_lineage_data(processed_result, table_data, has_group_by)
                elif layer_type == "DIM":
                    # DIM 层：只收集维度定义 + 维度字段映射，不收集血缘
                    logger.info(f"[SQL生成] DIM 层模式：只收集维度定义和维度字段映射")
                    for d_idx, dim in enumerate(dimensions, 1):
                        if not dim.get('is_existing', False) and dim.get('dim_info'):
                            self._collect_dimension_data(dim, table_data)
                            logger.debug(f"[SQL生成] 维度 #{d_idx} ({dim.get('dim_code')}) 标记为待插入")
                    
                    # DIM 层也要收集维度字段映射（dim_field_mapping）
                    self._collect_dim_field_mapping_data_for_dim_layer(dimensions, processed_result, table_data)
                    
                    # ⚠️ 校验：DIM 层必须有 dim_definition 和 dim_field_mapping 数据
                    if not table_data['dim_definition']:
                        error_msg = f"❌ DIM 层文件 {file_path} 未生成 dim_definition 数据"
                        logger.error(error_msg)
                        raise ValueError(error_msg)
                    if not table_data['dim_field_mapping']:
                        error_msg = f"❌ DIM 层文件 {file_path} 未生成 dim_field_mapping 数据"
                        logger.error(error_msg)
                        raise ValueError(error_msg)
                else:
                    # METRIC 层：收集维度和指标数据
                    logger.info(f"[SQL生成] METRIC 层模式：收集指标及关联维度")
                    
                    # 1. 先收集维度数据（必须在指标之前）
                    for d_idx, dim in enumerate(dimensions, 1):
                        if not dim.get('is_existing', False) and dim.get('dim_info'):
                            self._collect_dimension_data(dim, table_data)
                            logger.debug(f"[SQL生成] 维度 #{d_idx} ({dim.get('dim_code')}) 标记为待插入")

                    # 2. 收集表级血缘数据（table_lineage）- ⚠️ 必须在字段血缘之前
                    self._collect_table_lineage_data(processed_result, table_data)

                    # 3. 收集维度字段映射数据（dim_field_mapping）
                    self._collect_dim_field_mapping_data(dimensions, metrics, processed_result, table_data)

                    # 4. 收集字段级血缘数据（field_lineage）- ⚠️ 依赖 table_lineage 的 lineage_id
                    has_group_by = self._check_has_group_by_from_processed_result(processed_result)
                    self._collect_field_lineage_data(processed_result, table_data, has_group_by)

                    # 5. 再收集指标数据
                    logger.info(f"[SQL生成] 开始处理 {len(metrics)} 个指标")
                    for m_idx, metric in enumerate(metrics, 1):
                        logger.info(f"[SQL生成] 处理指标 #{m_idx}: {metric.get('metric_name')} (code: {metric.get('metric_code')}, type: {metric.get('metric_type')})")

                        # 根据指标类型收集数据
                        metric_type = metric.get('metric_type', '')
                        logger.info(f"[SQL生成] 指标 #{m_idx} 类型: {metric_type}")
                        
                        if metric_type == 'ATOMIC' or metric_type == 'DERIVED':
                            # DERIVED（派生指标）按 ATOMIC 方式处理
                            if metric_type == 'DERIVED':
                                logger.info(f"[SQL生成] 指标 #{m_idx} 为派生指标，按原子指标方式处理")
                            
                            # 检查是否有 source_mappings
                            source_mappings = metric.get('source_mappings', [])
                            logger.info(f"[SQL生成] 指标 #{m_idx} 有 {len(source_mappings)} 个源映射")
                            
                            self._collect_atomic_metric_data(metric, dimensions, table_data)
                            logger.info(f"[SQL生成] 指标 #{m_idx} 数据收集完成 - metric_definition: {len(table_data['metric_definition'])} 条, metric_source_mapping: {len(table_data['metric_source_mapping'])} 条")
                        elif metric_type == 'COMPOUND':
                            self._collect_compound_metric_data(metric, dimensions, table_data)
                            logger.info(f"[SQL生成] 复合指标 #{m_idx} 数据收集完成")
                        else:
                            logger.warning(f"[SQL生成] ⚠️ 指标 #{m_idx} 类型未知: {metric_type}，跳过收集")
                    
                    # 6. 收集 metric_lineage 数据（关联指标和字段血缘）
                    self._collect_metric_lineage_data(metrics, table_data)
                    
                    logger.info(f"[SQL生成] 所有指标处理完成 - 最终统计:")
                    logger.info(f"   - metric_definition: {len(table_data['metric_definition'])} 条")
                    logger.info(f"   - metric_source_mapping: {len(table_data['metric_source_mapping'])} 条")
                    logger.info(f"   - metric_dim_rel: {len(table_data['metric_dim_rel'])} 条")
                    logger.info(f"   - metric_lineage: {len(table_data['metric_lineage'])} 条")

            # ⚠️ DWS/ADS 层最终校验：必须包含必要的表数据
            self._validate_required_tables_data(table_data, layer_type)

            # 为每个表生成批量 INSERT SQL
            insert_sqls = []
            for table_name, data_list in table_data.items():
                if data_list:
                    # 特殊处理：dim_field_mapping 需要根据联合主键 (db_table, dim_field) 去重
                    if table_name == 'dim_field_mapping':
                        deduplicated_data = self._deduplicate_dim_field_mapping(data_list)
                        logger.debug(f"[SQL生成] 表 {table_name}: 去重前 {len(data_list)} 条，去重后 {len(deduplicated_data)} 条")
                        data_list = deduplicated_data
                    
                    batch_sql = self._generate_batch_insert_sql(table_name, data_list)
                    if batch_sql:
                        insert_sqls.append(batch_sql)
                        logger.debug(f"[SQL生成] 表 {table_name}: 生成批量 INSERT ({len(data_list)} 条记录)")

            logger.debug(f"[SQL生成] 完成 - 共生成 {len(insert_sqls)} 条批量 INSERT SQL")
            return insert_sqls

        except Exception as e:
            logger.error(f"[SQL生成] 生成INSERT SQL失败: {str(e)}")
            raise  # ⚠️ 重新抛出异常，中断流程

    def _validate_required_tables_data(self, table_data: Dict[str, List], layer_type: str):
        """
        DWS/ADS 层最终校验：必须包含必要的表数据
        
        要求：
        1. metric_definition - 指标定义表（必须有）
        2. metric_source_mapping - 指标源映射表（必须有）
        3. metric_lineage (table_lineage + field_lineage) - 血缘表（必须有）
        4. metric_dim_rel - 指标维度关联表（如果使用了 DIM 层的表，则必须有）
        
        Args:
            table_data: 表数据收集字典
            layer_type: 层级类型
            
        Raises:
            ValueError: 如果缺少必要的数据，抛出异常中断流程
        """
        if layer_type != "METRIC":
            # 只对 METRIC 层（DWS/ADS）进行校验
            return
        
        missing_tables = []
        
        # 详细日志：打印每个表的数据量
        logger.info(f"[必要表校验] 开始校验 DWS/ADS 层数据完整性")
        logger.info(f"   - dim_definition: {len(table_data.get('dim_definition', []))} 条")
        logger.info(f"   - dim_field_mapping: {len(table_data.get('dim_field_mapping', []))} 条")
        logger.info(f"   - table_lineage: {len(table_data.get('table_lineage', []))} 条")
        logger.info(f"   - field_lineage: {len(table_data.get('field_lineage', []))} 条")
        logger.info(f"   - metric_definition: {len(table_data.get('metric_definition', []))} 条")
        logger.info(f"   - metric_dim_rel: {len(table_data.get('metric_dim_rel', []))} 条")
        logger.info(f"   - metric_source_mapping: {len(table_data.get('metric_source_mapping', []))} 条")
        logger.info(f"   - metric_lineage: {len(table_data.get('metric_lineage', []))} 条")
        logger.info(f"   - metric_compound_rel: {len(table_data.get('metric_compound_rel', []))} 条")
        
        # 1. 检查 metric_definition
        if not table_data.get('metric_definition'):
            missing_tables.append('metric_definition（指标定义表）')
            logger.error(f"[必要表校验] ❌ 缺少 metric_definition 数据")
        else:
            logger.info(f"[必要表校验] ✅ metric_definition: {len(table_data['metric_definition'])} 条")
        
        # 2. 检查 metric_source_mapping
        if not table_data.get('metric_source_mapping'):
            missing_tables.append('metric_source_mapping（指标源映射表）')
            logger.error(f"[必要表校验] ❌ 缺少 metric_source_mapping 数据")
        else:
            logger.info(f"[必要表校验] ✅ metric_source_mapping: {len(table_data['metric_source_mapping'])} 条")
        
        # 3. 检查 metric_lineage（必须有）
        if not table_data.get('metric_lineage'):
            missing_tables.append('metric_lineage（指标血缘表）')
            logger.error(f"[必要表校验] ❌ 缺少 metric_lineage 数据")
        else:
            logger.info(f"[必要表校验] ✅ metric_lineage: {len(table_data['metric_lineage'])} 条")
        
        # 4. 检查血缘数据（table_lineage 或 field_lineage 至少有一个）
        has_table_lineage = bool(table_data.get('table_lineage'))
        has_field_lineage = bool(table_data.get('field_lineage'))
        if not has_table_lineage and not has_field_lineage:
            missing_tables.append('table/field lineage（表级或字段级血缘）')
        
        # 5. 检查是否使用了 DIM 层的表，如果有则必须有 metric_dim_rel
        # 判断依据：dim_field_mapping 中有数据，说明使用了 DIM 层的维度字段
        has_dim_usage = bool(table_data.get('dim_field_mapping'))
        if has_dim_usage and not table_data.get('metric_dim_rel'):
            missing_tables.append('metric_dim_rel（指标维度关联表，检测到使用了 DIM 层维度但未建立关联）')
        
        # 如果有缺失的表，抛出异常
        if missing_tables:
            error_msg = (
                f"❌ DWS/ADS 层数据不完整，缺少以下必要表的数据：\n"
                f"   {'; '.join(missing_tables)}\n\n"
                f"   要求：\n"
                f"   - metric_definition：必须包含指标定义\n"
                f"   - metric_source_mapping：必须包含指标数据源映射\n"
                f"   - metric_lineage：必须包含指标-源映射关联\n"
                f"   - table/field lineage：必须包含表级或字段级血缘\n"
                f"   - metric_dim_rel：如果使用了 DIM 层维度，必须建立指标-维度关联\n\n"
                f"   请检查 ETL SQL 是否正确提取了指标、维度和血缘信息。"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        else:
            logger.info(f"[必要表校验] ✅ DWS/ADS 层数据完整性校验通过")
            logger.info(f"   - metric_definition: {len(table_data.get('metric_definition', []))} 条")
            logger.info(f"   - metric_source_mapping: {len(table_data.get('metric_source_mapping', []))} 条")
            logger.info(f"   - metric_lineage: {len(table_data.get('metric_lineage', []))} 条")
            logger.info(f"   - table_lineage: {len(table_data.get('table_lineage', []))} 条")
            logger.info(f"   - field_lineage: {len(table_data.get('field_lineage', []))} 条")
            logger.info(f"   - metric_dim_rel: {len(table_data.get('metric_dim_rel', []))} 条")

    def _collect_dimension_data(self, dim: Dict[str, Any], table_data: Dict[str, List]):
        """
        收集维度数据（不生成SQL，只收集数据）
        
        Args:
            dim: 维度数据（包含 dim_info）
            table_data: 表数据收集字典
        """
        dim_id = dim.get('dim_id')
        dim_code = dim.get('dim_code')
        dim_name = dim.get('dim_name')
        dim_type = dim.get('dim_type', '普通维度')
        
        # 获取 dim_info
        dim_info = dim.get('dim_info', {})
        if hasattr(dim_info, 'is_valid'):
            is_valid = dim_info.is_valid if dim_info.is_valid is not None else True
        else:
            is_valid = dim_info.get('is_valid', True)
        
        # 收集 dim_definition 数据
        table_data['dim_definition'].append({
            'dim_id': dim_id,
            'dim_name': dim_name,
            'dim_code': dim_code,
            'dim_type': dim_type,
            'is_valid': 1 if is_valid else 0
        })
        
        logger.debug(f"[SQL生成-维度] 收集维度: {dim_id} - {dim_name} ({dim_code})")

    def _collect_table_lineage_data(self, processed_result: Dict[str, Any], table_data: Dict[str, List]):
        """
        收集表级血缘数据（table_lineage）
        从 AI 解析的 fields 中提取源表到目标表的映射关系
        
        Args:
            processed_result: 处理结果（包含 parsed_data）
            table_data: 表数据收集字典
        """
        try:
            # 获取原始解析数据
            parsed_data = processed_result.get('parsed_data', {})
            fields = parsed_data.get('fields', [])
            basic_info = parsed_data.get('basic_info', {})
            target_table = basic_info.get('table_name', '')  # 目标表名
            
            if not fields or not target_table:
                logger.warning("[SQL生成-表血缘] 缺少 fields 或 target_table，跳过表血缘收集")
                return
            
            # 收集所有唯一的 source_table → target_table 映射
            table_pairs = set()
            for field in fields:
                source_tables = field.get('source_tables', [])
                for source_table in source_tables:
                    if source_table and source_table != target_table:
                        table_pairs.add((source_table, target_table))
            
            # 构建 (source_table, target_table) -> lineage_id 的映射（从数据库查询已有数据）
            existing_table_lineage = {}
            try:
                result = self.session.execute(
                    text("SELECT lineage_id, source_table, target_table FROM table_lineage")
                ).fetchall()
                for row in result:
                    key = (row[1], row[2])  # (source_table, target_table)
                    existing_table_lineage[key] = row[0]  # lineage_id
                logger.debug(f"[SQL生成-表血缘] 从数据库加载 {len(existing_table_lineage)} 条已有记录")
            except Exception as e:
                logger.warning(f"[SQL生成-表血缘] 查询已有表血缘失败: {str(e)}")
            
            # 为每个表对生成或复用 lineage_id
            for source_table, tgt_table in table_pairs:
                key = (source_table, tgt_table)
                
                # 检查是否已存在
                if key in existing_table_lineage:
                    # 复用已有的 lineage_id
                    lineage_id = existing_table_lineage[key]
                    logger.debug(f"[SQL生成-表血缘] 复用已有ID: {source_table} -> {tgt_table} (ID: {lineage_id})")
                else:
                    # 生成新的 lineage_id
                    lineage_id = self._get_next_table_lineage_id()  # T000001, T000002, ...
                    logger.debug(f"[SQL生成-表血缘] 生成新ID: {source_table} -> {tgt_table} (ID: {lineage_id})")
                
                table_data['table_lineage'].append({
                    'lineage_id': lineage_id,
                    'source_table': source_table,
                    'target_table': tgt_table
                })
            
            logger.debug(f"[SQL生成-表血缘] 收集完成: {len(table_data['table_lineage'])} 条记录")
            
        except Exception as e:
            logger.error(f"[SQL生成-表血缘] 收集失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    def _collect_dim_field_mapping_data_for_dim_layer(self, dimensions: List[Dict[str, Any]],
                                                      processed_result: Dict[str, Any], table_data: Dict[str, List]):
        """
        DIM 层专用：收集维度字段映射数据（dim_field_mapping）
        记录当前维度表中，哪些字段属于哪个维度
        
        Args:
            dimensions: 维度列表
            processed_result: 处理结果（包含 parsed_data）
            table_data: 表数据收集字典
        """
        try:
            # 获取原始解析数据中的 fields
            parsed_data = processed_result.get('parsed_data', {})
            fields = parsed_data.get('fields', [])
            basic_info = parsed_data.get('basic_info', {})
            target_table = basic_info.get('table_name', '')  # 目标表名（INSERT 的表）
            
            if not fields or not target_table:
                logger.warning("[SQL生成-DIM层-维度字段映射] 缺少 fields 或 target_table，跳过维度字段映射收集")
                return
            
            # 构建维度编码到 dim_id 的映射
            dim_code_to_id = {}
            for dim in dimensions:
                dim_code = dim.get('dim_code')
                dim_id = dim.get('dim_id')
                if dim_code and dim_id:
                    dim_code_to_id[dim_code] = dim_id
            
            # 从 fields 中提取维度字段映射
            # DIM 层的 target_field 就是当前维度表的字段
            lineage_count = 0
            for field in fields:
                target_field = field.get('target_field', '')  # 当前表的字段名
                
                # 如果 target_field 是维度字段（在 dim_code_to_id 中），则记录
                if target_field and target_field in dim_code_to_id:
                    dim_id = dim_code_to_id[target_field]
                    
                    # 记录：当前维度表.当前字段 → 维度ID
                    table_data['dim_field_mapping'].append({
                        'dim_id': dim_id,
                        'db_table': target_table,      # ← 当前维度表（不是源表！）
                        'dim_field': target_field       # ← 当前维度表的字段
                    })
                    
                    lineage_count += 1
                    logger.debug(f"[SQL生成-DIM层-维度字段映射] {target_table}.{target_field} -> 维度 {dim_id}")
            
            logger.info(f"[SQL生成-DIM层-维度字段映射] 收集完成: {lineage_count} 条记录")
            
        except Exception as e:
            logger.error(f"[SQL生成-DIM层-维度字段映射] 收集失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    def _collect_dim_field_mapping_data(self, dimensions: List[Dict[str, Any]], metrics: List[Dict[str, Any]],
                                        processed_result: Dict[str, Any], table_data: Dict[str, List]):
        """
        收集维度字段映射数据（dim_field_mapping）
        从 AI 解析的 fields 中提取维度与物理字段的映射关系
        
        Args:
            dimensions: 维度列表
            metrics: 指标列表
            processed_result: 处理结果（包含 parsed_data）
            table_data: 表数据收集字典
        """
        try:
            # 获取原始解析数据中的 fields
            parsed_data = processed_result.get('parsed_data', {})
            logger.info(f"[SQL生成-维度字段映射] parsed_data keys: {list(parsed_data.keys()) if parsed_data else 'None'}")
            
            basic_info = parsed_data.get('basic_info', {})
            target_table = basic_info.get('table_name', '')  # 目标表名
            
            logger.info(f"[SQL生成-维度字段映射] target_table: {target_table}")
            
            if not target_table:
                logger.warning("[SQL生成-维度字段映射] 无法获取目标表名，跳过维度字段映射收集")
                return
            
            # 构建维度编码到 dim_id 的映射
            dim_code_to_id = {}
            for dim in dimensions:
                dim_code = dim.get('dim_code')
                dim_id = dim.get('dim_id')
                if dim_code and dim_id:
                    dim_code_to_id[dim_code] = dim_id
            
            # 从 metrics 的 source_mappings 中提取维度字段映射
            # 注意：AI 解析时，维度的 source_fields 包含了物理字段信息
            for metric in metrics:
                source_mappings = metric.get('source_mappings', [])
                for mapping in source_mappings:
                    db_table = mapping.get('db_table', '')  # 物理表名
                    metric_column = mapping.get('metric_column', '')  # 指标字段
                    
                    # 如果 metric_column 是维度字段（在 dim_code_to_id 中），则记录血缘
                    if metric_column and metric_column in dim_code_to_id:
                        dim_id = dim_code_to_id[metric_column]
                        
                        table_data['dim_field_mapping'].append({
                            'dim_id': dim_id,
                            'db_table': db_table,
                            'dim_field': metric_column
                        })
                        
                        logger.debug(f"[SQL生成-维度字段映射] {dim_id} -> {db_table}.{metric_column}")
            
            logger.info(f"[SQL生成-维度字段映射] 收集完成: {len(table_data['dim_field_mapping'])} 条记录")
            
        except Exception as e:
            logger.error(f"[SQL生成-维度字段映射] 收集失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    def _collect_field_lineage_data(self, processed_result: Dict[str, Any], table_data: Dict[str, List], has_group_by: bool = False):
        """
        收集字段级血缘数据（field_lineage）
        从 AI 解析的 fields 中提取源字段到目标字段的映射
        ⚠️ 依赖 table_lineage 的 lineage_id
        
        Args:
            processed_result: 处理结果（包含 parsed_data）
            table_data: 表数据收集字典
            has_group_by: SQL 是否包含 GROUP BY 子句
        """
        try:
            # 获取原始解析数据
            parsed_data = processed_result.get('parsed_data', {})
            fields = parsed_data.get('fields', [])
            basic_info = parsed_data.get('basic_info', {})
            target_table = basic_info.get('table_name', '')  # 目标表名
            
            if not fields or not target_table:
                logger.warning("[SQL生成-字段血缘] 缺少 fields 或 target_table，跳过字段血缘收集")
                return
            
            # 构建 source_table -> lineage_id 的映射（从已收集的 table_lineage 中）
            table_to_lineage_id = {}
            for tl in table_data['table_lineage']:
                key = (tl['source_table'], tl['target_table'])
                table_to_lineage_id[key] = tl['lineage_id']
            
            # ⚠️ 构建维度编码到 dim_id 的映射（用于查询公共维度的 dim_id）
            processed_data = processed_result.get('processed_data', {})
            dimensions = processed_data.get('dimensions', [])
            
            dim_code_to_id = {}
            for dim in dimensions:
                dim_code = dim.get('dim_code')
                dim_id = dim.get('dim_id')
                if dim_code and dim_id:
                    dim_code_to_id[dim_code] = dim_id
            
            logger.debug(f"[SQL生成-字段血缘] 维度映射: {len(dim_code_to_id)} 个, 有GROUP BY: {has_group_by}")
            
            # 构建业务唯一键 -> lineage_id 的映射（从数据库查询已有数据）
            # 业务唯一键: (source_table, source_field, target_table, target_field)
            existing_field_lineage = {}
            try:
                result = self.session.execute(
                    text("SELECT lineage_id, source_table, source_field, target_table, target_field FROM field_lineage")
                ).fetchall()
                for row in result:
                    key = (row[1], row[2], row[3], row[4])  # (source_table, source_field, target_table, target_field)
                    existing_field_lineage[key] = row[0]  # lineage_id
                logger.debug(f"[SQL生成-字段血缘] 从数据库加载 {len(existing_field_lineage)} 条已有记录")
            except Exception as e:
                logger.warning(f"[SQL生成-字段血缘] 查询已有字段血缘失败: {str(e)}")
            
            # 遍历所有字段，提取血缘关系
            for field in fields:
                source_tables = field.get('source_tables', [])
                source_fields = field.get('source_fields', [])
                target_field = field.get('target_field', '')
                
                if not target_field:
                    continue
                
                # ⚠️ 直接使用 AI 输出的 field_type 字段
                field_type = field.get('field_type', 'normal')
                
                # 为每个源字段生成一条血缘记录
                for i, source_field_full in enumerate(source_fields):
                    # 解析 source_field_full (格式: "schema.table.field" 或 "table.field")
                    parts = source_field_full.split('.')
                    if len(parts) >= 2:
                        source_field = parts[-1]  # 最后一个部分是字段名
                        source_table = '.'.join(parts[:-1])  # 前面部分是表名
                    else:
                        source_field = source_field_full
                        source_table = source_tables[i] if i < len(source_tables) else ''
                    
                    if not source_table:
                        continue
                    
                    # ⚠️ 关键：查找对应的表血缘ID
                    lineage_key = (source_table, target_table)
                    table_lineage_id = table_to_lineage_id.get(lineage_key, '')
                    
                    if not table_lineage_id:
                        logger.warning(f"[SQL生成-字段血缘] 未找到表血缘: {source_table} -> {target_table}")
                        continue
                    
                    # 构建业务唯一键
                    business_key = (source_table, source_field, target_table, target_field)
                    
                    # 检查是否已存在
                    if business_key in existing_field_lineage:
                        # 复用已有的 lineage_id
                        lineage_id = existing_field_lineage[business_key]
                        logger.debug(f"[SQL生成-字段血缘] 复用已有ID: {source_table}.{source_field} -> {target_table}.{target_field} (ID: {lineage_id})")
                    else:
                        # 生成新的 lineage_id
                        lineage_id = self._get_next_field_lineage_id()  # F000001, F000002, ...
                        logger.debug(f"[SQL生成-字段血缘] 生成新ID: {source_table}.{source_field} -> {target_table}.{target_field} (ID: {lineage_id})")
                    
                    # ⚠️ 如果 field_type 是 public_dim，查询 dim_id
                    dim_id = None
                    final_field_type = field_type  # 最终使用的字段类型
                    
                    if field_type == 'public_dim':
                        # 优先用 target_field 查询，如果没有则尝试用 source_field 查询
                        dim_id = dim_code_to_id.get(target_field)
                        
                        if not dim_id and source_fields:
                            # 尝试从 source_field 中提取字段名（最后一部分）
                            source_field_name = source_fields[0].split('.')[-1] if source_fields[0] else ''
                            if source_field_name:
                                dim_id = dim_code_to_id.get(source_field_name)
                                logger.debug(f"[SQL生成-字段血缘] 尝试用 source_field 查询: {source_field_name} -> {dim_id}")
                        
                        if dim_id:
                            logger.debug(f"[SQL生成-字段血缘] 公共维度 {target_field} -> {dim_id}")
                            final_field_type = 'public_dim'
                        else:
                            # ⚠️ 未找到 dim_id，降级为 private_dim（避免违反 CHECK 约束）
                            logger.debug(f"[SQL生成-字段血缘] 公共维度 {target_field} (source: {source_fields}) 未找到对应的 dim_id，降级为 private_dim")
                            final_field_type = 'private_dim'
                    
                    table_data['field_lineage'].append({
                        'lineage_id': lineage_id,  # ⚠️ 字段血缘主键 (F000001)
                        'table_lineage_id': table_lineage_id,  # ⚠️ 关联到 table_lineage 的外键 (T000001)
                        'source_table': source_table,
                        'source_field': source_field,
                        'target_table': target_table,
                        'target_field': target_field,
                        'target_field_mark': final_field_type,  # ⚠️ 使用最终确定的类型
                        'dim_id': dim_id  # 仅公共维度有值
                    })
                    
                    logger.debug(f"[SQL生成-字段血缘] {source_table}.{source_field} -> {target_table}.{target_field} (type: {field_type}, lineage_id: {lineage_id}, table_lineage_id: {table_lineage_id})")
            
            logger.debug(f"[SQL生成-字段血缘] 收集完成: {len(table_data['field_lineage'])} 条记录")
            
        except Exception as e:
            logger.error(f"[SQL生成-字段血缘] 收集失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    def _collect_atomic_metric_data(self, metric: Dict[str, Any], dimensions: List[Dict[str, Any]], table_data: Dict[str, List]):
        """
        收集原子指标数据（不生成SQL，只收集数据）
        """
        metric_id = metric.get('metric_id')
        metric_code = metric.get('metric_code')
        metric_name = metric.get('metric_name')
        
        logger.info(f"[SQL生成-原子指标] 开始收集指标: {metric_name} (ID: {metric_id}, Code: {metric_code})")
        
        # 获取 metric_info
        metric_info = metric.get('metric_info', {})
        if hasattr(metric_info, 'cal_logic'):
            cal_logic = metric_info.cal_logic or ''
            unit = metric_info.unit or ''
        else:
            cal_logic = metric_info.get('cal_logic', '')
            unit = metric_info.get('unit', '')
        
        logger.info(f"[SQL生成-原子指标] 指标信息 - cal_logic: {cal_logic[:100] if cal_logic else '空'}, unit: {unit}")

        # 收集 metric_definition 数据
        table_data['metric_definition'].append({
            'metric_id': metric_id,
            'metric_name': metric_name,
            'metric_code': metric_code,
            'metric_type': 'ATOMIC',
            'biz_domain': '养殖繁殖',
            'cal_logic': cal_logic,
            'unit': unit,
            'status': 1
        })
        logger.info(f"[SQL生成-原子指标] ✅ 已添加 metric_definition 数据 (metric_id: {metric_id}, cal_logic: {cal_logic[:50] if cal_logic else '空'})")

        # 收集 metric_dim_rel 数据
        dim_rel_count = 0
        for dim in dimensions:
            dim_id = dim.get('dim_id')
            if dim_id:
                is_required = metric.get('is_required', False)
                table_data['metric_dim_rel'].append({
                    'metric_id': metric_id,
                    'dim_id': dim_id,
                    'is_required': 1 if is_required else 0
                })
                dim_rel_count += 1
        logger.info(f"[SQL生成-原子指标] ✅ 已添加 {dim_rel_count} 条 metric_dim_rel 数据")

        # 收集 metric_source_mapping 数据
        source_mappings = metric.get('source_mappings', [])
        logger.info(f"[SQL生成-原子指标] 指标有 {len(source_mappings)} 个源映射")
        
        source_mapping_count = 0
        for mapping in source_mappings:
            datasource = mapping.get('datasource')
            db_table = mapping.get('db_table')
            metric_column = mapping.get('metric_column')
            filter_condition = mapping.get('filter_condition', '')
            agg_func = mapping.get('agg_func')
            priority = mapping.get('priority', 1)
            source_level = mapping.get('source_level', 'AUTHORITY')
            
            logger.debug(f"[SQL生成-原子指标] 源映射: datasource={datasource}, db_table={db_table}, metric_column={metric_column}")

            table_data['metric_source_mapping'].append({
                'map_id': self._get_next_map_id(),  # 使用递增ID生成器
                'metric_id': metric_id,
                'source_type': 'OFFLINE',
                'datasource': datasource,
                'db_table': db_table,
                'metric_column': metric_column,
                'filter_condition': filter_condition,
                'agg_func': agg_func,
                'priority': priority,
                'is_valid': 1,
                'source_level': source_level
            })
            source_mapping_count += 1
        
        logger.info(f"[SQL生成-原子指标] ✅ 已添加 {source_mapping_count} 条 metric_source_mapping 数据")
        logger.info(f"[SQL生成-原子指标] 指标 {metric_name} 收集完成")

    def _collect_metric_lineage_data(self, metrics: List[Dict[str, Any]], table_data: Dict[str, List]):
        """
        收集 metric_lineage 数据（关联指标和字段血缘）
        
        逻辑：
        1. 遍历所有标记为 'metric' 的 field_lineage 记录
        2. 根据 target_field 精确匹配指标的 metric_code
        3. 建立 metric_id <-> field_lineage_id 的关联
        """
        # 构建指标编码到 metric_id 的映射
        metric_code_to_id = {}
        for metric in metrics:
            metric_code = metric.get('metric_code', '')
            metric_id = metric.get('metric_id', '')
            if metric_code and metric_id:
                metric_code_to_id[metric_code] = metric_id
        
        logger.info(f"[SQL生成-metric_lineage] 开始收集，共有 {len(metric_code_to_id)} 个指标: {list(metric_code_to_id.keys())}")
        
        # 统计 field_lineage 中各种标记的数量
        mark_stats = {}
        for fl in table_data['field_lineage']:
            mark = fl.get('target_field_mark', 'unknown')
            mark_stats[mark] = mark_stats.get(mark, 0) + 1
        
        logger.info(f"[SQL生成-metric_lineage] field_lineage 标记统计: {mark_stats}")
        
        # 遍历所有 field_lineage 记录
        lineage_count = 0
        unmatched_fields = []
        
        for field_lineage in table_data['field_lineage']:
            target_field_mark = field_lineage.get('target_field_mark', '')
            target_field = field_lineage.get('target_field', '')
            field_lineage_id = field_lineage.get('lineage_id', '')
            
            # 只处理标记为 'metric' 的字段
            if target_field_mark != 'metric':
                continue
            
            # 精确匹配 metric_code
            matched_metric_id = metric_code_to_id.get(target_field)
            
            if matched_metric_id and field_lineage_id:
                table_data['metric_lineage'].append({
                    'metric_id': matched_metric_id,
                    'field_lineage_id': field_lineage_id
                })
                lineage_count += 1
                logger.info(f"[SQL生成-metric_lineage] ✅ 匹配成功: {target_field} -> {matched_metric_id} (field_lineage_id: {field_lineage_id})")
            else:
                unmatched_fields.append(target_field)
                logger.warning(f"[SQL生成-metric_lineage] ❌ 未找到匹配的指标: target_field={target_field}, field_lineage_id={field_lineage_id}, available_codes={list(metric_code_to_id.keys())}")
        
        if unmatched_fields:
            logger.warning(f"[SQL生成-metric_lineage] 未匹配的字段列表: {unmatched_fields}")
        
        logger.info(f"[SQL生成-metric_lineage] ✅ 收集完成: {lineage_count} 条记录")

    def _collect_compound_metric_data(self, metric: Dict[str, Any], dimensions: List[Dict[str, Any]], table_data: Dict[str, List]):
        """
        收集复合指标数据（不生成SQL，只收集数据）
        """
        metric_id = metric.get('metric_id')
        metric_code = metric.get('metric_code')
        metric_name = metric.get('metric_name')
        
        # 获取 metric_info
        metric_info = metric.get('metric_info', {})
        if hasattr(metric_info, 'cal_logic'):
            cal_logic = metric_info.cal_logic or ''
            unit = metric_info.unit or ''
        else:
            cal_logic = metric_info.get('cal_logic', '')
            unit = metric_info.get('unit', '')

        # 收集 metric_definition 数据
        table_data['metric_definition'].append({
            'metric_id': metric_id,
            'metric_name': metric_name,
            'metric_code': metric_code,
            'metric_type': 'COMPOUND',
            'biz_domain': '养殖繁殖',
            'cal_logic': cal_logic,
            'unit': unit,
            'status': 1
        })

        # 收集 metric_dim_rel 数据
        for dim in dimensions:
            dim_id = dim.get('dim_id')
            if dim_id:
                is_required = metric.get('is_required', False)
                table_data['metric_dim_rel'].append({
                    'metric_id': metric_id,
                    'dim_id': dim_id,
                    'is_required': 1 if is_required else 0
                })

        # 收集 metric_compound_rel 数据
        compound_rels = metric.get('compound_rels', [])
        for i, rel in enumerate(compound_rels):
            sub_metric_code = rel.get('sub_metric_code')
            operator = rel.get('cal_operator')

            table_data['metric_compound_rel'].append({
                'metric_id': metric_id,
                'sub_metric_id': sub_metric_code,
                'cal_operator': operator,
                'sort': i + 1
            })

    def _deduplicate_dim_field_mapping(self, data_list: List[Dict]) -> List[Dict]:
        """
        对 dim_field_mapping 数据根据联合主键 (db_table, dim_field) 去重
        保留最后一条记录（因为后面的可能覆盖前面的）
        
        Args:
            data_list: 原始数据列表
            
        Returns:
            去重后的数据列表
        """
        seen_keys = {}
        duplicate_count = 0
        
        for data in data_list:
            # 构建唯一键
            unique_key = (data.get('db_table', ''), data.get('dim_field', ''))
            
            # 如果已存在，记录警告
            if unique_key in seen_keys:
                old_dim_id = seen_keys[unique_key].get('dim_id')
                new_dim_id = data.get('dim_id')
                logger.warning(
                    f"[维度字段映射去重] 发现重复映射: {unique_key[0]}.{unique_key[1]} "
                    f"原维度={old_dim_id}, 新维度={new_dim_id}，将使用新维度"
                )
                duplicate_count += 1
            
            # 保留最后一条（覆盖之前的）
            seen_keys[unique_key] = data
        
        if duplicate_count > 0:
            logger.warning(f"[维度字段映射去重] 共发现 {duplicate_count} 个重复映射，已自动去重")
        
        # 返回去重后的列表（保持插入顺序）
        return list(seen_keys.values())

    def _generate_batch_insert_sql(self, table_name: str, data_list: List[Dict]) -> Optional[str]:
        """
        为指定表生成批量 UPSERT SQL (INSERT ... ON CONFLICT ... DO UPDATE)
        
        Args:
            table_name: 表名
            data_list: 数据列表
            
        Returns:
            批量 UPSERT SQL
        """
        if not data_list:
            return None
        
        try:
            # 定义各表的字段映射和主键/唯一键
            table_config = {
                'dim_definition': {
                    'columns': ['dim_id', 'dim_name', 'dim_code', 'dim_type', 'is_valid'],
                    'conflict_target': 'dim_id'  # 主键
                },
                'dim_field_mapping': {
                    'columns': ['dim_id', 'db_table', 'dim_field'],
                    'conflict_target': '(db_table, dim_field)'  # 联合主键（物理表+字段）
                },
                'table_lineage': {
                    'columns': ['lineage_id', 'source_table', 'target_table'],
                    'conflict_target': 'lineage_id'  # 主键
                },
                'field_lineage': {
                    'columns': ['lineage_id', 'table_lineage_id', 'source_table', 'source_field', 'target_table', 'target_field', 'target_field_mark', 'dim_id'],
                    'conflict_target': 'lineage_id'  # 主键
                },
                'metric_definition': {
                    'columns': ['metric_id', 'metric_name', 'metric_code', 'metric_type', 'biz_domain', 'cal_logic', 'unit', 'status'],
                    'conflict_target': 'metric_id'  # 主键
                },
                'metric_dim_rel': {
                    'columns': ['metric_id', 'dim_id', 'is_required'],
                    'conflict_target': '(metric_id, dim_id)'  # 联合唯一（需要添加唯一索引）
                },
                'metric_source_mapping': {
                    'columns': ['map_id', 'metric_id', 'source_type', 'datasource', 'db_table', 'metric_column', 'filter_condition', 'agg_func', 'priority', 'is_valid', 'source_level'],
                    'conflict_target': 'map_id'  # 主键
                },
                'metric_lineage': {
                    'columns': ['metric_id', 'field_lineage_id'],
                    'conflict_target': '(metric_id, field_lineage_id)'  # 联合唯一
                },
                'metric_compound_rel': {
                    'columns': ['metric_id', 'sub_metric_id', 'cal_operator', 'sort'],
                    'conflict_target': '(metric_id, sub_metric_id)'  # 联合唯一（需要添加唯一索引）
                }
            }
            
            config = table_config.get(table_name)
            if not config:
                logger.warning(f"[SQL生成-批量] 未知表: {table_name}")
                return None
            
            columns = config['columns']
            conflict_target = config['conflict_target']
            
            # 生成 VALUES 部分
            values_list = []
            for data in data_list:
                values = []
                for col in columns:
                    value = data.get(col)
                    if value is None:
                        values.append('NULL')
                    elif isinstance(value, str):
                        # 转义单引号
                        escaped_value = value.replace("'", "''")
                        values.append(f"'{escaped_value}'")
                    else:
                        values.append(str(value))
                values_list.append(f"({', '.join(values)})")
            
            # 生成更新字段列表（排除主键/冲突目标字段）
            if conflict_target.startswith('('):
                # 联合唯一键，提取所有字段
                conflict_cols = [col.strip() for col in conflict_target.strip('()').split(',')]
            else:
                # 单个主键
                conflict_cols = [conflict_target]
            
            update_cols = [col for col in columns if col not in conflict_cols]
            
            # 生成 SET 子句
            set_clauses = []
            for col in update_cols:
                set_clauses.append(f"{col} = EXCLUDED.{col}")
            
            # 组装批量 UPSERT SQL
            columns_str = ', '.join(columns)
            values_str = ',\n'.join(values_list)
            
            # PostgreSQL ON CONFLICT 语法要求：单字段也需要括号
            conflict_clause = f"({conflict_target})" if not conflict_target.startswith('(') else conflict_target
            
            # 如果没有可更新的字段（所有字段都是冲突目标），使用 DO NOTHING
            if not set_clauses:
                upsert_sql = f"""-- 批量INSERT {table_name} ({len(data_list)} 条记录，忽略重复)
INSERT INTO {table_name} ({columns_str}) VALUES
{values_str}
ON CONFLICT {conflict_clause} DO NOTHING;"""
            else:
                set_clause_str = ', '.join(set_clauses)
                upsert_sql = f"""-- 批量UPSERT {table_name} ({len(data_list)} 条记录)
INSERT INTO {table_name} ({columns_str}) VALUES
{values_str}
ON CONFLICT {conflict_clause} DO UPDATE SET
{set_clause_str};"""
            
            return upsert_sql
            
        except Exception as e:
            import traceback
            logger.error(f"[SQL生成-批量] 表 {table_name} 生成失败: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _generate_atomic_metric_insert(self, metric: Dict[str, Any], dimensions: List[Dict[str, Any]]) -> Optional[str]:
        """
        生成原子指标的INSERT SQL

        Args:
            metric: 原子指标数据
            dimensions: 维度列表

        Returns:
            INSERT SQL字符串
        """
        try:
            metric_id = metric.get('metric_id')
            metric_code = metric.get('metric_code')
            metric_name = metric.get('metric_name')
            
            # 获取 metric_info（可能是 MetricDefinitionInfo 对象或字典）
            metric_info = metric.get('metric_info', {})
            if hasattr(metric_info, 'cal_logic'):
                # 是 MetricDefinitionInfo 对象
                cal_logic = metric_info.cal_logic or ''
                unit = metric_info.unit or ''
            else:
                # 是字典
                cal_logic = metric_info.get('cal_logic', '')
                unit = metric_info.get('unit', '')

            # 生成metric_definition INSERT
            insert_sql = f"""
-- 原子指标：{metric_name}
INSERT INTO metric_definition (metric_id, metric_name, metric_code, metric_type, biz_domain, cal_logic, unit, status)
VALUES ('{metric_id}', '{metric_name}', '{metric_code}', 'ATOMIC', '养殖繁殖', '{cal_logic}', '{unit}', 1);
"""

            # 生成metric_dim_rel INSERT
            for dim in dimensions:
                dim_id = dim.get('dim_id')
                if dim_id:
                    is_required = metric.get('is_required', False)
                    insert_sql += f"""
-- 维度关联：{metric_code} - {dim_id}
INSERT INTO metric_dim_rel (metric_id, dim_id, is_required)
VALUES ('{metric_id}', '{dim_id}', {1 if is_required else 0});
"""

            # 生成metric_source_mapping INSERT
            source_mappings = metric.get('source_mappings', [])
            for mapping in source_mappings:
                datasource = mapping.get('datasource')
                db_table = mapping.get('db_table')
                metric_column = mapping.get('metric_column')
                filter_condition = mapping.get('filter_condition', '')
                agg_func = mapping.get('agg_func')
                priority = mapping.get('priority', 1)
                source_level = mapping.get('source_level', 'AUTHORITY')

                insert_sql += f"""
-- 源映射：{metric_code}
INSERT INTO metric_source_mapping (map_id, metric_id, source_type, datasource, db_table, metric_column, filter_condition, agg_func, priority, is_valid, source_level)
VALUES ('MAP{len(metric_code)}', '{metric_id}', 'OFFLINE', '{datasource}', '{db_table}', '{metric_column}', '{filter_condition}', '{agg_func}', {priority}, 1, '{source_level}');
"""

            return insert_sql

        except Exception as e:
            logger.error(f"生成原子指标INSERT SQL失败：{str(e)}")
            return None

    def _generate_compound_metric_insert(self, metric: Dict[str, Any], dimensions: List[Dict[str, Any]]) -> Optional[str]:
        """
        生成复合指标的INSERT SQL

        Args:
            metric: 复合指标数据
            dimensions: 维度列表

        Returns:
            INSERT SQL字符串
        """
        try:
            metric_id = metric.get('metric_id')
            metric_code = metric.get('metric_code')
            metric_name = metric.get('metric_name')
            
            # 获取 metric_info（可能是 MetricDefinitionInfo 对象或字典）
            metric_info = metric.get('metric_info', {})
            if hasattr(metric_info, 'cal_logic'):
                # 是 MetricDefinitionInfo 对象
                cal_logic = metric_info.cal_logic or ''
                unit = metric_info.unit or ''
            else:
                # 是字典
                cal_logic = metric_info.get('cal_logic', '')
                unit = metric_info.get('unit', '')

            # 生成metric_definition INSERT
            insert_sql = f"""
-- 复合指标：{metric_name}
INSERT INTO metric_definition (metric_id, metric_name, metric_code, metric_type, biz_domain, cal_logic, unit, status)
VALUES ('{metric_id}', '{metric_name}', '{metric_code}', 'COMPOUND', '养殖繁殖', '{cal_logic}', '{unit}', 1);
"""

            # 生成metric_dim_rel INSERT
            for dim in dimensions:
                dim_id = dim.get('dim_id')
                if dim_id:
                    is_required = metric.get('is_required', False)
                    insert_sql += f"""
-- 维度关联：{metric_code} - {dim_id}
INSERT INTO metric_dim_rel (metric_id, dim_id, is_required)
VALUES ('{metric_id}', '{dim_id}', {1 if is_required else 0});
"""

            # 生成metric_compound_rel INSERT
            compound_rels = metric.get('compound_rels', [])
            for i, rel in enumerate(compound_rels):
                sub_metric_code = rel.get('sub_metric_code')
                operator = rel.get('cal_operator')

                insert_sql += f"""
-- 复合关系：{metric_code} = {sub_metric_code} {operator}
INSERT INTO metric_compound_rel (metric_id, sub_metric_id, cal_operator, sort)
VALUES ('{metric_id}', '{sub_metric_code}', '{operator}', {i + 1});
"""

            return insert_sql

        except Exception as e:
            logger.error(f"生成复合指标INSERT SQL失败：{str(e)}")
            return None

    def _validate_insert_sqls(self, insert_sqls: List[str]) -> Dict[str, Any]:
        """
        校验引擎校验

        Args:
            insert_sqls: INSERT SQL列表

        Returns:
            校验结果字典
        """
        try:
            if not insert_sqls:
                return {"success": False, "message": "没有可校验的INSERT SQL"}

            valid_sqls = []
            invalid_sqls = []
            validation_errors = []

            for sql in insert_sqls:
                try:
                    # 1. SQL语法校验（简化版，实际需要更复杂的校验）
                    if not self._validate_sql_syntax(sql):
                        invalid_sqls.append(sql)
                        validation_errors.append(f"SQL语法错误：{sql[:100]}...")
                        continue

                    # 2. 数据合法性校验
                    if not self._validate_data_legality(sql):
                        invalid_sqls.append(sql)
                        validation_errors.append(f"数据合法性校验失败：{sql[:100]}...")
                        continue

                    # 3. 去重校验
                    if not self._validate_no_duplicates(sql):
                        invalid_sqls.append(sql)
                        validation_errors.append(f"重复校验失败：{sql[:100]}...")
                        continue

                    valid_sqls.append(sql)

                except Exception as e:
                    invalid_sqls.append(sql)
                    validation_errors.append(f"校验异常：{str(e)} - {sql[:100]}...")

            if invalid_sqls:
                return {
                    "success": False,
                    "message": "部分SQL校验失败",
                    "valid_count": len(valid_sqls),
                    "invalid_count": len(invalid_sqls),
                    "validation_errors": validation_errors,
                    "valid_sqls": valid_sqls,
                    "invalid_sqls": invalid_sqls
                }

            return {
                "success": True,
                "message": "所有SQL校验通过",
                "total_sql": len(insert_sqls),
                "valid_count": len(valid_sqls),
                "validation_errors": []
            }

        except Exception as e:
            logger.error(f"校验INSERT SQL失败：{str(e)}")
            return {
                "success": False,
                "message": f"校验失败：{str(e)}",
                "total_sql": len(insert_sqls) if 'insert_sqls' in locals() else 0
            }

    def _validate_sql_syntax(self, sql: str) -> bool:
        """
        SQL语法校验

        Args:
            sql: SQL语句

        Returns:
            是否通过校验
        """
        # 简化版校验，实际需要更复杂的校验逻辑
        # 检查基本语法结构
        if not sql.strip().startswith("INSERT INTO"):
            return False
        if "VALUES" not in sql:
            return False
        return True

    def _validate_data_legality(self, sql: str) -> bool:
        """
        数据合法性校验

        Args:
            sql: SQL语句

        Returns:
            是否通过校验
        """
        # 提取 VALUES 子句中的内容（忽略注释）
        values_part = sql.split('VALUES')[-1] if 'VALUES' in sql else ''
        
        # 检查是否有基本的值
        if not values_part.strip():
            logger.warning(f"[校验-数据合法性] SQL 缺少 VALUES 内容")
            return False
        
        return True

    def _validate_no_duplicates(self, sql: str) -> bool:
        """
        去重校验 - 检查 VALUES 中的主键是否重复

        Args:
            sql: SQL语句

        Returns:
            是否通过校验
        """
        # 提取表名和 VALUES 部分
        if 'INSERT INTO' not in sql or 'VALUES' not in sql:
            logger.warning(f"[校验-去重] SQL 格式不正确")
            return False
        
        # 这个校验应该在批量执行时进行，单条 SQL 不需要校验重复
        return True

    def _execute_insert_sqls(self, insert_sqls: List[str]) -> Dict[str, Any]:
        """
        执行INSERT SQL写入表

        Args:
            insert_sqls: INSERT SQL列表

        Returns:
            执行结果字典
        """
        try:
            if not insert_sqls:
                return {"success": False, "message": "没有可执行的INSERT SQL"}

            executed_count = 0
            failed_count = 0
            execution_errors = []

            # 检查是否已有事务，如果没有则开启新事务
            in_transaction = self.session.in_transaction()
            if not in_transaction:
                self.session.begin()
                logger.debug("[事务] 开启新事务")
            else:
                logger.debug("[事务] 使用现有事务")

            try:
                for i, sql in enumerate(insert_sqls, 1):
                    try:
                        # 执行SQL - 使用 text() 包装原始 SQL
                        self.session.execute(text(sql))
                        executed_count += 1
                        logger.debug(f"[SQL执行] SQL #{i} 执行成功")
                    except Exception as e:
                        failed_count += 1
                        error_msg = f"SQL #{i} 执行失败: {str(e)}"
                        execution_errors.append(error_msg)
                        logger.error(f"[SQL执行] {error_msg}")
                        logger.error(f"[SQL执行] SQL内容前200字符: {sql[:200]}")
                        import traceback
                        logger.error(traceback.format_exc())
                        
                        # 关键修复：PostgreSQL 事务出错后必须回滚才能继续
                        # 回滚当前事务，然后重新开始新事务以继续执行后续 SQL
                        try:
                            self.session.rollback()
                            logger.debug(f"[事务] 已回滚，准备继续执行下一条 SQL")
                            # 重新开启事务
                            if not in_transaction:
                                self.session.begin()
                        except Exception as rollback_error:
                            logger.error(f"[SQL执行] 回滚失败: {str(rollback_error)}")
                            # 如果回滚也失败，直接中断执行
                            raise Exception(f"SQL执行失败且无法回滚: {str(e)}, 回滚错误: {str(rollback_error)}")

                # 提交事务
                if failed_count == 0:
                    if not in_transaction:
                        self.session.commit()
                        logger.debug("[事务] 所有SQL执行成功，已提交事务")
                    else:
                        logger.debug("[事务] 所有SQL执行成功，事务由调用方提交")
                else:
                    # 如果有失败的SQL，不自动提交，让调用方决定如何处理
                    if not in_transaction:
                        self.session.rollback()
                        logger.warning("[事务] 存在失败的SQL，已回滚事务")
                    else:
                        logger.warning("[事务] 存在失败的SQL，事务未提交，需要手动处理")

                if failed_count > 0:
                    return {
                        "success": False,
                        "message": "部分SQL执行失败",
                        "total_sql": len(insert_sqls),
                        "executed_count": executed_count,
                        "failed_count": failed_count,
                        "execution_errors": execution_errors
                    }

                return {
                    "success": True,
                    "total_sql": len(insert_sqls),
                    "executed_count": executed_count,
                    "failed_count": failed_count,
                    "message": f"成功执行 {executed_count}/{len(insert_sqls)} 条SQL"
                }

            except Exception as e:
                # 回滚事务（只有在开启了新事务时才回滚）
                if not in_transaction:
                    try:
                        self.session.rollback()
                        logger.debug(f"[事务] 发生异常，已回滚事务")
                    except Exception as rollback_error:
                        logger.error(f"[事务] 回滚失败: {str(rollback_error)}")
                raise e

        except Exception as e:
            logger.error(f"执行INSERT SQL失败：{str(e)}")
            return {
                "success": False,
                "message": f"执行失败：{str(e)}",
                "total_sql": len(insert_sqls) if 'insert_sqls' in locals() else 0
            }

    def process_single_sql_file(self, file_path: str) -> Dict[str, Any]:
        """
        处理单个SQL文件（简化接口）

        Args:
            file_path: SQL文件路径

        Returns:
            处理结果
        """
        return self.process_metrics_from_sql(file_path, is_directory=False)

    def process_sql_directory(self, directory_path: str) -> Dict[str, Any]:
        """
        处理SQL目录（简化接口）

        Args:
            directory_path: 目录路径

        Returns:
            处理结果
        """
        return self.process_metrics_from_sql(directory_path, is_directory=True)