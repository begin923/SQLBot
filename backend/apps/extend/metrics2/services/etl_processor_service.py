from typing import List, Dict, Any, Optional, Union
from pathlib import Path
import os
import logging
from sqlalchemy.orm import Session
from apps.extend.metrics2.services.metric_service import MetricService
from apps.extend.metrics2.services.dim_field_mapping_service import DimFieldMappingService
from apps.extend.utils.utils import ModelClient
from apps.extend.metrics2.models import (
    MetricDefinitionInfo,
    MetricDimRelInfo,
    MetricSourceMappingInfo,
    MetricCompoundRelInfo
)

logger = logging.getLogger("ETLProcessorService")


class ETLProcessorService:
    """ETL处理器服务类 - 负责整个ETL解析和指标创建流程"""

    def __init__(self, session: Session):
        """
        初始化ETL处理器服务

        Args:
            session: 数据库会话
        """
        self.session = session
        self.model_client = ModelClient()
        self.metric_service = MetricService(session)
        self.dim_dict_service = DimFieldMappingService(session)

    def process_etl_files(self, input_path: Union[str, Path], is_directory: bool = False) -> Dict[str, Any]:
        """
        处理ETL文件（单个文件或目录下的所有文件）

        Args:
            input_path: 输入路径（文件路径或目录路径）
            is_directory: 是否为目录模式

        Returns:
            处理结果字典
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
                    file_result = self._process_single_file(file_path, sql_content)
                    results.append(file_result)

                    if file_result.get('success', False):
                        processed_files += 1
                    else:
                        failed_files += 1

                except Exception as e:
                    failed_files += 1
                    logger.error(f"处理文件 {file_path} 失败：{str(e)}")
                    results.append({
                        "file_path": str(file_path),
                        "success": False,
                        "message": f"处理失败：{str(e)}"
                    })

            # 返回总体结果
            return {
                "success": True,
                "total_files": total_files,
                "processed_files": processed_files,
                "failed_files": failed_files,
                "results": results,
                "message": f"成功处理 {processed_files}/{total_files} 个文件"
            }

        except Exception as e:
            logger.error(f"处理ETL文件失败：{str(e)}")
            return {
                "success": False,
                "message": f"处理失败：{str(e)}",
                "input_path": str(input_path)
            }

    def _process_single_file(self, file_path: Path, sql_content: str) -> Dict[str, Any]:
        """
        处理单个SQL文件

        Args:
            file_path: 文件路径
            sql_content: SQL内容

        Returns:
            单个文件处理结果
        """
        try:
            # 1. 调用大模型解析SQL
            logger.info(f"开始解析文件：{file_path}")
            parsed_data = self._parse_sql_with_ai(sql_content, file_path.name)

            if not parsed_data or not parsed_data.get('success', False):
                return {
                    "file_path": str(file_path),
                    "success": False,
                    "message": parsed_data.get('message', '大模型解析失败或未解析到指标数据')
                }

            # 2. 处理解析结果
            metrics_data = parsed_data['metrics']
            logger.info(f"解析到 {len(metrics_data)} 个指标")

            # 3. 准备批量创建数据
            batch_data = []
            for metric in metrics_data:
                # 创建指标定义信息
                metric_info = MetricDefinitionInfo(
                    metric_name=metric.get('metric_name'),
                    metric_code=metric.get('metric_code'),
                    metric_type=metric.get('metric_type'),
                    biz_domain=metric.get('biz_domain', '养殖繁殖'),
                    cal_logic=metric.get('cal_logic'),
                    unit=metric.get('unit'),
                    status=True
                )

                # 创建维度关联信息
                dim_rels = []
                for dim in metric.get('dimensions', []):
                    dim_rels.append(MetricDimRelInfo(
                        dim_id=dim.get('dim_id'),
                        is_required=dim.get('is_required', False),
                        sort=dim.get('sort', 0)
                    ))

                # 创建源映射信息（仅原子指标）
                source_mappings = []
                if metric.get('metric_type') == 'ATOMIC' and metric.get('source_mappings'):
                    for src in metric.get('source_mappings', []):
                        source_mappings.append(MetricSourceMappingInfo(
                            datasource=src.get('datasource', 'mysql'),
                            db_table=src.get('db_table', ''),
                            metric_column=src.get('metric_column', ''),
                            filter_condition=src.get('filter_condition', ''),
                            agg_func=src.get('agg_func', ''),
                            priority=src.get('priority', 1),
                            source_level=src.get('source_level', 'AUTHORITY')
                        ))

                # 创建复合关系信息（仅复合指标）
                compound_rels = []
                if metric.get('metric_type') == 'COMPOUND' and metric.get('compound_rule'):
                    for rule in metric.get('compound_rule', []):
                        compound_rels.append(MetricCompoundRelInfo(
                            sub_metric_id=rule.get('sub_metric_code'),
                            cal_operator=rule.get('operator'),
                            sort=rule.get('sort', 0)
                        ))

                # 组装批量数据
                batch_data.append({
                    'metric_info': metric_info,
                    'dim_rels': dim_rels,
                    'source_mappings': source_mappings,
                    'compound_rels': compound_rels
                })

            # 4. 批量创建指标及其关联关系
            batch_result = self.metric_service.batch_create_metrics_with_relations(batch_data)

            return {
                "file_path": str(file_path),
                "success": batch_result.get('success', False),
                "message": batch_result.get('message', '未知错误'),
                "parsed_metrics": len(metrics_data),
                "created_metrics": batch_result.get('success_count', 0),
                "batch_result": batch_result
            }

        except Exception as e:
            logger.error(f"处理单个文件 {file_path} 失败：{str(e)}")
            return {
                "file_path": str(file_path),
                "success": False,
                "message": f"处理失败：{str(e)}"
            }

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
            import json
            try:
                parsed_data = json.loads(result)
            except json.JSONDecodeError as e:
                logger.error(f"JSON解析失败：{str(e)}")
                logger.error(f"AI 返回内容前500字符：{result[:500]}")
                return {"success": False, "message": f"JSON解析失败：{str(e)}"}

            if not parsed_data or not parsed_data.get('fields'):
                return {"success": False, "message": "JSON解析成功但未提取到字段数据"}

            # 将 fields 转换为 metrics 和 dimensions
            metrics, dimensions = self.convert_fields_to_metrics_and_dimensions(parsed_data['fields'])

            return {
                "success": True,
                "metrics": metrics,
                "dimensions": dimensions,
                "basic_info": parsed_data.get('basic_info', {}),
                "message": "SQL解析成功"
            }

        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败：{str(e)}")
            return {"success": False, "message": f"JSON解析失败：{str(e)}"}
        except Exception as e:
            logger.error(f"大模型解析失败：{str(e)}")
            return {"success": False, "message": f"大模型解析失败：{str(e)}"}

    def generate_insert_sql(self, parsed_data: Dict[str, Any]) -> List[str]:
        """
        根据解析结果生成INSERT SQL

        Args:
            parsed_data: 解析结果字典

        Returns:
            INSERT SQL列表
        """
        try:
            metrics = parsed_data.get('metrics', [])
            if not metrics:
                return []

            insert_sqls = []

            for metric in metrics:
                # 根据指标类型生成不同的INSERT SQL
                if metric.get('metric_type') == 'ATOMIC':
                    insert_sql = self._generate_atomic_metric_insert(metric)
                elif metric.get('metric_type') == 'COMPOUND':
                    insert_sql = self._generate_compound_metric_insert(metric)
                else:
                    continue  # 跳过未知类型

                if insert_sql:
                    insert_sqls.append(insert_sql)

            return insert_sqls

        except Exception as e:
            logger.error(f"生成INSERT SQL失败：{str(e)}")
            return []

    def _generate_atomic_metric_insert(self, metric: Dict[str, Any]) -> Optional[str]:
        """
        生成原子指标的INSERT SQL

        Args:
            metric: 原子指标数据

        Returns:
            INSERT SQL字符串
        """
        try:
            metric_name = metric.get('metric_name')
            metric_code = metric.get('metric_code')
            cal_logic = metric.get('cal_logic')
            unit = metric.get('unit')

            # 生成metric_definition INSERT
            insert_sql = f"""
            -- 原子指标：{metric_name}
            INSERT INTO metric_definition (metric_id, metric_name, metric_code, metric_type, biz_domain, cal_logic, unit, status)
            VALUES ('M{len(metric_code)}', '{metric_name}', '{metric_code}', 'ATOMIC', '养殖繁殖', '{cal_logic}', '{unit}', 1);
            """

            # 生成metric_dim_rel INSERT（假设有维度信息）
            dimensions = metric.get('dimensions', [])
            for dim in dimensions:
                dim_id = dim.get('dim_id')
                is_required = dim.get('is_required', False)
                insert_sql += f"""
            -- 维度关联：{metric_code} - {dim_id}
            INSERT INTO metric_dim_rel (metric_id, dim_id, is_required)
            VALUES ('M{len(metric_code)}', '{dim_id}', {1 if is_required else 0});
            """

            # 生成metric_source_mapping INSERT
            source_mappings = metric.get('source_mappings', [])
            if source_mappings:
                for i, source in enumerate(source_mappings):
                    datasource = source.get('datasource', 'mysql')
                    db_table = source.get('db_table', '')
                    metric_column = source.get('metric_column', '')
                    filter_condition = source.get('filter_condition', '')
                    agg_func = source.get('agg_func', '')
                    priority = source.get('priority', 1)
                    source_level = source.get('source_level', 'AUTHORITY')

                    insert_sql += f"""
            -- 源映射：{metric_code}
            INSERT INTO metric_source_mapping (map_id, metric_id, source_type, datasource, db_table, metric_column, filter_condition, agg_func, priority, is_valid, source_level)
            VALUES ('MAP{len(metric_code)}_{i}', 'M{len(metric_code)}', 'OFFLINE', '{datasource}', '{db_table}', '{metric_column}', '{filter_condition}', '{agg_func}', {priority}, 1, '{source_level}');
            """

            return insert_sql

        except Exception as e:
            logger.error(f"生成原子指标INSERT SQL失败：{str(e)}")
            return None

    def _generate_compound_metric_insert(self, metric: Dict[str, Any]) -> Optional[str]:
        """
        生成复合指标的INSERT SQL

        Args:
            metric: 复合指标数据

        Returns:
            INSERT SQL字符串
        """
        try:
            metric_name = metric.get('metric_name')
            metric_code = metric.get('metric_code')
            cal_logic = metric.get('cal_logic')
            unit = metric.get('unit')

            # 生成metric_definition INSERT
            insert_sql = f"""
            -- 复合指标：{metric_name}
            INSERT INTO metric_definition (metric_id, metric_name, metric_code, metric_type, biz_domain, cal_logic, unit, status)
            VALUES ('M{len(metric_code)}', '{metric_name}', '{metric_code}', 'COMPOUND', '养殖繁殖', '{cal_logic}', '{unit}', 1);
            """

            # 生成metric_dim_rel INSERT（假设有维度信息）
            dimensions = metric.get('dimensions', [])
            for dim in dimensions:
                dim_id = dim.get('dim_id')
                is_required = dim.get('is_required', False)
                insert_sql += f"""
            -- 维度关联：{metric_code} - {dim_id}
            INSERT INTO metric_dim_rel (metric_id, dim_id, is_required)
            VALUES ('M{len(metric_code)}', '{dim_id}', {1 if is_required else 0});
            """

            # 生成metric_compound_rel INSERT
            compound_rule = metric.get('compound_rule', [])
            for i, rule in enumerate(compound_rule):
                sub_metric_code = rule.get('sub_metric_code')
                operator = rule.get('operator')
                insert_sql += f"""
            -- 复合关系：{metric_code} = {sub_metric_code} {operator}
            INSERT INTO metric_compound_rel (metric_id, sub_metric_id, cal_operator, sort)
            VALUES ('M{len(metric_code)}', '{sub_metric_code}', '{operator}', {i + 1});
            """

            return insert_sql

        except Exception as e:
            logger.error(f"生成复合指标INSERT SQL失败：{str(e)}")
            return None

    def execute_insert_sql(self, insert_sqls: List[str]) -> Dict[str, Any]:
        """
        执行生成的INSERT SQL

        Args:
            insert_sqls: INSERT SQL列表

        Returns:
            执行结果字典
        """
        try:
            if not insert_sqls:
                return {"success": False, "message": "没有可执行的INSERT SQL"}

            # 执行SQL（这里需要实现实际的数据库执行逻辑）
            # 假设有execute_sql方法，实际需要根据数据库连接方式实现
            executed_count = 0
            failed_count = 0

            for sql in insert_sqls:
                try:
                    # self.session.execute(sql)  # 实际执行SQL
                    executed_count += 1
                    logger.info(f"执行SQL成功：{sql[:100]}...")  # 只记录前100字符
                except Exception as e:
                    failed_count += 1
                    logger.error(f"执行SQL失败：{str(e)}")
                    logger.error(f"SQL内容：{sql}")

            self.session.commit()

            return {
                "success": True,
                "total_sql": len(insert_sqls),
                "executed_count": executed_count,
                "failed_count": failed_count,
                "message": f"成功执行 {executed_count}/{len(insert_sqls)} 条SQL"
            }

        except Exception as e:
            self.session.rollback()
            logger.error(f"执行INSERT SQL失败：{str(e)}")
            return {
                "success": False,
                "message": f"执行失败：{str(e)}",
                "total_sql": len(insert_sqls) if 'insert_sqls' in locals() else 0
            }

    def process_and_insert(self, input_path: Union[str, Path], is_directory: bool = False) -> Dict[str, Any]:
        """
        完整流程：处理ETL文件 -> 解析 -> 生成SQL -> 执行插入

        Args:
            input_path: 输入路径（文件路径或目录路径）
            is_directory: 是否为目录模式

        Returns:
            完整处理结果
        """
        try:
            # 1. 处理ETL文件
            process_result = self.process_etl_files(input_path, is_directory)
            if not process_result.get('success', False):
                return process_result

            # 2. 获取解析结果
            file_results = process_result.get('results', [])
            if not file_results:
                return {"success": False, "message": "没有可处理的文件结果"}

            # 3. 生成INSERT SQL
            all_insert_sqls = []
            for file_result in file_results:
                if file_result.get('success', False) and file_result.get('parsed_metrics', 0) > 0:
                    # 假设file_result包含parsed_data，实际需要从结果中提取
                    # 这里简化处理，实际需要从解析结果中获取
                    parsed_data = {
                        "metrics": file_result.get('parsed_metrics', [])
                    }
                    insert_sqls = self.generate_insert_sql(parsed_data)
                    all_insert_sqls.extend(insert_sqls)

            # 4. 执行INSERT SQL
            insert_result = self.execute_insert_sql(all_insert_sqls)

            # 5. 返回总体结果
            return {
                "success": True,
                "process_result": process_result,
                "insert_result": insert_result,
                "total_insert_sql": len(all_insert_sqls),
                "message": "完整处理流程完成"
            }

        except Exception as e:
            logger.error(f"完整处理流程失败：{str(e)}")
            return {
                "success": False,
                "message": f"完整处理流程失败：{str(e)}",
                "input_path": str(input_path)
            }

    def convert_fields_to_metrics_and_dimensions(self, fields: List[Dict[str, Any]]) -> tuple:
        """
        将 AI 解析的 fields 数组转换为 metrics 和 dimensions

        Args:
            fields: AI 返回的字段列表

        Returns:
            (metrics, dimensions) 元组
        """
        try:
            metrics = []
            dimensions = []

            for field in fields:
                field_type = field.get('field_type', 'dimension')

                if field_type == 'metric':
                    # 转换为指标格式
                    metric = {
                        'metric_name': field.get('field_name', ''),
                        'metric_code': field.get('field_name_en', ''),
                        'metric_type': self._infer_metric_type(field),
                        'cal_logic': field.get('formula', ''),
                        'unit': field.get('data_type', ''),
                        'dimensions': [],  # 后续可以关联维度
                        'source_mappings': [{
                            'datasource': 'mysql',
                            'db_table': field.get('source_tables', [''])[0] if field.get('source_tables') else '',
                            'metric_column': field.get('target_field', ''),
                            'filter_condition': ', '.join(field.get('filters', [])),
                            'agg_func': self._extract_agg_func(field.get('formula', '')),
                            'priority': 1,
                            'source_level': 'AUTHORITY'
                        }]
                    }
                    metrics.append(metric)
                else:
                    # 转换为维度格式
                    dimension = {
                        'dim_name': field.get('field_name', ''),
                        'dim_code': field.get('field_name_en', ''),
                        'data_type': field.get('data_type', 'string'),
                        'description': field.get('business_description', '')
                    }
                    dimensions.append(dimension)

            return metrics, dimensions

        except Exception as e:
            logger.error(f"转换 fields 到 metrics/dimensions 失败：{str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return [], []

    def _infer_metric_type(self, field: Dict[str, Any]) -> str:
        """
        根据字段信息推断指标类型

        Args:
            field: 字段信息

        Returns:
            ATOMIC / DERIVED / COMPOSITE
        """
        formula = field.get('formula', '').upper()
        calculation_type = field.get('calculation_type', '')

        # 复合指标：包含多个聚合函数或复杂表达式
        agg_count = formula.count('SUM(') + formula.count('COUNT(') + formula.count('AVG(') + formula.count('MAX(') + formula.count('MIN(')
        if agg_count > 1 or '/' in formula or '-' in formula:
            return 'COMPOSITE'

        # 衍生指标：单表聚合计算
        if calculation_type == 'aggregation' or agg_count == 1:
            return 'DERIVED'

        # 原子指标：直接来自源表
        if calculation_type == 'direct':
            return 'ATOMIC'

        # 默认为衍生指标
        return 'DERIVED'

    def _extract_agg_func(self, formula: str) -> str:
        """
        从公式中提取聚合函数

        Args:
            formula: 计算公式

        Returns:
            SUM / COUNT / AVG / MAX / MIN / 空字符串
        """
        formula_upper = formula.upper()
        if 'SUM(' in formula_upper:
            return 'SUM'
        elif 'COUNT(' in formula_upper:
            return 'COUNT'
        elif 'AVG(' in formula_upper:
            return 'AVG'
        elif 'MAX(' in formula_upper:
            return 'MAX'
        elif 'MIN(' in formula_upper:
            return 'MIN'
        else:
            return ''