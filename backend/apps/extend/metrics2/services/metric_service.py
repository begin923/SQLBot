from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from apps.extend.metrics2.curd import (
    create_metric_definition,
    batch_create_metric_definition,
    create_metric_dim_rel,
    batch_create_metric_dim_rel,
    create_metric_source_mapping,
    batch_create_metric_source_mapping,
    create_metric_compound_rel,
    batch_create_metric_compound_rel
)
from apps.extend.metrics2.models import (
    MetricDefinitionInfo,
    MetricDimRelInfo,
    MetricSourceMappingInfo,
    MetricCompoundRelInfo
)
from apps.extend.utils.utils import ModelClient
import logging

logger = logging.getLogger("MetricService")


class MetricService:
    """指标服务类 - 负责业务逻辑编排和流程组装"""

    def __init__(self, session: Session):
        """
        初始化指标服务

        Args:
            session: 数据库会话
        """
        self.session = session
        self.model_client = ModelClient()

    def create_metric_with_relations(self, metric_info: MetricDefinitionInfo,
                                 dim_rels: List[MetricDimRelInfo],
                                 source_mappings: List[MetricSourceMappingInfo],
                                 compound_rels: Optional[List[MetricCompoundRelInfo]] = None) -> Dict[str, Any]:
        """
        创建指标及其关联关系（维度、源映射、复合关系）

        Args:
            metric_info: 指标定义信息
            dim_rels: 维度关联列表
            source_mappings: 源映射列表
            compound_rels: 复合关系列表（可选）

        Returns:
            创建结果字典
        """
        try:
            # 1. 创建指标定义
            metric_id = create_metric_definition(self.session, metric_info)
            logger.info(f"成功创建指标定义：{metric_id}")

            # 2. 创建维度关联
            dim_rel_results = []
            for dim_rel in dim_rels:
                dim_rel.metric_id = metric_id  # 设置指标ID
                rel_id = create_metric_dim_rel(self.session, dim_rel)
                dim_rel_results.append({"id": rel_id, "dim_id": dim_rel.dim_id})

            # 3. 创建源映射
            source_mapping_results = []
            for mapping in source_mappings:
                mapping.metric_id = metric_id  # 设置指标ID
                map_id = create_metric_source_mapping(self.session, mapping)
                source_mapping_results.append({"id": map_id, "datasource": mapping.datasource})

            # 4. 创建复合关系（如果存在）
            compound_rel_results = []
            if compound_rels:
                for compound_rel in compound_rels:
                    compound_rel.metric_id = metric_id  # 设置指标ID
                    rel_id = create_metric_compound_rel(self.session, compound_rel)
                    compound_rel_results.append({"id": rel_id, "sub_metric_id": compound_rel.sub_metric_id})

            # 提交事务
            self.session.commit()

            return {
                "metric_id": metric_id,
                "dim_relations": dim_rel_results,
                "source_mappings": source_mappings,
                "compound_relations": compound_rel_results,
                "success": True,
                "message": "指标及其关联关系创建成功"
            }

        except Exception as e:
            self.session.rollback()
            logger.error(f"创建指标及其关联关系失败：{str(e)}")
            return {
                "success": False,
                "message": f"创建失败：{str(e)}",
                "metric_id": metric_info.metric_id if hasattr(metric_info, 'metric_id') else None
            }

    def batch_create_metrics_with_relations(self, metrics_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        批量创建指标及其关联关系

        Args:
            metrics_data: 指标数据列表，每个元素包含：
                - metric_info: 指标定义信息
                - dim_rels: 维度关联列表
                - source_mappings: 源映射列表
                - compound_rels: 复合关系列表（可选）

        Returns:
            批量创建结果
        """
        try:
            # 验证输入数据
            if not metrics_data or len(metrics_data) == 0:
                return {"success": False, "message": "输入数据不能为空"}

            # 收集所有需要创建的指标定义
            metric_infos = [data['metric_info'] for data in metrics_data]
            dim_rels_list = [data.get('dim_rels', []) for data in metrics_data]
            source_mappings_list = [data.get('source_mappings', []) for data in metrics_data]
            compound_rels_list = [data.get('compound_rels', []) for data in metrics_data]

            # 1. 批量创建指标定义
            metric_results = batch_create_metric_definition(self.session, metric_infos)
            logger.info(f"批量创建指标定义完成，成功：{metric_results['success_count']}，失败：{len(metric_results['failed_records'])}")

            # 2. 为每个成功创建的指标创建关联关系
            success_metrics = []
            for i, metric_info in enumerate(metric_infos):
                if i < metric_results['success_count']:  # 只处理成功创建的指标
                    metric_id = metric_info.metric_id  # 假设metric_info已包含metric_id
                    dim_rels = dim_rels_list[i]
                    source_mappings = source_mappings_list[i]
                    compound_rels = compound_rels_list[i]

                    # 创建维度关联
                    for dim_rel in dim_rels:
                        dim_rel.metric_id = metric_id
                        create_metric_dim_rel(self.session, dim_rel)

                    # 创建源映射
                    for mapping in source_mappings:
                        mapping.metric_id = metric_id
                        create_metric_source_mapping(self.session, mapping)

                    # 创建复合关系
                    if compound_rels:
                        for compound_rel in compound_rels:
                            compound_rel.metric_id = metric_id
                            create_metric_compound_rel(self.session, compound_rel)

                    success_metrics.append(metric_id)

            # 提交事务
            self.session.commit()

            return {
                "success": True,
                "message": "批量创建指标及其关联关系成功",
                "total_processed": len(metrics_data),
                "success_count": len(success_metrics),
                "failed_count": len(metrics_data) - len(success_metrics),
                "success_metrics": success_metrics,
                "batch_result": metric_results
            }

        except Exception as e:
            self.session.rollback()
            logger.error(f"批量创建指标及其关联关系失败：{str(e)}")
            return {
                "success": False,
                "message": f"批量创建失败：{str(e)}",
                "total_processed": len(metrics_data) if 'metrics_data' in locals() else 0
            }

    def get_metric_with_relations(self, metric_id: str) -> Dict[str, Any]:
        """
        获取指标及其所有关联关系

        Args:
            metric_id: 指标ID

        Returns:
            包含指标及其关联关系的字典
        """
        try:
            # 获取指标定义
            metric_info = get_metric_definition_by_id(self.session, metric_id)
            if not metric_info:
                return {"success": False, "message": "指标不存在"}

            # 获取维度关联
            dim_rels = get_metric_dim_rel_by_metric_id(self.session, metric_id)

            # 获取源映射
            source_mappings = get_metric_source_mapping_by_metric_id(self.session, metric_id)

            # 获取复合关系
            compound_rels = get_metric_compound_rel_by_metric_id(self.session, metric_id)

            return {
                "success": True,
                "metric": metric_info,
                "dimension_relations": dim_rels,
                "source_mappings": source_mappings,
                "compound_relations": compound_rels
            }

        except Exception as e:
            logger.error(f"获取指标及其关联关系失败：{str(e)}")
            return {
                "success": False,
                "message": f"获取失败：{str(e)}",
                "metric_id": metric_id
            }

    def parse_etl_and_create_metrics(self, sql_content: str, sql_file: str = "") -> Dict[str, Any]:
        """
        解析ETL脚本并创建指标（使用大模型）

        Args:
            sql_content: SQL脚本内容
            sql_file: SQL文件名（可选）

        Returns:
            解析和创建结果
        """
        try:
            # 1. 调用大模型解析SQL
            result = self.model_client.call_ai(
                template_name="sql_analysis",
                sql_content=sql_content,
                sql_file=sql_file
            )

            if not result:
                return {"success": False, "message": "大模型返回空内容"}

            # 2. 解析大模型返回的JSON（这里需要实现JSON解析逻辑）
            # 假设result是一个JSON字符串，包含dimensions和metrics
            import json
            parsed_data = json.loads(result)

            # 3. 提取指标数据
            metrics_data = parsed_data.get('metrics', [])
            if not metrics_data:
                return {"success": False, "message": "未解析到指标数据"}

            # 4. 准备批量创建数据
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
                if metric.get('metric_type') == 'ATOMIC' and metric.get('source'):
                    source = metric.get('source')
                    source_mappings.append(MetricSourceMappingInfo(
                        datasource=source.get('datasource'),
                        db_table=source.get('db_table'),
                        metric_column=source.get('metric_column'),
                        filter_condition=source.get('filter_condition'),
                        agg_func=source.get('agg_func'),
                        priority=source.get('priority', 1),
                        source_level=source.get('source_level', 'AUTHORITY')
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

            # 5. 批量创建指标及其关联关系
            batch_result = self.batch_create_metrics_with_relations(batch_data)

            return {
                "success": batch_result.get('success', False),
                "message": batch_result.get('message', '未知错误'),
                "parsed_metrics": len(metrics_data),
                "created_metrics": batch_result.get('success_count', 0),
                "batch_result": batch_result
            }

        except Exception as e:
            logger.error(f"解析ETL并创建指标失败：{str(e)}")
            return {
                "success": False,
                "message": f"解析创建失败：{str(e)}",
                "sql_file": sql_file
            }

    def update_metric_with_relations(self, metric_id: str,
                                 metric_info: MetricDefinitionInfo,
                                 dim_rels: List[MetricDimRelInfo],
                                 source_mappings: List[MetricSourceMappingInfo],
                                 compound_rels: Optional[List[MetricCompoundRelInfo]] = None) -> Dict[str, Any]:
        """
        更新指标及其关联关系

        Args:
            metric_id: 指标ID
            metric_info: 指标定义信息
            dim_rels: 维度关联列表
            source_mappings: 源映射列表
            compound_rels: 复合关系列表（可选）

        Returns:
            更新结果字典
        """
        try:
            # 1. 更新指标定义
            update_metric_definition(self.session, metric_info)
            logger.info(f"成功更新指标定义：{metric_id}")

            # 2. 更新维度关联（先删除旧关联，再创建新关联）
            # 删除旧维度关联
            old_dim_rels = get_metric_dim_rel_by_metric_id(self.session, metric_id)
            delete_metric_dim_rel(self.session, [rel.id for rel in old_dim_rels])

            # 创建新维度关联
            for dim_rel in dim_rels:
                dim_rel.metric_id = metric_id
                create_metric_dim_rel(self.session, dim_rel)

            # 3. 更新源映射（先删除旧映射，再创建新映射）
            old_mappings = get_metric_source_mapping_by_metric_id(self.session, metric_id)
            delete_metric_source_mapping(self.session, [mapping.map_id for mapping in old_mappings])

            # 创建新源映射
            for mapping in source_mappings:
                mapping.metric_id = metric_id
                create_metric_source_mapping(self.session, mapping)

            # 4. 更新复合关系（先删除旧关系，再创建新关系）
            if compound_rels:
                old_compound_rels = get_metric_compound_rel_by_metric_id(self.session, metric_id)
                delete_metric_compound_rel(self.session, [rel.id for rel in old_compound_rels])

                # 创建新复合关系
                for compound_rel in compound_rels:
                    compound_rel.metric_id = metric_id
                    create_metric_compound_rel(self.session, compound_rel)

            # 提交事务
            self.session.commit()

            return {
                "metric_id": metric_id,
                "success": True,
                "message": "指标及其关联关系更新成功"
            }

        except Exception as e:
            self.session.rollback()
            logger.error(f"更新指标及其关联关系失败：{str(e)}")
            return {
                "success": False,
                "message": f"更新失败：{str(e)}",
                "metric_id": metric_id
            }

    def delete_metric_with_relations(self, metric_id: str) -> Dict[str, Any]:
        """
        删除指标及其关联关系

        Args:
            metric_id: 指标ID

        Returns:
            删除结果字典
        """
        try:
            # 1. 获取指标的所有关联关系
            dim_rels = get_metric_dim_rel_by_metric_id(self.session, metric_id)
            source_mappings = get_metric_source_mapping_by_metric_id(self.session, metric_id)
            compound_rels = get_metric_compound_rel_by_metric_id(self.session, metric_id)

            # 2. 删除关联关系
            if dim_rels:
                delete_metric_dim_rel(self.session, [rel.id for rel in dim_rels])
            if source_mappings:
                delete_metric_source_mapping(self.session, [mapping.map_id for mapping in source_mappings])
            if compound_rels:
                delete_metric_compound_rel(self.session, [rel.id for rel in compound_rels])

            # 3. 删除指标定义
            delete_metric_definition(self.session, [metric_id])

            # 提交事务
            self.session.commit()

            return {
                "metric_id": metric_id,
                "success": True,
                "message": "指标及其关联关系删除成功"
            }

        except Exception as e:
            self.session.rollback()
            logger.error(f"删除指标及其关联关系失败：{str(e)}")
            return {
                "success": False,
                "message": f"删除失败：{str(e)}",
                "metric_id": metric_id
            }