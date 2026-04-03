#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Markdown to Metric Lineage and Dimension Converter
将 Markdown 文档解析为指标血缘和维度数据，并批量写入数据库
"""

import os
from typing import Dict, List, Any, Optional, Tuple

from apps.extend.metric_metadata.models.metric_lineage_model import MetricLineageInfo, MetricDimensionInfo
from apps.extend.metric_metadata.curd.metric_lineage import (
    batch_create_metric_lineage,
    batch_create_metric_dimension,
    delete_metric_lineage_by_table,
    delete_metric_dimension_by_table
)
from apps.extend.metric_metadata.parse_md_to_json import ParseMDToJson
from apps.extend.utils.utils import Utils
from common.core.deps import SessionDep


class ParseMDToLineageAndDimension:
    """MD 文档解析器，支持血缘和维度数据解析与入库"""

    def __init__(self):
        pass

    def parse_markdown_to_lineage_and_dimension(
        self,
        table_name: str ,
        markdown_path: str = None
    ) -> Tuple[List[MetricLineageInfo], List[MetricDimensionInfo]]:
        """
        解析 Markdown 文件并转换为血缘和维度信息列表
        
        Args:
            markdown_path: Markdown 文件路径或目录
            table_name: 表名（可选，用于单文件处理）
            
        Returns:
            (lineage_list, dimension_list) 元组
        """
        # 调用原有方法获取 JSON 结果

        parser = ParseMDToJson()
        json_results = parser.parse_md_to_json(table_name,markdown_path)

        # 如果返回的是错误信息，返回空列表
        if isinstance(json_results, str):
            print(f"⚠️  {json_results}")
            return [], []
        if isinstance(json_results, dict) and json_results.get('success') is False:
            print(f"❌ 解析失败：{json_results.get('error')}")
            return [], []
        
        lineage_list = []
        dimension_list = []
        
        # 遍历每个文件的解析结果
        for json_result in json_results:
            if not isinstance(json_result, dict) or not json_result.get('success'):
                continue
                
            data = json_result.get('data', {})
            target = data.get('target', {})
            metrics = data.get('metrics', [])
            dimensions = data.get('dimensions', [])  # 新增：获取维度字典
            fields = json_result.get('data', {}).get('fields', [])

            print(f"\n📝 metrics 数量：{len(metrics)} , dimensions 数量：{len(dimensions)}")
            
            table_name_val = target.get('table_name', '')
            
            # 解析血缘数据
            lineages = self._extract_lineages(metrics, table_name_val)
            lineage_list.extend(lineages)
            
            # 解析维度数据（从 dimensions 字典中提取）
            dims = self._extract_dimensions_from_dict(dimensions, table_name_val)
            dimension_list.extend(dims)
        
        print(f"✅ 成功解析 {len(lineage_list)} 个血缘关系，{len(dimension_list)} 个维度")
        return lineage_list, dimension_list

    def _extract_lineages(
        self, 
        metrics: List[Dict[str, Any]],
        table_name: str
    ) -> List[MetricLineageInfo]:
        """
        从指标字典中提取血缘信息
        
        Args:
            metrics: 指标字典
            table_name: 目标表名
            
        Returns:
            血缘信息列表
        """
        lineage_list = []
        
        print(f"\n📊 开始提取血缘信息...")
        print(f"   待处理指标数：{len(metrics)}")
        
        for idx, metric_data in enumerate(metrics, 1):
            try:
                # 获取指标字段名（从 metric_name_en）
                metric_column = metric_data.get('metric_name_en', '')
                
                # 提取计算逻辑
                calc_logic = metric_data.get('calculation', {}).get('formula', '')
                
                # 提取上游表（从依赖关系中获取第一个表）
                upstream_table = self._extract_upstream_table(metric_data)
                
                # 提取核心字段（从依赖关系中获取所有字段）
                core_fields = self._extract_core_fields(metric_data)
                
                # 构建血缘信息对象
                lineage_info = MetricLineageInfo(
                    metric_column=metric_column,
                    table_name=table_name,
                    metric_name=metric_data.get('metric_name', ''),
                    synonyms=None,  # Markdown 中没有同义词字段
                    upstream_table=upstream_table,
                    filter=core_fields,  # 使用核心字段作为过滤条件
                    calc_logic=calc_logic,
                    dw_layer=table_name.split('.')[0] if '.' in table_name else '',
                    enabled=True
                )
                lineage_list.append(lineage_info)
                
                print(f"   ✅ [{idx}/{len(metrics)}] {metric_column} -> {metric_data.get('metric_name', '')}")
                print(f"      计算逻辑：{calc_logic[:50]}..." if len(calc_logic) > 50 else f"      计算逻辑：{calc_logic}")
                print(f"      上游表：{upstream_table}")
                print(f"      核心字段：{core_fields}")
                
            except Exception as e:
                print(f"   ❌ [{idx}/{len(metrics)}] {metric_column} 提取失败：{e}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"✅ 血缘信息提取完成，共 {len(lineage_list)} 条\n")
        return lineage_list

    def _extract_dimensions_from_dict(
        self, 
        dimensions: List[Dict[str, Any]],
        table_name: str
    ) -> List[MetricDimensionInfo]:
        """
        从维度列表中提取维度信息
        
        Args:
            dimensions: 维度列表 [{"dim_name": "...", "dim_column": "..."}]
            table_name: 目标表名
            
        Returns:
            维度信息列表
        """
        dimension_list = []
        
        print(f"\n📊 开始提取维度信息...")
        print(f"   待处理维度数：{len(dimensions)}")
        
        for idx, dim_data in enumerate(dimensions, 1):
            try:
                dim_column = dim_data.get('dim_column', '')
                dim_name = dim_data.get('dim_name', '')
                
                if not dim_column:
                    print(f"   ⚠️  [{idx}/{len(dimensions)}] 维度字段名为空，跳过")
                    continue
                
                # 构建维度信息对象
                dimension_info = MetricDimensionInfo(
                    table_name=table_name,
                    dim_column=dim_column,
                    dim_name=dim_name,
                    enabled=True
                )
                dimension_list.append(dimension_info)
                
                print(f"   ✅ [{idx}/{len(dimensions)}] {dim_column} -> {dim_name}")
                
            except Exception as e:
                print(f"   ❌ [{idx}/{len(dimensions)}] {dim_column} 提取失败：{e}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"\n✅ 维度信息提取完成，共 {len(dimension_list)} 条\n")
        return dimension_list

    def _extract_upstream_table(self, metric_data: Dict[str, Any]) -> str:
        """
        从指标数据中提取上游表
        
        Args:
            metric_data: 指标数据
            
        Returns:
            上游表名
        """
        dependencies = metric_data.get('calculation', {}).get('dependencies', [])
        if dependencies:
            # 返回第一个依赖的表作为上游表
            return dependencies[0].get('table', '')
        return ''

    def _extract_core_fields(self, metric_data: Dict[str, Any]) -> str:
        """
        从指标数据中提取核心字段（逗号分隔）
        
        Args:
            metric_data: 指标数据
            
        Returns:
            核心字段字符串
        """
        all_fields = set()
        dependencies = metric_data.get('calculation', {}).get('dependencies', [])
        
        for dep in dependencies:
            fields = dep.get('fields', [])
            all_fields.update(fields)
        
        return ', '.join(sorted(all_fields)) if all_fields else ''

    def save_to_database(
        self, 
        session: SessionDep,
        lineage_list: List[MetricLineageInfo],
        dimension_list: List[MetricDimensionInfo]
    ) -> Dict[str, Any]:
        """
        将血缘和维度数据批量保存到数据库
        
        Args:
            session: 数据库会话
            lineage_list: 血缘信息列表
            dimension_list: 维度信息列表
            
        Returns:
            保存结果统计
        """
        result = {
            'lineage': {'success': 0, 'update': 0, 'failed': 0, 'duplicate': 0},
            'dimension': {'success': 0, 'update': 0, 'failed': 0, 'duplicate': 0}
        }
        
        try:
            # 批量保存血缘数据
            if lineage_list:
                print(f"\n💾 开始保存血缘数据到数据库...")
                print(f"   待保存记录数：{len(lineage_list)}")
                
                # 获取表名（从第一条记录）
                target_table_name = lineage_list[0].table_name
                
                # 步骤 1: 先删除该表的所有历史数据
                print(f"   🗑️  删除表 {target_table_name} 的历史血缘数据...")
                delete_metric_lineage_by_table(session, target_table_name)
                print(f"   ✅ 历史数据删除完成")
                
                # 步骤 2: 插入新数据
                print(f"   📝 插入新的血缘数据...")
                
                # 打印前 3 条记录的详细信息
                for i, lineage in enumerate(lineage_list[:3], 1):
                    print(f"   示例 [{i}]:")
                    print(f"      metric_column: {lineage.metric_column}")
                    print(f"      table_name: {lineage.table_name}")
                    print(f"      metric_name: {lineage.metric_name}")
                    print(f"      calc_logic: {lineage.calc_logic}")
                
                lineage_result = batch_create_metric_lineage(session, lineage_list)
                result['lineage']['success'] = lineage_result.get('success_count', 0)
                result['lineage']['update'] = lineage_result.get('update_count', 0)
                result['lineage']['failed'] = len(lineage_result.get('failed_records', []))
                result['lineage']['duplicate'] = lineage_result.get('duplicate_count', 0)
                
                # 如果有失败记录，打印详细信息
                if lineage_result.get('failed_records'):
                    print(f"\n❌ 血缘数据保存失败详情:")
                    for failed in lineage_result.get('failed_records', [])[:3]:
                        data = failed.get('data')
                        metric_column = getattr(data, 'metric_column', 'unknown') if hasattr(data, 'metric_column') else 'unknown'
                        errors = failed.get('errors', [])
                        print(f"   - {metric_column}: {errors}")
                
                print(f"\n✅ 血缘数据保存完成：成功 {result['lineage']['success']} 条 (新增 {result['lineage']['success'] - result['lineage']['update']} 条，更新 {result['lineage']['update']} 条), "
                      f"重复 {result['lineage']['duplicate']} 条，失败 {result['lineage']['failed']} 条")
            else:
                print(f"\n⏭️  没有血缘数据需要保存")
            
            # 批量保存维度数据
            if dimension_list:
                print(f"\n💾 开始保存维度数据到数据库...")
                print(f"   待保存记录数：{len(dimension_list)}")
                
                # 获取表名（从第一条记录）
                target_table_name = dimension_list[0].table_name
                
                # 步骤 1: 先删除该表的所有历史数据
                print(f"   🗑️  删除表 {target_table_name} 的历史维度数据...")
                delete_metric_dimension_by_table(session, target_table_name)
                print(f"   ✅ 历史数据删除完成")
                
                # 步骤 2: 插入新数据
                print(f"   📝 插入新的维度数据...")
                
                # 打印前 3 条记录的详细信息
                for i, dim in enumerate(dimension_list[:3], 1):
                    print(f"   示例 [{i}]:")
                    print(f"      table_name: {dim.table_name}")
                    print(f"      dim_column: {dim.dim_column}")
                    print(f"      dim_name: {dim.dim_name}")
                
                dimension_result = batch_create_metric_dimension(session, dimension_list)
                result['dimension']['success'] = dimension_result.get('success_count', 0)
                result['dimension']['update'] = dimension_result.get('update_count', 0)
                result['dimension']['failed'] = len(dimension_result.get('failed_records', []))
                result['dimension']['duplicate'] = dimension_result.get('duplicate_count', 0)
                
                # 如果有失败记录，打印详细信息
                if dimension_result.get('failed_records'):
                    print(f"\n❌ 维度数据保存失败详情:")
                    for failed in dimension_result.get('failed_records', [])[:3]:
                        data = failed.get('data')
                        dim_column = getattr(data, 'dim_column', 'unknown') if hasattr(data, 'dim_column') else 'unknown'
                        errors = failed.get('errors', [])
                        print(f"   - {dim_column}: {errors}")
                
                print(f"\n✅ 维度数据保存完成：成功 {result['dimension']['success']} 条 (新增 {result['dimension']['success'] - result['dimension']['update']} 条，更新 {result['dimension']['update']} 条), "
                      f"重复 {result['dimension']['duplicate']} 条，失败 {result['dimension']['failed']} 条")
            else:
                print(f"\n⏭️  没有维度数据需要保存")
            
            return result
            
        except Exception as e:
            print(f"\n❌ 保存到数据库失败：{e}")
            import traceback
            traceback.print_exc()
            raise

    def process_and_save(
        self, 
        session: SessionDep,
        table_name: str,
        markdown_path: str = None
    ) -> Dict[str, Any]:
        """
        一站式处理：解析 Markdown 并保存到数据库
        
        Args:
            session: 数据库会话
            markdown_path: Markdown 文件路径或目录
            table_name: 表名（可选）
            
        Returns:
            处理结果统计
        """
        print("\n" + "="*80)
        print(f"🚀 开始处理 Markdown 文件...")
        print("="*80)
        print(f"   📁 路径：{markdown_path or '默认路径'}")
        print(f"   📋 表名：{table_name or '全部'}")
        print("="*80 + "\n")
        
        # 解析文件
        lineage_list, dimension_list = self.parse_markdown_to_lineage_and_dimension(
            table_name,markdown_path,
        )
        
        if not lineage_list and not dimension_list:
            print("\n⚠️  没有需要保存的数据")
            return {
                'lineage': {'success': 0, 'failed': 0, 'duplicate': 0},
                'dimension': {'success': 0, 'failed': 0, 'duplicate': 0}
            }
        
        # 保存到数据库
        result = self.save_to_database(session, lineage_list, dimension_list)
        
        print("\n" + "="*80)
        print(f"🎉 处理完成！")
        print("="*80)
        print(f"📊 最终统计:")
        print(f"   血缘：成功 {result['lineage']['success']} 条 (新增 {result['lineage']['success'] - result['lineage']['update']} 条，更新 {result['lineage']['update']} 条), 重复 {result['lineage']['duplicate']} 条，失败 {result['lineage']['failed']} 条")
        print(f"   维度：成功 {result['dimension']['success']} 条 (新增 {result['dimension']['success'] - result['dimension']['update']} 条，更新 {result['dimension']['update']} 条), 重复 {result['dimension']['duplicate']} 条，失败 {result['dimension']['failed']} 条")
        print("="*80 + "\n")
        
        return result


# ========== 便捷函数 ==========

def parse_and_save_lineage_dimension(
    session: SessionDep,
    table_name: str,
    markdown_path: str = None
) -> Dict[str, Any]:
    """
    便捷函数：解析 Markdown 并保存血缘和维度数据到数据库
    
    Args:
        session: 数据库会话
        markdown_path: Markdown 文件路径或目录
        table_name: 表名（可选）
        
    Returns:
        处理结果统计
    """
    processor = ParseMDToLineageAndDimension()
    return processor.process_and_save(session, table_name, markdown_path)


if __name__ == "__main__":
    """
    本地测试入口
    注意：需要配置数据库会话才能执行完整流程
    """
    import sys
    import os
    
    # 添加项目根目录到 Python 路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_dir))))
    sys.path.insert(0, project_root)
    
    print(f"📁 项目根目录：{project_root}")
    print(f"📝 Python 路径：{sys.path[:3]}...\n")
    
    # 创建会话
    session = Utils.create_local_session()
    
    try:
        # 示例：处理单个表
        result = parse_and_save_lineage_dimension(
            session=session,
            table_name="yz_datawarehouse_dws.dws_male_entry_remove_detail"
        )
        
        print("\n📊 处理结果统计:")
        print(f"   血缘：成功 {result['lineage']['success']} 条 (新增 {result['lineage']['success'] - result['lineage']['update']} 条，更新 {result['lineage']['update']} 条), "
              f"重复 {result['lineage']['duplicate']} 条，失败 {result['lineage']['failed']} 条")
        print(f"   维度：成功 {result['dimension']['success']} 条 (新增 {result['dimension']['success'] - result['dimension']['update']} 条，更新 {result['dimension']['update']} 条), "
              f"重复 {result['dimension']['duplicate']} 条，失败 {result['dimension']['failed']} 条")
        
    except Exception as e:
        print(f"❌ 测试失败：{e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()
