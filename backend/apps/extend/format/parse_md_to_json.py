#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Markdown to JSON Converter
Converts structured Markdown documentation to standardized JSON format
"""

import json
import os
import re
import sys
import ast
from typing import Dict, List, Any, Optional

# 导入指标元数据模型
from apps.extend.metric_metadata.models.metric_metadata_model import MetricMetadataInfo


class ParseMDToJson:
    """MD 文档解析器，支持血缘和维度属性解析"""

    def __init__(self):
        pass


    def parse_markdown_file(self,file_path: str) -> str:
        """读取Markdown文件内容"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Markdown file not found: {file_path}")
        except Exception as e:
            raise Exception(f"Error reading file: {str(e)}")


    def extract_basic_info(self,markdown: str) -> Dict[str, str]:
        """从 Markdown 中提取基本信息 - 使用逐行解析"""
        lines = markdown.split('\n')

        for i, line in enumerate(lines):
            if '| file_name |' in line or 'file_name' in line:
                # 这是表头行，下一行是数据行
                if i + 2 < len(lines):  # 跳过表头和分隔线
                    data_line = lines[i + 2]  # 表头->分隔线->数据行
                    if data_line.startswith('|') and 'ads_' in data_line:
                        # 分割单元格（去掉首尾的|）
                        parts = data_line.strip().split('|')
                        cells = [part.strip() for part in parts[1:-1]]  # 去掉首尾空元素

                        # 根据表头确定索引
                        header_parts = line.strip().split('|')
                        headers = [h.strip() for h in header_parts[1:-1]]

                        result = {}
                        for idx, header in enumerate(headers):
                            if idx < len(cells):
                                if header == 'file_name':
                                    result['file_name'] = cells[idx]
                                elif header == 'table_name':  # table_name 映射到 target_table_name
                                    result['target_table_name'] = cells[idx]
                                elif header == 'table_desc':  # table_desc 映射到 target_table_desc
                                    result['target_table_desc'] = cells[idx]
                                elif header == 'warehouse_layer':
                                    result['warehouse_layer'] = cells[idx]
                                elif header == 'updated_at':
                                    result['updated_at'] = cells[idx]
                                elif header == 'source_system':
                                    result['source_system'] = cells[idx]
                                elif header == 'version':
                                    result['version'] = cells[idx]

                        return result

        raise ValueError("Basic info not found in markdown")


    def extract_field_list(self,markdown: str) -> List[Dict[str, Any]]:
        """从 Markdown 中提取字段清单 - 使用逐行解析"""
        lines = markdown.split('\n')
        fields = []
        in_field_section = False
        headers = []

        for line in lines:
            line = line.strip()

            # 检测字段清单章节
            if line.startswith('## 字段清单'):
                in_field_section = True
                continue

            if not in_field_section:
                continue

            # 跳过空行和分隔线
            if not line or line.startswith('|---') or line.startswith('|------'):
                continue

            # 检测表头行
            if line.startswith('|') and 'field_id' in line:
                headers = [cell.strip() for cell in line.split('|') if cell.strip()]
                continue

            # 解析数据行
            if line.startswith('|') and headers:
                cells = [cell.strip() for cell in line.split('|') if cell.strip()]

                # 确保单元格数量与表头一致（允许最后一列为空）
                while len(cells) < len(headers):
                    cells.append('')

                field_dict = {}
                for idx, header in enumerate(headers):
                    if idx < len(cells):
                        field_dict[header] = cells[idx]

                if field_dict:
                    fields.append(field_dict)

        return fields


    def parse_array(self,value: str) -> List[str]:
        """解析数组字符串"""
        if not value or value.strip() in ['[]', '']:
            return []

        try:
            # 尝试解析JSON数组
            return ast.literal_eval(value)
        except:
            # 处理简单逗号分隔的字符串
            return [item.strip().strip('"') for item in value.split(',') if item.strip() != '']


    def parse_joins(self,value: str) -> List[Dict[str, str]]:
        """解析JOIN条件字符串"""
        if not value or value.strip() in ['[]', '']:
            return []

        try:
            # 尝试解析JSON数组
            joins = ast.literal_eval(value)
            # 确保是字典列表
            return [join for join in joins if isinstance(join, dict)]
        except:
            return []


    def parse_boolean(self,value: str) -> bool:
        """解析布尔值字符串"""
        if isinstance(value, bool):
            return value
        return value.strip().lower() == 'true'


    def create_metric_dict(self,fields: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """从字段列表创建指标字典 (以 metric_name_en 为 key)"""
        metrics = {}
        for field in fields:
            if field['field_type'] != 'metric':
                continue

            metric_name_en = field['field_name_en']
            metrics[metric_name_en] = self.convert_field_to_metric(field)

        return metrics


    def convert_field_to_metric(self,field: Dict[str, Any]) -> Dict[str, Any]:
        """将字段转换为指标格式"""
        return {
            "metric_id": field.get('field_id', ''),
            "metric_name": field.get('field_name', ''),
            "metric_type": "composite" if field.get('is_cross_table', False) or field.get(
                'calculation_type', '') == 'expression' else "derived",
            "data_type": field.get('data_type', ''),
            "calculation": {
                "type": field.get('calculation_type', ''),
                "formula": field.get('formula', ''),
                "dependencies": self.build_dependencies(field)
            },
            "filters": self.parse_array(field.get('filters', '[]')),
            "joins": self.parse_joins(field.get('joins', '[]')),
            "business": {
                "description": field.get('business_description', ''),
                "owner": "数据团队",
                "domain": self.infer_business_domain(field)
            },
            "support_drill_down": self.parse_boolean(field.get('support_drill_down', 'false')),
            "is_cross_table": self.parse_boolean(field.get('is_cross_table', 'false')),
            "drill_down_fields": self.parse_array(field.get('drill_down_fields', '[]'))
        }


    def build_dependencies(self,field: Dict[str, Any]) -> List[Dict[str, List[str]]]:
        """构建依赖关系"""
        dependencies = []

        # 解析 source_tables 和 source_fields
        source_tables = self.parse_array(field.get('source_tables', '[]'))
        source_fields = self.parse_array(field.get('source_fields', '[]'))

        if not source_tables and not source_fields:
            return []

        # 按表分组字段
        table_fields_map = {}
        for field_name in source_fields:
            if '.' in field_name:
                parts = field_name.rsplit('.', 1)
                tbl = parts[0]
                fld = parts[1]
                if tbl not in table_fields_map:
                    table_fields_map[tbl] = []
                table_fields_map[tbl].append(fld)

        # 构建 dependencies
        for tbl in table_fields_map:
            dependencies.append({"table": tbl, "fields": table_fields_map[tbl]})

        return dependencies


    def build_joins(self,joins: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """构建JOIN条件列表"""
        return [
            {"condition": join['condition'], "type": join['type']}
            for join in joins
            if 'condition' in join and 'type' in join
        ]


    def infer_business_domain(self,field: Dict[str, Any]) -> str:
        """推断业务域"""
        text = f"{field['field_name']} {field['business_description']}"

        if "发情" in text or "配种" in text or "分娩" in text or "断奶" in text:
            return "繁殖"
        if "死亡" in text or "淘汰" in text or "健康" in text:
            return "养殖"
        if "批次" in text or "计划" in text:
            return "生产"

        return "待确认"


    def create_json_result(self,basic_info: Dict[str, str], metrics: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """创建最终的 JSON 结果"""
        return {
            "success": True,
            "data": {
                "meta": {
                    "version": basic_info.get('version', '1.0'),
                    "updated_at": basic_info.get('updated_at', '2026-03-18T09:00:00Z'),
                    "source_system": "datawarehouse"
                },
                "target": {
                    "table_id": "",
                    "table_name": basic_info['target_table_name'],  # table_name
                    "desc": basic_info['target_table_desc'],  # table_desc
                    "file_path": f"{basic_info['warehouse_layer']}/{basic_info['file_name']}"
                },
                "metrics": metrics
            }
        }


    def parse_single_md_file(self, markdown_file: str) -> Dict[str, Any]:
        """解析单个 Markdown 文件并返回 JSON 结果"""
        print(f"markdown_file:{markdown_file}")
            
        # 1. 读取 Markdown 文件
        markdown_content = self.parse_markdown_file(markdown_file)
    
        # 2. 提取基本信息
        basic_info = self.extract_basic_info(markdown_content)
    
        # 3. 提取字段清单
        fields = self.extract_field_list(markdown_content)
    
        # 4. 创建指标字典
        metrics = self.create_metric_dict(fields)
    
        # 5. 创建最终 JSON
        result = self.create_json_result(basic_info, metrics)
    
        # 6. 输出结果
        # print(json.dumps(result, indent=2, ensure_ascii=False))
    
        return result
    
    
    def parse_md_to_json(self, markdown_path: str = None ,dw_layer: str = None, table_name: str = None):
        """主函数：处理整个转换流程"""
        # 如果没有传入 markdown_path，使用默认路径
        if not dw_layer:
            dw_layer = "yz_datawarehouse_ads"
        if not markdown_path:
            # 获取当前脚本所在目录
            current_script_dir = os.path.dirname(os.path.abspath(__file__))
            # 回退到上一级目录的 metric_blood 文件夹
            parent_dir = os.path.dirname(current_script_dir)
            metric_blood_path_parent = os.path.join(parent_dir, 'metric_blood')
            metric_blood_path = os.path.join(metric_blood_path_parent, dw_layer)
            print(f"metric_blood_path:{metric_blood_path}")
            if os.path.exists(metric_blood_path) and os.path.isdir(metric_blood_path):
                markdown_path = metric_blood_path
            else:
                return "未找到 metric_blood 目录"
        
        # 优先使用传入的参数，如果没有则使用默认路径
        # 默认路径
        markdown_file = ""
        results = []

        print(f"markdown_path:{markdown_path}")
        try:
            if markdown_path:
                if table_name:
                    markdown_file = os.path.join(markdown_path, f"{table_name}.md")
                    if not os.path.exists(markdown_file):
                        return "文件不存在"
                    # 单文件模式
                    result = self.parse_single_md_file(markdown_file)
                    results.append(result)
                elif os.path.isdir(markdown_path):
                    # 目录模式：获取所有 md 文件
                    md_files = [os.path.join(markdown_path, f) for f in os.listdir(markdown_path) if f.endswith('.md')]
                    if not md_files:
                        return "目录下没有找到 Markdown 文件"
                    # 批量处理所有 md 文件
                    for md_file in md_files:
                        try:
                            result = self.parse_single_md_file(md_file)
                            results.append(result)
                        except Exception as e:
                            print(f"❌ Error processing {md_file}: {str(e)}")
                            results.append({
                                "success": False,
                                "error": str(e),
                                "file": md_file
                            })
            else:
                return "请输入正确的参数"
                
            return results

        except Exception as e:
            print(f"❌ Error: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def parse_md_to_metric_metadata_list(self, markdown_path: str = None , dw_layer = None, table_name: str = None) -> List[MetricMetadataInfo]:
        """解析 Markdown 文件并转换为 MetricMetadataInfo 列表
        
        Args:
            markdown_path: Markdown 文件路径或目录
            table_name: 表名（可选）
            
        Returns:
            MetricMetadataInfo 对象列表
        """
        # 调用原有方法获取 JSON 结果
        json_results = self.parse_md_to_json(markdown_path , dw_layer, table_name)

        # 如果返回的是错误信息，返回空列表
        if isinstance(json_results, str):
            print(f"⚠️  {json_results}")
            return []
        if isinstance(json_results, dict) and json_results.get('success') is False:
            print(f"❌ 解析失败：{json_results.get('error')}")
            return []
        
        metric_metadata_list = []
        
        # 遍历每个文件的解析结果
        for json_result in json_results:
            if not isinstance(json_result, dict) or not json_result.get('success'):
                continue
                
            data = json_result.get('data', {})
            target = data.get('target', {})
            metrics = data.get('metrics', {})
            
            # 从每个指标中提取信息
            for metric_name_en, metric_data in metrics.items():
                try:
                    # 构建 MetricMetadataInfo 对象
                    metric_info = MetricMetadataInfo(
                        metric_name=metric_data.get('metric_name', ''),
                        metric_column=metric_name_en,  # 使用 JSON 的 key 作为 metric_column
                        synonyms=None,  # Markdown 中没有同义词字段
                        datasource_id=None,  # 需要从其他地方获取
                        table_name=target.get('table_name', ''),  # 使用表名
                        core_fields=self._extract_core_fields(metrics),
                        calc_logic=metric_data.get('calculation', {}).get('formula', ''),
                        upstream_table=self._extract_upstream_table(metric_data),
                        dw_layer=target.get('file_path', '').split('/')[0] if target.get('file_path') else '',
                        enabled=True
                    )
                    metric_metadata_list.append(metric_info)
                except Exception as e:
                    print(f"⚠️  转换指标 {metric_name_en} 失败：{e}")
                    continue
        
        print(f"✅ 成功转换 {len(metric_metadata_list)} 个指标元数据")
        return metric_metadata_list
    
    def _extract_core_fields(self, metrics: Dict[str, Any]) -> str:
        """从指标字典中提取核心字段（逗号分隔）"""
        # 这里可以根据实际需求定义如何提取核心字段
        # 暂时返回空字符串，或者可以从 dependencies 中提取
        all_fields = set()
        for metric_data in metrics.values():
            dependencies = metric_data.get('calculation', {}).get('dependencies', [])
            for dep in dependencies:
                fields = dep.get('fields', [])
                all_fields.update(fields)
        return ', '.join(sorted(all_fields)) if all_fields else ''
    
    def _extract_upstream_table(self, metric_data: Dict[str, Any]) -> str:
        """从指标数据中提取上游表"""
        dependencies = metric_data.get('calculation', {}).get('dependencies', [])
        if dependencies:
            # 返回第一个依赖的表作为上游表
            return dependencies[0].get('table', '')
        return ''

if __name__ == "__main__":
    parse = ParseMDToJson()
    res = parse.parse_md_to_metric_metadata_list(table_name="yz_datawarehouse_ads.ads_pig_feed_sum_month")
    print(res)