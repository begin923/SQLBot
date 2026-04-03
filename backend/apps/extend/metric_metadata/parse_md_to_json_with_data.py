#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Markdown to JSON Converter using markdown-to-data
Converts structured Markdown documentation to standardized JSON format
使用 markdown-to-data 库进行解析
"""

import json
import os
import sys
from typing import Dict, List, Any, Optional
import ast
from markdown_to_data import md_to_dict

# 导入指标元数据模型
from apps.extend.metric_metadata.models.metric_metadata_model import MetricMetadataInfo


class ParseMDToJsonWithData:
    """MD 文档解析器，支持血缘和维度属性解析 - 使用 markdown-to-data 库"""

    def __init__(self):
        pass


    def parse_markdown_file(self, file_path: str) -> str:
        """读取 Markdown 文件内容"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Markdown file not found: {file_path}")
        except Exception as e:
            raise Exception(f"Error reading file: {str(e)}")


    def extract_basic_info_from_parsed(self, parsed_data: Dict[str, Any]) -> Dict[str, str]:
        """从解析后的数据中提取基本信息"""
        print(f"\n📝 开始提取基本信息...")
        
        # 查找包含 table_name 或 file_name 的表格数据
        for key, value in parsed_data.items():
            if isinstance(value, list) and len(value) >= 2:
                # 第一个元素通常是表头
                headers = value[0] if isinstance(value[0], list) else []
                
                # 检查是否包含目标字段
                if 'table_name' in headers or 'file_name' in headers:
                    # 第二行是数据行
                    data_row = value[1] if isinstance(value[1], list) else []
                    
                    result = {}
                    for idx, header in enumerate(headers):
                        if idx < len(data_row):
                            val = data_row[idx]
                            if header == 'file_name':
                                result['file_name'] = val
                            elif header == 'table_name':
                                result['target_table_name'] = val
                            elif header == 'table_desc':
                                result['target_table_desc'] = val
                            elif header == 'warehouse_layer':
                                result['warehouse_layer'] = val
                            elif header == 'updated_at':
                                result['updated_at'] = val
                            elif header == 'source_system':
                                result['source_system'] = val
                            elif header == 'version':
                                result['version'] = val
                    
                    if result:
                        print(f"   ✅ 提取到基本信息：{result}")
                        return result
        
        raise ValueError("Basic info not found in parsed data")


    def extract_field_list_from_parsed(self, parsed_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """从解析后的数据中提取字段清单"""
        print(f"\n📊 开始提取字段列表...")
        
        fields = []
        
        # 查找"字段清单"章节
        for key, value in parsed_data.items():
            if '字段清单' in key and isinstance(value, list) and len(value) >= 2:
                print(f"   ✅ 找到字段清单章节：{key}")
                
                # 第一个元素是表头
                headers = value[0] if isinstance(value[0], list) else []
                print(f"   📋 表头列数：{len(headers)}")
                
                # 后续元素是数据行
                data_rows = value[1:]
                
                for row in data_rows:
                    if isinstance(row, list) and len(row) == len(headers):
                        field_dict = {}
                        for idx, header in enumerate(headers):
                            if idx < len(row):
                                field_dict[header] = str(row[idx]).strip() if row[idx] else ''
                        
                        if field_dict:
                            fields.append(field_dict)
                
                print(f"   ✅ 共提取 {len(fields)} 个字段\n")
                break
        
        return fields


    def parse_array(self, value: str) -> List[str]:
        """解析数组字符串"""
        if not value or value.strip() in ['[]', '']:
            return []

        try:
            # 尝试解析 JSON 数组
            return ast.literal_eval(value)
        except:
            # 处理简单逗号分隔的字符串
            return [item.strip().strip('"') for item in value.split(',') if item.strip() != '']


    def parse_joins(self, value: str) -> List[Dict[str, str]]:
        """解析 JOIN 条件字符串"""
        if not value or value.strip() in ['[]', '']:
            return []

        try:
            # 尝试解析 JSON 数组
            joins = ast.literal_eval(value)
            # 确保是字典列表
            return [join for join in joins if isinstance(join, dict)]
        except:
            return []


    def parse_boolean(self, value: str) -> bool:
        """解析布尔值字符串"""
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() == 'true'


    def create_dimension_dict(self, fields: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """从字段列表创建维度字典 (以 field_name_en 为 key)"""
        dimensions = {}
        dimension_count = 0
        metric_count = 0
        other_count = 0
        
        print(f"\n📊 开始创建维度字典...")
        print(f"   输入字段总数：{len(fields)}")
        
        for field in fields:
            field_type = field.get('field_type', '')
            field_name_en = field.get('field_name_en', '')
            
            if field_type != 'dimension':
                if field_type == 'metric':
                    metric_count += 1
                else:
                    other_count += 1
                continue
            
            dimension_count += 1
            dimensions[field_name_en] = {
                "dim_id": field.get('field_id', ''),
                "dim_name": field.get('field_name', ''),
                "dim_column": field_name_en,
                "data_type": field.get('data_type', ''),
                "calculation_type": field.get('calculation_type', ''),
                "formula": field.get('formula', ''),
                "source_tables": self.parse_array(field.get('source_tables', '[]')),
                "source_fields": self.parse_array(field.get('source_fields', '[]')),
                "filters": field.get('filters', ''),
                "joins": field.get('joins', ''),
                "business_description": field.get('business_description', ''),
                "is_cross_table": self.parse_boolean(field.get('is_cross_table', 'false')),
                "support_drill_down": self.parse_boolean(field.get('support_drill_down', 'false')),
                "drill_down_fields": self.parse_array(field.get('drill_down_fields', '[]')),
                "validation_status": field.get('validation_status', '')
            }
        
        print(f"   - 指标字段：{metric_count} 个")
        print(f"   - 维度字段：{dimension_count} 个")
        print(f"   - 其他字段：{other_count} 个")
        print(f"✅ 维度字典创建完成，共 {len(dimensions)} 条\n")
        
        return dimensions
    
    
    def create_metric_dict(self, fields: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """从字段列表创建指标字典 (以 metric_name_en 为 key)"""
        metrics = {}
        for field in fields:
            if field['field_type'] != 'metric':
                continue

            metric_name_en = field['field_name_en']
            metrics[metric_name_en] = self.convert_field_to_metric(field)

        return metrics


    def convert_field_to_metric(self, field: Dict[str, Any]) -> Dict[str, Any]:
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


    def build_dependencies(self, field: Dict[str, Any]) -> List[Dict[str, List[str]]]:
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


    def build_joins(self, joins: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """构建 JOIN 条件列表"""
        return [
            {"condition": join['condition'], "type": join['type']}
            for join in joins
            if 'condition' in join and 'type' in join
        ]


    def infer_business_domain(self, field: Dict[str, Any]) -> str:
        """推断业务域"""
        text = f"{field['field_name']} {field['business_description']}"

        if "发情" in text or "配种" in text or "分娩" in text or "断奶" in text:
            return "繁殖"
        if "死亡" in text or "淘汰" in text or "健康" in text:
            return "养殖"
        if "批次" in text or "计划" in text:
            return "生产"

        return "待确认"


    def create_json_result(self, basic_info: Dict[str, str], metrics: Dict[str, Dict[str, Any]], dimensions: Dict[str, Dict[str, Any]], fields: List[Dict[str, Any]]) -> Dict[str, Any]:
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
                "metrics": metrics,
                "dimensions": dimensions,  # 新增：维度字段列表
                "fields": fields  # 保留：所有字段信息（包括维度和指标）
            }
        }


    def parse_single_md_file(self, markdown_file: str) -> Dict[str, Any]:
        """解析单个 Markdown 文件并返回 JSON 结果 - 使用 markdown-to-data"""
        print(f"markdown_file:{markdown_file}")
            
        # 1. 读取 Markdown 文件
        markdown_content = self.parse_markdown_file(markdown_file)
    
        # 2. 使用 markdown-to-data 解析整个文档
        print(f"\n🔧 使用 markdown-to-data 解析...")
        parsed_data = md_to_dict(markdown_content)
        print(f"   ✅ 解析完成，顶层 keys: {list(parsed_data.keys())}")
    
        # 3. 从解析后的数据中提取基本信息
        basic_info = self.extract_basic_info_from_parsed(parsed_data)
    
        # 4. 从解析后的数据中提取字段清单
        fields = self.extract_field_list_from_parsed(parsed_data)
    
        # 5. 创建指标字典
        metrics = self.create_metric_dict(fields)
        
        # 6. 创建维度字典
        dimensions = self.create_dimension_dict(fields)
    
        # 7. 创建最终 JSON（包含 metrics, dimensions, fields）
        result = self.create_json_result(basic_info, metrics, dimensions, fields)
    
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
    parse = ParseMDToJsonWithData()
    res = parse.parse_md_to_metric_metadata_list(table_name="yz_datawarehouse.ads.ads_anc_idx_female_wean_info")
    print(res)
