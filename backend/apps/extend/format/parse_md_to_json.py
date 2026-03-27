#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Markdown to JSON Converter
Converts structured Markdown documentation to standardized JSON format
"""

import json
import re
import sys
import ast
from typing import Dict, List, Any, Optional


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
                                elif header == 'target_table_id':
                                    result['target_table_id'] = cells[idx]
                                elif header == 'target_table_name':
                                    result['target_table_name'] = cells[idx]
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
        """创建最终的JSON结果"""
        return {
            "success": True,
            "data": {
                "meta": {
                    "version": basic_info.get('version', '1.0'),
                    "updated_at": basic_info.get('updated_at', '2026-03-18T09:00:00Z'),
                    "source_system": "datawarehouse"
                },
                "target": {
                    "table_id": basic_info['target_table_id'],
                    "table_name": "母猪批次生产指标表",
                    "file_path": f"{basic_info['warehouse_layer']}/{basic_info['file_name']}"
                },
                "metrics": metrics
            }
        }


    def parse_md_to_json(self, markdown_path: str = None,table_name: str = None):
        """主函数：处理整个转换流程"""
        # 优先使用传入的参数，如果没有则使用默认路径
        # 默认路径
        markdown_file = r"D:\codes\AIDataEasy\data_governance_agent\sql_to_md\metric_blood\yz_datawarehouse_ads.ads_pig_feed_sum_month.md"
    
        try:
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
            print(json.dumps(result, indent=2, ensure_ascii=False))

            return result

        except Exception as e:
            print(f"❌ Error: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }


if __name__ == "__main__":
    parse_md_to_json = ParseMDToJson()
    parse_md_to_json.parse_md_to_json("yz_datawarehouse_ads.ads_pig_feed_sum_month")