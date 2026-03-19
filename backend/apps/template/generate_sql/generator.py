from typing import Union

from apps.db.constant import DB
from apps.template.template import get_base_template, get_sql_template as get_base_sql_template


def get_sql_template():
    """获取常规SQL生成的模板"""
    template = get_base_template()
    return template['template']['sql']


def get_static_sql_template():
    """获取静态SQL执行的模板"""
    template = get_base_template()
    return template['template']['static_sql']


def get_drill_down_template():
    """获取静态SQL执行的模板"""
    template = get_base_template()
    return template['template']['drill_down']


def get_view_details_template():
    """获取静态SQL执行的模板"""
    template = get_base_template()
    return template['template']['view_details']


def get_sql_example_template(db_type: Union[str, DB]):
    """获取特定数据库类型的SQL示例模板"""
    template = get_base_sql_template(db_type)
    return template['template']
