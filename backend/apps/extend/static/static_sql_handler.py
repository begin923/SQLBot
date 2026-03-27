"""
静态 SQL 执行处理器
负责识别和处理静态 SQL 执行模式
"""
import json
import re
from typing import List, Dict, Any, Set, Optional

from orjson import orjson

from apps.chat.curd.chat import save_sql

from apps.datasource.crud.datasource import chooseTables, getTablesByDs
from apps.datasource.models.datasource import CoreTable, CoreDatasource
from sqlalchemy.orm import sessionmaker
from common.core.db import engine
from sqlmodel import Session
from common.core.deps import Trans
from common.utils.utils import SQLBotLogUtil
from sqlalchemy import and_



class StaticSQLHandler:
    """静态 SQL 处理器类"""
    
    def __init__(self):
        """
        初始化静态 SQL 处理器
        """

    def check_static_sql_mode(self, question: str) -> str:
        """
        检查是否为静态 SQL 执行模式
        
        Returns:
            bool: 是否为静态 SQL 模式
        """
        if not question:
            SQLBotLogUtil.error("Invalid chat question")
            return None
            
        sql_pattern = r'#FIXED_SQL_START#([\s\S]*?)#FIXED_SQL_END#'
        match = re.search(sql_pattern, question, re.DOTALL | re.IGNORECASE)

        if match:
            extracted_content = match.group(1).strip()
            # 尝试解析 JSON 格式的 SQL
            sql_data = json.loads(extracted_content, strict=False)
            base_sql = sql_data.get('sql', '')
            in_parm = sql_data.get('in_parm', {})

            # 如果有参数，则进行参数替换
            if in_parm and isinstance(in_parm, dict):
                provided_sql = self.replace_parameters(base_sql, in_parm)
                SQLBotLogUtil.info(f"Detected static SQL mode with JSON format and parameters, original SQL: {base_sql}, parameters: {in_parm}, final SQL: {provided_sql}")
                return provided_sql

            # 没有参数，直接使用 sql 字段的内容
            SQLBotLogUtil.info(f"Detected static SQL mode with JSON format, SQL: {base_sql}")
            return base_sql
    
    def replace_parameters(self, sql_template: str, parameters: dict) -> str:
        """
        替换 SQL 模板中的参数
        
        Args:
            sql_template: SQL 模板字符串，包含${paramName}格式的占位符
            parameters: 参数字典 {"paramName": "paramValue"}
            
        Returns:
            替换参数后的 SQL 字符串
        """
        if not sql_template or not parameters:
            return sql_template

        result_sql = sql_template

        # 直接替换每个参数，保持原始 SQL 意图
        # SQL 模板中已经根据字段类型添加了适当的引号和 NULL 处理
        for param_name, param_value in parameters.items():
            placeholder = f"${{{param_name}}}"
            # 直接转换为字符串进行替换，不进行任何额外处理
            replacement = str(param_value)

            result_sql = result_sql.replace(placeholder, replacement)
            SQLBotLogUtil.debug(f"Replaced parameter {placeholder} with {replacement}")

        return result_sql
    
    def extract_tables_from_sql(self, sql_query: str) -> List[str]:
        """
        从 SQL 查询语句中提取涉及的完整表名（包含 schema）
        使用 SQL 语法树解析器 (sqlglot) 进行精确解析
        注意：只提取实际的物理表名，排除 CTE 别名、单独的 schema 名称等
        
        Args:
            sql_query: SQL 查询语句
            
        Returns:
            List[str]: 完整表名列表（格式：schema.table_name）
            例如：['yz_datawarehouse_dim.dim_fpf_spc_admin_detail', 'yz_datawarehouse_dws.dws_fpf_farrow_detail']
        """
        if not sql_query:
            return []
        
        tables: Set[str] = set()
        cte_aliases: Set[str] = set()
        
        try:
            # 使用 sqlglot 进行精确的 SQL 语法树解析
            import sqlglot
            from sqlglot import exp
            
            # 解析 SQL 语句
            parsed_statements = sqlglot.parse(sql_query, dialect=None)
            
            for statement in parsed_statements:
                if statement is None:
                    continue
                
                # 首先收集所有 CTE 别名
                for cte in statement.find_all(exp.CTE):
                    if hasattr(cte, 'alias') and cte.alias:
                        alias_name = str(cte.alias.this) if hasattr(cte.alias, 'this') else str(cte.alias)
                        cte_aliases.add(alias_name.lower())
                
                # 递归遍历语法树，提取所有表名引用
                for table_exp in statement.find_all(exp.Table):
                    # 获取表名的各个部分
                    catalog = table_exp.args.get('catalog')
                    db = table_exp.args.get('db')
                    table = table_exp.args.get('this')
                    
                    if table:
                        table_name = str(table.this) if hasattr(table, 'this') else str(table)
                        table_name_lower = table_name.lower()
                        
                        # 排除 CTE 别名
                        if table_name_lower in cte_aliases:
                            continue
                        
                        # 关键修复：只有当同时有 db 和 table 组件时才构成有效表名
                        # 避免单独的 schema 名称被提取
                        if db:
                            db_name = str(db.this) if hasattr(db, 'this') else str(db)
                            db_name_lower = db_name.lower()
                            
                            # 验证 db 和 table 都是有效的标识符
                            if (self.is_valid_identifier(db_name_lower) and
                                self.is_valid_identifier(table_name_lower)):
                                full_table_name = f"{db_name_lower}.{table_name_lower}"
                                tables.add(full_table_name)
                                SQLBotLogUtil.debug(f"提取完整表名：{full_table_name}")
                        # 简单表名（没有 schema 前缀）
                        elif self.is_valid_identifier(table_name_lower):
                            tables.add(table_name_lower)
                            SQLBotLogUtil.debug(f"提取简单表名：{table_name_lower}")
                            
        except Exception as e:
            SQLBotLogUtil.warning(f"sqlglot 解析失败，回退到正则表达式方法：{str(e)}")
            # 回退到正则表达式方法作为备用方案
            self.extract_tables_regex_backup(sql_query, tables)
        
        # 过滤掉可能的 schema 名称（单独出现的）
        filtered_tables = set()
        all_parts = set()
        
        # 收集所有表名的组成部分
        for table_name in tables:
            if '.' in table_name:
                parts = table_name.split('.')
                if len(parts) == 2:
                    schema_part, table_part = parts
                    all_parts.add(schema_part)
                    all_parts.add(table_part)
                    # 只有完整的 schema.table 格式才保留
                    filtered_tables.add(table_name)
            else:
                # 简单表名直接保留
                filtered_tables.add(table_name)
                all_parts.add(table_name)
        
        # 最终过滤：移除那些只是 schema 名称的项
        final_tables = set()
        for table_name in filtered_tables:
            if '.' in table_name:
                final_tables.add(table_name)
            else:
                # 对于简单表名，确保它不是某个完整表名的 schema 部分
                if table_name not in all_parts or any(table_name in t.split('.')[0] for t in filtered_tables if '.' in t):
                    final_tables.add(table_name)
        
        return sorted(list(final_tables))
    
    def is_valid_identifier(self, name: str) -> bool:
        """
        验证是否为有效的 SQL 标识符
        
        Args:
            name: 待验证的名称
            
        Returns:
            bool: 是否为有效标识符
        """
        if not name:
            return False
        
        # 排除关键字
        exclude_words = {
            'as', 'on', 'and', 'or', 'by', 'in', 'is', 'not', 'null', 'true', 'false',
            'where', 'group', 'order', 'having', 'limit', 'select', 'from', 'join',
            'inner', 'left', 'right', 'full', 'update', 'insert', 'into', 'delete',
            'set', 'values', 'case', 'when', 'then', 'else', 'end', 'u', 'd', 'a', 'b'
        }
        
        if name in exclude_words:
            return False
        
        # 排除数字
        if name.isdigit():
            return False
        
        # 排除函数调用等
        if any(char in name for char in '()'):
            return False
        
        # 必须以字母或下划线开头
        if not (name[0].isalpha() or name[0] == '_'):
            return False
        
        # 只能包含字母、数字、下划线
        return all(c.isalnum() or c == '_' for c in name)
    
    def extract_tables_regex_backup(self, sql_query: str, tables: Set[str]):
        """
        正则表达式方法作为备用方案
        
        Args:
            sql_query: SQL 查询语句
            tables: 表名集合（用于存储结果）
        """
        # 简单的正则匹配 FROM 和 JOIN 后面的表名
        patterns = [
            r'\bFROM\s+([^\s,(]+)',
            r'\bJOIN\s+([^\s,]+)',
            r'\bINTO\s+([^\s,]+)',
            r'\bUPDATE\s+([^\s,]+)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, sql_query, re.IGNORECASE)
            for match in matches:
                table_name = match.strip().strip('`').strip('"').strip("'").lower()
                if table_name and self.is_valid_identifier(table_name):
                    tables.add(table_name)

    def build_response_data(self, sql: str, tables: List[str]) -> Dict[str, Any]:
        """
        构建响应数据
        
        Args:
            sql: SQL 语句
            tables: 表名列表
            
        Returns:
            Dict: 响应数据
        """
        response_data = {
            "success": True,
            "sql": sql,
            "tables": tables,
            "chart-type": 'table'  # 默认图表类型
        }
        
        # 添加数据源信息
        if ds:
            response_data["datasource"] = {
                "id": ds.id,
                "name": ds.name,
                "type": ds.type,
                "description": ds.description
            }
        
        return response_data

    def add_table_to_ds(self, ds, table_name: str) -> bool:
        """
        添加表到当前数据源

        参考 exe_static_sql 函数的逻辑，使用项目原有接口:
        1. 如果表不存在则添加到数据源
        2. 如果已存在则跳过

        Args:
            table_name: 表名 (如："ads_sales_summary" 或 "schema.table_name")

        Returns:
            bool: 是否成功添加或已存在
        """
        if not ds or not table_name:
            SQLBotLogUtil.warning("No datasource or table name provided")
            return False

        try:
            # 获取数据库会话
            local_session_maker = sessionmaker(bind=engine, class_=Session)
            session = local_session_maker()

            # 创建事务对象
            trans = Trans()

            # 构造临时的 CoreDatasource 对象用于查询
            from apps.datasource.models.datasource import CoreDatasource
            temp_ds = CoreDatasource(
                id=ds.id,
                type=ds.type,
                configuration=ds.configuration
            )

            # 获取数据源中已有的所有表
            existing_tables = session.query(CoreTable).filter(
                CoreTable.ds_id == ds.id
            ).all()

            # 创建已有表名集合，用于快速查找
            existing_table_names = {table.table_name for table in existing_tables}

            # 获取所有表信息 (用于获取表注释)
            all_tables_info = getTablesByDs(session, temp_ds)
            table_comment_map = {}
            for table_info in all_tables_info:
                table_comment_map[table_info.tableName.lower()] = table_info.tableComment

            # 构造表对象列表
            table_objects = []

            # 先添加已有的表
            for existing_table in existing_tables:
                table_objects.append(existing_table)

            # 再添加新表 (如果不存在)
            if table_name not in existing_table_names:
                # 从映射中获取真实表注释
                table_comment = table_comment_map.get(table_name.lower(), "")

                table_obj = CoreTable(
                    ds_id=ds.id,
                    checked=True,
                    table_name=table_name,
                    table_comment=table_comment,
                    custom_comment=table_comment
                )
                table_objects.append(table_obj)
                SQLBotLogUtil.info(f"Adding new table: {table_name}")
            else:
                SQLBotLogUtil.info(f"Table {table_name} already exists")

            # 调用后端的 chooseTables 函数
            chooseTables(session, trans, ds.id, table_objects)

            # 强制提交事务确保数据持久化
            session.commit()

            # 验证表是否真正添加成功
            added_table = session.query(CoreTable).filter(
                and_(CoreTable.ds_id == ds.id,
                     CoreTable.table_name == table_name)
            ).first()

            if added_table:
                SQLBotLogUtil.info(f"Successfully added/verified table: {table_name}")
                session.close()
                return True
            else:
                SQLBotLogUtil.warning(f"Failed to verify table: {table_name}")
                session.close()
                return False

        except Exception as e:
            SQLBotLogUtil.error(f"Error adding table: {str(e)}")
            try:
                session.close()
            except:
                pass
            return False

    def exe_static_sql(self, _session, in_chat:bool , ds:CoreDatasource , provided_sql:str,record_id:Optional[int]):
        # 直接执行模式：直接使用用户提供的 SQL，跳过大模型调用
        full_sql_text = provided_sql  # 设置 full_sql_text 为用户提供的 SQL
        SQLBotLogUtil.info(f"Direct execute mode: using provided SQL directly: {provided_sql}")
        sql = provided_sql
        save_sql(session=_session, sql=sql, record_id=record_id)
        # self.chat_question.sql = sql
        # 从用户提供的 SQL 中提取表名
        tables = self.extract_tables_from_sql(sql)
        SQLBotLogUtil.info(f"Extracted tables from provided SQL: {tables}")
        # 如果有数据源且提取到表名，则调用标准接口添加表到指定数据源中
        for table in tables:
            self.add_table_to_ds(ds,table)
        # 直接返回结果，不发送响应，由调用方处理
        return full_sql_text, sql
