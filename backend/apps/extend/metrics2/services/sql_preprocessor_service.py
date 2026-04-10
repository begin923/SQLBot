from typing import List, Dict, Any, Optional
import re
import logging

logger = logging.getLogger("SQLPreprocessorService")


class SQLPreprocessorService:
    """
    SQL预处理服务 - 防Token超限核心算法
    
    功能：
    1. 剔除注释、空行、SET语句
    2. 提取INSERT/SELECT核心片段
    3. 超长脚本切片处理
    """

    def __init__(self, max_token_limit: int = 8000):
        """
        初始化SQL预处理器
        
        Args:
            max_token_limit: 最大Token限制（默认8000）
        """
        self.max_token_limit = max_token_limit

    def preprocess_sql(self, sql_content: str) -> str:
        """
        预处理SQL内容
        
        Args:
            sql_content: 原始SQL内容
            
        Returns:
            预处理后的SQL内容
        """
        if not sql_content or not sql_content.strip():
            raise ValueError("SQL内容不能为空")

        # 1. 剔除SQL注释
        sql_content = self._remove_comments(sql_content)

        # 2. 剔除空行
        sql_content = self._remove_empty_lines(sql_content)

        # 3. 剔除SET语句
        sql_content = self._remove_set_statements(sql_content)

        # 4. 标准化空白字符
        sql_content = self._normalize_whitespace(sql_content)

        return sql_content.strip()

    def _remove_comments(self, sql: str) -> str:
        """
        剔除SQL注释
        
        Args:
            sql: SQL内容
            
        Returns:
            无注释的SQL
        """
        # 剔除单行注释 (-- 开头)
        sql = re.sub(r'--[^\n]*', '', sql)

        # 剔除多行注释 (/* ... */)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)

        return sql

    def _remove_empty_lines(self, sql: str) -> str:
        """
        剔除空行
        
        Args:
            sql: SQL内容
            
        Returns:
            无空行的SQL
        """
        lines = sql.split('\n')
        non_empty_lines = [line for line in lines if line.strip()]
        return '\n'.join(non_empty_lines)

    def _remove_set_statements(self, sql: str) -> str:
        """
        剔除SET语句
        
        Args:
            sql: SQL内容
            
        Returns:
            无SET语句的SQL
        """
        # 剔除SET语句（不区分大小写）
        sql = re.sub(r'^\s*SET\s+[^;]+;?\s*', '', sql, flags=re.IGNORECASE | re.MULTILINE)
        return sql

    def _normalize_whitespace(self, sql: str) -> str:
        """
        标准化空白字符
        
        Args:
            sql: SQL内容
            
        Returns:
            标准化后的SQL
        """
        # 将多个空格替换为单个空格
        sql = re.sub(r'\s+', ' ', sql)

        # 在关键字前后添加换行（可选，便于阅读）
        keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING',
                    'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'ON',
                    'INSERT INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE FROM']

        for keyword in keywords:
            sql = re.sub(r'\b' + keyword + r'\b', f'\n{keyword}', sql, flags=re.IGNORECASE)

        return sql.strip()

    def extract_core_statements(self, sql_content: str) -> List[str]:
        """
        提取核心SQL语句（INSERT/SELECT）
        
        Args:
            sql_content: SQL内容
            
        Returns:
            核心语句列表
        """
        # 预处理
        sql_content = self.preprocess_sql(sql_content)

        # 按分号分割语句
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]

        # 过滤出INSERT和SELECT语句
        core_statements = []
        for stmt in statements:
            if re.match(r'^\s*(INSERT|SELECT)', stmt, re.IGNORECASE):
                core_statements.append(stmt)

        return core_statements

    def split_long_sql(self, sql_content: str, max_chunk_size: int = None) -> List[str]:
        """
        将超长SQL切片
        
        Args:
            sql_content: SQL内容
            max_chunk_size: 最大切片大小（字符数）
            
        Returns:
            切片后的SQL列表
        """
        if max_chunk_size is None:
            max_chunk_size = self.max_token_limit * 4  # 假设1 token ≈ 4字符

        # 预处理
        sql_content = self.preprocess_sql(sql_content)

        # 如果SQL长度在限制内，直接返回
        if len(sql_content) <= max_chunk_size:
            return [sql_content]

        # 提取核心语句
        statements = self.extract_core_statements(sql_content)

        # 按语句分组切片
        chunks = []
        current_chunk = []
        current_size = 0

        for stmt in statements:
            stmt_size = len(stmt)

            # 如果单个语句就超过限制，强制拆分
            if stmt_size > max_chunk_size:
                # 如果当前chunk不为空，先保存
                if current_chunk:
                    chunks.append(';\n'.join(current_chunk))
                    current_chunk = []
                    current_size = 0

                # 强制拆分大语句（按FROM/JOIN/WHERE等关键字拆分）
                sub_chunks = self._split_single_statement(stmt, max_chunk_size)
                chunks.extend(sub_chunks)
            else:
                # 如果当前chunk + 新语句超过限制，保存当前chunk
                if current_size + stmt_size > max_chunk_size and current_chunk:
                    chunks.append(';\n'.join(current_chunk))
                    current_chunk = []
                    current_size = 0

                current_chunk.append(stmt)
                current_size += stmt_size

        # 保存最后一个chunk
        if current_chunk:
            chunks.append(';\n'.join(current_chunk))

        logger.info(f"SQL切片完成：原始长度={len(sql_content)}, 切片数量={len(chunks)}")

        return chunks

    def _split_single_statement(self, stmt: str, max_size: int) -> List[str]:
        """
        拆分单个超长语句
        
        Args:
            stmt: SQL语句
            max_size: 最大大小
            
        Returns:
            拆分后的语句列表
        """
        # 简单策略：按UNION拆分
        if 'UNION' in stmt.upper():
            parts = re.split(r'\bUNION\b', stmt, flags=re.IGNORECASE)
            chunks = []
            for i, part in enumerate(parts):
                if i > 0:
                    part = 'UNION ' + part
                chunks.append(part.strip())
            return chunks

        # 如果无法智能拆分，直接截断（警告）
        logger.warning(f"无法智能拆分超长语句，强制截断：长度={len(stmt)}")
        return [stmt[:max_size]]

    def get_sql_stats(self, sql_content: str) -> Dict[str, Any]:
        """
        获取SQL统计信息
        
        Args:
            sql_content: SQL内容
            
        Returns:
            统计信息字典
        """
        preprocessed = self.preprocess_sql(sql_content)
        statements = self.extract_core_statements(sql_content)

        return {
            'original_length': len(sql_content),
            'preprocessed_length': len(preprocessed),
            'statement_count': len(statements),
            'estimated_tokens': len(preprocessed) // 4,
            'exceeds_limit': len(preprocessed) > self.max_token_limit * 4
        }

    def validate_sql(self, sql_content: str) -> Dict[str, Any]:
        """
        验证SQL格式
        
        Args:
            sql_content: SQL内容
            
        Returns:
            验证结果
        """
        errors = []
        warnings = []

        # 检查是否为空
        if not sql_content or not sql_content.strip():
            errors.append("SQL内容为空")
            return {'valid': False, 'errors': errors, 'warnings': warnings}

        # 检查是否包含基本关键字
        if not re.search(r'\b(SELECT|INSERT|UPDATE|DELETE)\b', sql_content, re.IGNORECASE):
            warnings.append("SQL可能不包含标准DML语句")

        # 检查括号匹配
        if sql_content.count('(') != sql_content.count(')'):
            errors.append("括号不匹配")

        # 检查引号匹配
        if sql_content.count("'") % 2 != 0:
            errors.append("单引号不匹配")

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }
