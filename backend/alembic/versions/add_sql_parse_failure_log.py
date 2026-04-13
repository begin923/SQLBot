"""
添加 SQL 解析失败日志表

Revision ID: add_sql_parse_failure_log
Revises: 058_create_metrics2_tables
Create Date: 2026-04-13

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'add_sql_parse_failure_log'
down_revision: Union[str, None] = '058'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """升级：创建 sql_parse_failure_log 表"""
    op.create_table(
        'sql_parse_failure_log',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True, comment='主键ID'),
        sa.Column('file_path', sa.String(length=500), nullable=False, comment='SQL文件完整路径'),
        sa.Column('file_name', sa.String(length=200), nullable=False, comment='SQL文件名称'),
        sa.Column('layer_type', sa.String(length=20), nullable=True, comment='数据层级类型，如DIM/DWD/METRIC/AUTO'),
        sa.Column('failure_reason', sa.Text(), nullable=False, comment='解析失败具体原因描述'),
        sa.Column('error_type', sa.String(length=50), nullable=True, comment='错误类型枚举，如SELECT_STAR/JSON_PARSE/AI_ERROR'),
        sa.Column('sql_content', sa.Text(), nullable=True, comment='异常SQL原文内容，可为空'),
        sa.Column('matched_pattern', sa.String(length=100), nullable=True, comment='匹配到的异常模式，如a.*'),
        sa.Column('parse_time', sa.DateTime(), nullable=False, server_default=sa.func.now(), comment='SQL解析执行时间'),
        sa.Column('is_resolved', sa.Boolean(), nullable=False, server_default=sa.false(), comment='是否已处理解决，默认未解决'),
        sa.Column('resolve_time', sa.DateTime(), nullable=True, comment='问题解决处理时间'),
        sa.Column('retry_count', sa.Integer(), nullable=False, server_default=sa.text('0'), comment='自动重试执行次数'),
        sa.Column('create_time', sa.DateTime(), nullable=False, server_default=sa.func.now(), comment='记录创建时间'),
        sa.Column('modify_time', sa.DateTime(), nullable=False, server_default=sa.func.now(), onupdate=sa.func.now(), comment='记录最后更新时间')
    )
    
    # 创建索引以优化查询性能
    op.create_index('idx_file_path', 'sql_parse_failure_log', ['file_path'])
    op.create_index('idx_error_type', 'sql_parse_failure_log', ['error_type'])
    op.create_index('idx_is_resolved', 'sql_parse_failure_log', ['is_resolved'])
    op.create_index('idx_parse_time', 'sql_parse_failure_log', ['parse_time'])
    
    # 创建唯一索引：file_path + file_name 组合唯一
    op.create_unique_constraint('uq_file_path_name', 'sql_parse_failure_log', ['file_path', 'file_name'])


def downgrade() -> None:
    """降级：删除 sql_parse_failure_log 表"""
    op.drop_constraint('uq_file_path_name', 'sql_parse_failure_log', type_='unique')
    op.drop_index('idx_parse_time', table_name='sql_parse_failure_log')
    op.drop_index('idx_is_resolved', table_name='sql_parse_failure_log')
    op.drop_index('idx_error_type', table_name='sql_parse_failure_log')
    op.drop_index('idx_file_path', table_name='sql_parse_failure_log')
    op.drop_table('sql_parse_failure_log')
