"""
创建 sql_parse_success_log 表

Revision ID: 072
Revises: 071
Create Date: 2026-04-16 10:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '072'
down_revision = '071'
branch_labels = None
depends_on = None


def upgrade():
    """创建 SQL 解析成功记录表"""
    print("创建 sql_parse_success_log 表...")
    
    # 创建表
    op.create_table(
        'sql_parse_success_log',
        sa.Column('id', sa.Integer(), sa.Identity(always=True), primary_key=True, comment='主键ID'),
        sa.Column('file_path', sa.String(length=500), nullable=False, unique=True, comment='SQL文件完整路径（唯一）'),
        sa.Column('file_name', sa.String(length=200), nullable=False, comment='SQL文件名称'),
        sa.Column('layer_type', sa.String(length=20), nullable=False, comment='数据层级类型，如DIM/DWD/METRIC'),
        sa.Column('target_table', sa.String(length=200), nullable=True, comment='目标表名'),
        sa.Column('table_stats', sa.Text(), nullable=True, comment='各表写入统计信息（JSON格式）'),
        sa.Column('parse_time', sa.DateTime(), server_default=sa.text('NOW()'), comment='SQL解析执行时间'),
        sa.Column('processing_duration', sa.Float(), nullable=True, comment='处理耗时（秒）'),
        sa.Column('create_time', sa.DateTime(), server_default=sa.text('NOW()'), comment='记录创建时间'),
        sa.Column('modify_time', sa.DateTime(), server_default=sa.text('NOW()'), comment='记录最后更新时间'),
    )
    
    # 创建索引
    op.create_index('idx_sql_parse_success_file_path', 'sql_parse_success_log', ['file_path'])
    op.create_index('idx_sql_parse_success_layer_type', 'sql_parse_success_log', ['layer_type'])
    
    print("✅ sql_parse_success_log 表创建成功")


def downgrade():
    """删除 SQL 解析成功记录表"""
    print("删除 sql_parse_success_log 表...")
    
    # 删除索引
    op.drop_index('idx_sql_parse_success_layer_type', table_name='sql_parse_success_log')
    op.drop_index('idx_sql_parse_success_file_path', table_name='sql_parse_success_log')
    
    # 删除表
    op.drop_table('sql_parse_success_log')
    
    print("✅ sql_parse_success_log 表已删除")
