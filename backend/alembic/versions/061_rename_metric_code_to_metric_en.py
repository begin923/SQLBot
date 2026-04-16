"""rename metric_code to metric_en in metric_definition

Revision ID: 061
Revises: 060
Create Date: 2026-04-14 15:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = '061'
down_revision = '060'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """将 metric_definition 表的 metric_code 字段重命名为 metric_en"""
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    
    # 检查字段是否存在
    columns = [col['name'] for col in inspector.get_columns('metric_definition')]
    
    if 'metric_code' in columns and 'metric_en' not in columns:
        # 重命名字段
        op.alter_column(
            'metric_definition',
            'metric_code',
            new_column_name='metric_en',
            existing_type=sa.String(64),
            existing_nullable=False,
            comment='指标英文编码（如mating_total_cnt），唯一，用于接口/BI调用'
        )
        
        # 重命名索引
        indexes = [idx['name'] for idx in inspector.get_indexes('metric_definition')]
        if 'idx_metric_code_unique' in indexes:
            op.drop_index('idx_metric_code_unique', table_name='metric_definition')
        
        # 创建新索引
        op.create_index(
            'idx_metric_en_unique',
            'metric_definition',
            ['metric_en'],
            unique=True
        )
        
        print("✅ 成功将 metric_code 重命名为 metric_en")
    elif 'metric_en' in columns:
        print("⚠️  metric_en 字段已存在，跳过")
    else:
        print("❌ metric_code 字段不存在，无法重命名")


def downgrade() -> None:
    """将 metric_definition 表的 metric_en 字段重命名为 metric_code"""
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    
    columns = [col['name'] for col in inspector.get_columns('metric_definition')]
    
    if 'metric_en' in columns and 'metric_code' not in columns:
        # 重命名字段
        op.alter_column(
            'metric_definition',
            'metric_en',
            new_column_name='metric_code',
            existing_type=sa.String(64),
            existing_nullable=False,
            comment='指标英文编码（如mating_total_cnt），唯一，用于接口/BI调用'
        )
        
        # 重命名索引
        indexes = [idx['name'] for idx in inspector.get_indexes('metric_definition')]
        if 'idx_metric_en_unique' in indexes:
            op.drop_index('idx_metric_en_unique', table_name='metric_definition')
        
        # 创建新索引
        op.create_index(
            'idx_metric_code_unique',
            'metric_definition',
            ['metric_code'],
            unique=True
        )
        
        print("✅ 成功将 metric_en 重命名为 metric_code")
    elif 'metric_code' in columns:
        print("⚠️  metric_code 字段已存在，跳过")
    else:
        print("❌ metric_en 字段不存在，无法重命名")
