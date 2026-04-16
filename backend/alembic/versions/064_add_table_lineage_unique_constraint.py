"""add unique constraint to table_lineage

Revision ID: 064
Revises: 063
Create Date: 2026-04-14

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '064'
down_revision = '063'
branch_labels = None
depends_on = None


def upgrade():
    """
    为 table_lineage 表添加唯一约束
    
    唯一键：(source_table, target_table)
    确保相同的表血缘关系不会重复插入
    """
    
    # 添加唯一约束
    op.create_unique_constraint(
        'uk_source_target_table',
        'table_lineage',
        ['source_table', 'target_table']
    )
    
    print("✅ 已为 table_lineage 表添加唯一约束: uk_source_target_table (source_table, target_table)")


def downgrade():
    """删除唯一约束"""
    
    op.drop_constraint('uk_source_target_table', 'table_lineage', type_='unique')
    
    print("✅ 已删除 table_lineage 表的唯一约束: uk_source_target_table")
