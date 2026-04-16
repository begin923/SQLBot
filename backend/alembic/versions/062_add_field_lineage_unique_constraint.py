"""add unique constraint to field_lineage

Revision ID: 062
Revises: 061
Create Date: 2026-04-14

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '062'
down_revision = '061'
branch_labels = None
depends_on = None


def upgrade():
    """
    为 field_lineage 表添加唯一索引
    
    唯一索引字段：(source_table, source_field, target_table, target_field)
    作用：确保同一源字段到目标字段的映射只有一条记录
    """
    
    # 创建唯一索引
    op.create_unique_constraint(
        'uq_field_lineage_source_target',
        'field_lineage',
        ['source_table', 'source_field', 'target_table', 'target_field']
    )
    
    print("✅ 已为 field_lineage 表添加唯一索引: uq_field_lineage_source_target")
    print("   索引字段: (source_table, source_field, target_table, target_field)")


def downgrade():
    """删除唯一索引"""
    
    op.drop_constraint('uq_field_lineage_source_target', 'field_lineage', type_='unique')
    
    print("✅ 已删除 field_lineage 表的唯一索引: uq_field_lineage_source_target")
