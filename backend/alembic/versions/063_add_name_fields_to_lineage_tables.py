"""add name fields to table_lineage and field_lineage

Revision ID: 063
Revises: 062
Create Date: 2026-04-14

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '063'
down_revision = '062'
branch_labels = None
depends_on = None


def upgrade():
    """
    为 table_lineage 和 field_lineage 表添加中文名称字段
    
    table_lineage 新增：
    - source_table_name: 源表中文名
    - target_table_name: 目标表中文名
    
    field_lineage 新增：
    - source_table_name: 源表中文名
    - target_table_name: 目标表中文名
    - source_field_name: 源字段中文名
    - target_field_name: 目标字段中文名
    """
    
    # table_lineage 表新增字段
    op.add_column('table_lineage', sa.Column('source_table_name', sa.String(255), nullable=True))
    op.add_column('table_lineage', sa.Column('target_table_name', sa.String(255), nullable=True))
    
    print("✅ 已为 table_lineage 表添加字段: source_table_name, target_table_name")
    
    # field_lineage 表新增字段
    op.add_column('field_lineage', sa.Column('source_table_name', sa.String(255), nullable=True))
    op.add_column('field_lineage', sa.Column('target_table_name', sa.String(255), nullable=True))
    op.add_column('field_lineage', sa.Column('source_field_name', sa.String(255), nullable=True))
    op.add_column('field_lineage', sa.Column('target_field_name', sa.String(255), nullable=True))
    
    print("✅ 已为 field_lineage 表添加字段: source_table_name, target_table_name, source_field_name, target_field_name")


def downgrade():
    """删除新增的字段"""
    
    # 删除 field_lineage 表的字段
    op.drop_column('field_lineage', 'target_field_name')
    op.drop_column('field_lineage', 'source_field_name')
    op.drop_column('field_lineage', 'target_table_name')
    op.drop_column('field_lineage', 'source_table_name')
    
    print("✅ 已删除 field_lineage 表的名称字段")
    
    # 删除 table_lineage 表的字段
    op.drop_column('table_lineage', 'target_table_name')
    op.drop_column('table_lineage', 'source_table_name')
    
    print("✅ 已删除 table_lineage 表的名称字段")
