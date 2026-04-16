"""
修改 field_lineage 表的 formula 字段类型为 TEXT

Revision ID: 073
Revises: 072
Create Date: 2026-04-16 14:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '073'
down_revision = '072'
branch_labels = None
depends_on = None


def upgrade():
    """修改 formula 字段类型为 TEXT"""
    print("修改 field_lineage 表的 formula 字段类型...")
    
    # 将 formula 字段从 VARCHAR(500) 改为 TEXT
    op.alter_column(
        'field_lineage',
        'formula',
        type_=sa.Text(),
        existing_type=sa.String(length=500),
        comment='字段转换公式（支持长SQL表达式）'
    )
    
    print("✅ field_lineage.formula 字段类型修改完成 (VARCHAR(500) -> TEXT)")


def downgrade():
    """回滚修改"""
    print("回滚 field_lineage 表的 formula 字段类型...")
    
    # ⚠️ 注意：回滚时如果已有数据超过500字符会被截断
    op.alter_column(
        'field_lineage',
        'formula',
        type_=sa.String(length=500),
        existing_type=sa.Text(),
        comment='字段转换公式'
    )
    
    print("✅ field_lineage.formula 字段类型回滚完成 (TEXT -> VARCHAR(500))")
