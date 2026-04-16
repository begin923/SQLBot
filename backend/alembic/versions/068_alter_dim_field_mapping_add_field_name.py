"""
修改 dim_field_mapping 表结构
- dim_field 改为 field
- 新增 field_name 字段

Revision ID: 068
Revises: 067
Create Date: 2026-04-15 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '068'
down_revision = '067'
branch_labels = None
depends_on = None


def upgrade():
    """
    修改 dim_field_mapping 表结构：
    1. 删除唯一约束 uk_db_table_dim_field
    2. 重命名 dim_field 为 field
    3. 添加 field_name 字段
    4. 创建新的唯一约束 uk_db_table_field
    """
    
    # 1. 删除旧的唯一约束
    op.execute("ALTER TABLE dim_field_mapping DROP CONSTRAINT IF EXISTS uk_db_table_dim_field")
    
    # 2. 重命名列 dim_field -> field
    op.execute("ALTER TABLE dim_field_mapping RENAME COLUMN dim_field TO field")
    
    # 3. 添加 field_name 字段（默认值为空字符串）
    op.execute("""
        ALTER TABLE dim_field_mapping 
        ADD COLUMN field_name VARCHAR(128) NOT NULL DEFAULT ''
    """)
    
    # 4. 创建新的唯一约束 (db_table, field)
    op.execute("""
        CREATE UNIQUE INDEX uk_db_table_field 
        ON dim_field_mapping (db_table, field)
    """)


def downgrade():
    """回滚修改"""
    
    # 1. 删除新的唯一索引
    op.execute("DROP INDEX IF EXISTS uk_db_table_field")
    
    # 2. 删除 field_name 字段
    op.execute("ALTER TABLE dim_field_mapping DROP COLUMN IF EXISTS field_name")
    
    # 3. 重命名列 field -> dim_field
    op.execute("ALTER TABLE dim_field_mapping RENAME COLUMN field TO dim_field")
    
    # 4. 恢复原来的唯一约束
    op.execute("""
        CREATE UNIQUE INDEX uk_db_table_dim_field 
        ON dim_field_mapping (db_table, dim_field)
    """)
