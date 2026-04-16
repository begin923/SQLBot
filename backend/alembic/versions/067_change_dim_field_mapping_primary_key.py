"""change dim_field_mapping primary key to dim_id

Revision ID: 067
Revises: 066
Create Date: 2026-04-13

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '067'
down_revision = '066'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    修改 dim_field_mapping 表结构：
    1. 删除原有的 id 字段和主键
    2. 清理重复的 dim_id 数据（保留第一条）
    3. 将 dim_id 设为主键
    4. 添加唯一索引 (db_table, dim_field)
    """
    
    # 1. 删除 id 字段（如果存在）
    op.execute("ALTER TABLE dim_field_mapping DROP COLUMN IF EXISTS id")
    
    # 2. 删除原有的联合主键约束（如果存在）
    op.execute("ALTER TABLE dim_field_mapping DROP CONSTRAINT IF EXISTS dim_field_mapping_pkey")
    
    # 3. 清理重复的 dim_id 数据（保留 ctid 最小的那条）
    op.execute("""
        DELETE FROM dim_field_mapping a
        USING dim_field_mapping b
        WHERE a.dim_id = b.dim_id
        AND a.ctid > b.ctid
    """)
    
    # 4. 设置 dim_id 为主键
    op.execute("""
        ALTER TABLE dim_field_mapping 
        ADD PRIMARY KEY (dim_id)
    """)
    
    # 5. 添加唯一索引 (db_table, dim_field)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uk_db_table_dim_field 
        ON dim_field_mapping (db_table, dim_field)
    """)


def downgrade() -> None:
    """回滚修改"""
    
    # 1. 删除唯一索引
    op.execute("DROP INDEX IF EXISTS uk_db_table_dim_field")
    
    # 2. 删除主键
    op.execute("ALTER TABLE dim_field_mapping DROP CONSTRAINT IF EXISTS dim_field_mapping_pkey")
    
    # 3. 恢复原来的联合主键
    op.execute("""
        ALTER TABLE dim_field_mapping 
        ADD PRIMARY KEY (db_table, dim_field)
    """)
