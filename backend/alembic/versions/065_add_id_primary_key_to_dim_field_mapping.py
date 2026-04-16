"""add id primary key to dim_field_mapping

Revision ID: 065
Revises: add_sql_parse_failure_log
Create Date: 2026-04-13

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '065'
down_revision = 'add_sql_parse_failure_log'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    修改 dim_field_mapping 表结构：
    1. 删除原有的联合主键 (db_table, dim_field)
    2. 添加 id 字段作为自增主键
    3. 添加唯一索引 (db_table, dim_field)
    """
    
    # 1. 删除原有主键约束
    op.execute("ALTER TABLE dim_field_mapping DROP CONSTRAINT IF EXISTS dim_field_mapping_pkey")
    
    # 2. 添加 id 字段（自增主键）
    op.execute("""
        ALTER TABLE dim_field_mapping 
        ADD COLUMN IF NOT EXISTS id SERIAL PRIMARY KEY
    """)
    
    # 3. 添加唯一索引 (db_table, dim_field)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uk_db_table_dim_field 
        ON dim_field_mapping (db_table, dim_field)
    """)


def downgrade() -> None:
    """回滚操作"""
    
    # 1. 删除唯一索引
    op.execute("DROP INDEX IF EXISTS uk_db_table_dim_field")
    
    # 2. 删除 id 字段
    op.execute("ALTER TABLE dim_field_mapping DROP COLUMN IF EXISTS id")
    
    # 3. 恢复联合主键
    op.execute("""
        ALTER TABLE dim_field_mapping 
        ADD CONSTRAINT dim_field_mapping_pkey 
        PRIMARY KEY (db_table, dim_field)
    """)
