"""change metric_dim_rel id to varchar(50)

Revision ID: 066
Revises: 065
Create Date: 2026-04-13

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '066'
down_revision = '065'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    修改 metric_dim_rel 表结构：
    1. 删除原有的 BIGINT 自增主键 id
    2. 添加 VARCHAR(50) 类型的 id 字段作为主键
    """
    
    # 1. 删除原有主键约束和 id 字段
    op.execute("ALTER TABLE metric_dim_rel DROP CONSTRAINT IF EXISTS metric_dim_rel_pkey")
    op.execute("ALTER TABLE metric_dim_rel DROP COLUMN IF EXISTS id")
    
    # 2. 添加新的 id 字段（VARCHAR(50)，允许 NULL）
    op.execute("""
        ALTER TABLE metric_dim_rel 
        ADD COLUMN id VARCHAR(50)
    """)
    
    # 3. 为现有数据生成默认值
    op.execute("""
        UPDATE metric_dim_rel m
        SET id = sub.new_id
        FROM (
            SELECT ctid, 'MD' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY create_time) AS TEXT), 7, '0') as new_id
            FROM metric_dim_rel
            WHERE id IS NULL
        ) sub
        WHERE m.ctid = sub.ctid
    """)
    
    # 4. 设置为主键
    op.execute("""
        ALTER TABLE metric_dim_rel 
        ALTER COLUMN id SET NOT NULL,
        ADD PRIMARY KEY (id)
    """)
    
    # 5. 添加注释
    op.execute("""
        COMMENT ON COLUMN metric_dim_rel.id IS '主键ID，格式：MD+7位数字（如MD0000001），自增'
    """)


def downgrade() -> None:
    """回滚修改"""
    
    # 1. 删除新的 id 字段
    op.execute("ALTER TABLE metric_dim_rel DROP CONSTRAINT IF EXISTS metric_dim_rel_pkey")
    op.execute("ALTER TABLE metric_dim_rel DROP COLUMN IF EXISTS id")
    
    # 2. 恢复原来的 BIGINT 自增主键
    op.execute("""
        ALTER TABLE metric_dim_rel 
        ADD COLUMN id BIGSERIAL PRIMARY KEY
    """)
    
    # 3. 添加注释
    op.execute("""
        COMMENT ON COLUMN metric_dim_rel.id IS '自增主键'
    """)
