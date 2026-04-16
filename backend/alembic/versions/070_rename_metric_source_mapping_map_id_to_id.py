"""
重命名 metric_source_mapping 表的 map_id 为 id

Revision ID: 070
Revises: 069
Create Date: 2026-04-15 16:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '070'
down_revision = '069'
branch_labels = None
depends_on = None


def upgrade():
    """执行字段重命名"""
    
    print("处理 metric_source_mapping 表...")
    
    # 重命名字段 map_id -> id
    op.execute("ALTER TABLE metric_source_mapping RENAME COLUMN map_id TO id")
    
    # 更新注释
    op.execute("COMMENT ON COLUMN metric_source_mapping.id IS '映射唯一ID，格式：MAP+6位数字（如MAP000001），自增'")
    
    print("✅ metric_source_mapping 表修改完成")


def downgrade():
    """回滚修改"""
    
    print("回滚 metric_source_mapping 表...")
    
    # 重命名字段 id -> map_id
    op.execute("ALTER TABLE metric_source_mapping RENAME COLUMN id TO map_id")
    
    # 恢复注释
    op.execute("COMMENT ON COLUMN metric_source_mapping.map_id IS '映射唯一ID，格式：MAP+3位数字（如MAP001），自增'")
    
    print("✅ metric_source_mapping 表回滚完成")
