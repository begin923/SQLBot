"""
重命名 metric_source_mapping 表的 ID 前缀从 MAP 改为 S

Revision ID: 071
Revises: 070
Create Date: 2026-04-15 18:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = '071'
down_revision = '070'
branch_labels = None
depends_on = None


def upgrade():
    """执行 ID 前缀修改"""
    print("处理 metric_source_mapping 表的 ID 前缀...")
    
    # 第一步：将所有 MAP 前缀的 ID 改为临时前缀 TMP
    op.execute("UPDATE metric_source_mapping SET id = 'TMP' || SUBSTRING(id FROM 4) WHERE id LIKE 'MAP%'")
    
    # 第二步：将临时前缀改为 S 前缀
    op.execute("UPDATE metric_source_mapping SET id = 'S' || SUBSTRING(id FROM 4) WHERE id LIKE 'TMP%'")
    
    # 更新注释
    op.execute("COMMENT ON COLUMN metric_source_mapping.id IS '映射唯一ID，格式：S+6位数字（如S000001），自增'")
    
    print("✅ metric_source_mapping 表 ID 前缀修改完成 (MAP -> S)")


def downgrade():
    """回滚修改"""
    print("回滚 metric_source_mapping 表的 ID 前缀...")
    
    # 第一步：将所有 S 前缀的 ID 改为临时前缀 TMP
    op.execute("UPDATE metric_source_mapping SET id = 'TMP' || SUBSTRING(id FROM 2) WHERE id LIKE 'S%'")
    
    # 第二步：将临时前缀改回 MAP 前缀
    op.execute("UPDATE metric_source_mapping SET id = 'MAP' || SUBSTRING(id FROM 4) WHERE id LIKE 'TMP%'")
    
    # 恢复注释
    op.execute("COMMENT ON COLUMN metric_source_mapping.id IS '映射唯一ID，格式：MAP+6位数字（如MAP000001），自增'")
    
    print("✅ metric_source_mapping 表 ID 前缀回滚完成 (S -> MAP)")
