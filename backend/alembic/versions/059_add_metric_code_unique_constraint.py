"""add metric_code unique constraint

Revision ID: 059
Revises: 058
Create Date: 2026-04-14 11:15:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '059'
down_revision = '058'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """添加 metric_code 唯一约束"""
    # 检查索引是否已存在
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    indexes = inspector.get_indexes('metric_definition')
    
    index_names = [idx['name'] for idx in indexes]
    
    if 'idx_metric_code_unique' not in index_names:
        # 创建唯一索引
        op.create_index(
            'idx_metric_code_unique',
            'metric_definition',
            ['metric_code'],
            unique=True
        )
        print("✅ 成功创建 metric_code 唯一索引")
    else:
        print("⚠️  metric_code 唯一索引已存在，跳过")


def downgrade() -> None:
    """删除 metric_code 唯一约束"""
    op.drop_index('idx_metric_code_unique', table_name='metric_definition')
    print("✅ 已删除 metric_code 唯一索引")
