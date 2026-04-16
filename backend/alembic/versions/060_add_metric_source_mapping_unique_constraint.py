"""add metric_source_mapping unique constraint

Revision ID: 060
Revises: 059
Create Date: 2026-04-14 11:25:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '060'
down_revision = '059'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """添加 metric_source_mapping 唯一约束"""
    # 检查索引是否已存在
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    indexes = inspector.get_indexes('metric_source_mapping')
    
    index_names = [idx['name'] for idx in indexes]
    
    if 'idx_metric_source_unique' not in index_names:
        # 创建唯一索引：metric_id + db_table + metric_column
        op.create_index(
            'idx_metric_source_unique',
            'metric_source_mapping',
            ['metric_id', 'db_table', 'metric_column'],
            unique=True
        )
        print("✅ 成功创建 metric_source_mapping 唯一索引 (metric_id, db_table, metric_column)")
    else:
        print("⚠️  metric_source_mapping 唯一索引已存在，跳过")


def downgrade() -> None:
    """删除 metric_source_mapping 唯一约束"""
    op.drop_index('idx_metric_source_unique', table_name='metric_source_mapping')
    print("✅ 已删除 metric_source_mapping 唯一索引")
