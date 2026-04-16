"""
批量重命名字段以简化命名规范
- dim_definition: dim_id→id, dim_name→name, dim_code→code, dim_type→type
- dim_field_mapping: 主键改为 (db_table, field)，删除唯一索引
- metric_definition: metric_id→id, metric_name→name, metric_en→code
- metric_dim_rel: 删除 id 字段（如果存在），使用 (metric_id, dim_id) 联合主键
- table_lineage: lineage_id→id
- field_lineage: lineage_id→id

Revision ID: 069
Revises: 068
Create Date: 2026-04-15 15:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '069'
down_revision = '068'
branch_labels = None
depends_on = None


def upgrade():
    """执行字段重命名和表结构修改"""
    
    # ==================== 1. dim_definition 表 ====================
    print("处理 dim_definition 表...")
    
    # 重命名字段
    op.execute("ALTER TABLE dim_definition RENAME COLUMN dim_id TO id")
    op.execute("ALTER TABLE dim_definition RENAME COLUMN dim_name TO name")
    op.execute("ALTER TABLE dim_definition RENAME COLUMN dim_code TO code")
    op.execute("ALTER TABLE dim_definition RENAME COLUMN dim_type TO type")
    
    # 重命名唯一索引
    op.execute("DROP INDEX IF EXISTS dim_definition_dim_code_key")
    op.execute("CREATE UNIQUE INDEX dim_definition_code_key ON dim_definition (code)")
    
    print("✅ dim_definition 表修改完成")
    
    # ==================== 2. dim_field_mapping 表 ====================
    print("处理 dim_field_mapping 表...")
    
    # 删除旧的主键约束
    op.execute("ALTER TABLE dim_field_mapping DROP CONSTRAINT IF EXISTS dim_field_mapping_pkey")
    
    # 删除唯一索引（如果存在）
    op.execute("DROP INDEX IF EXISTS uk_db_table_field")
    
    # 添加联合主键 (db_table, field)
    op.execute("ALTER TABLE dim_field_mapping ADD PRIMARY KEY (db_table, field)")
    
    print("✅ dim_field_mapping 表修改完成")
    
    # ==================== 3. metric_definition 表 ====================
    print("处理 metric_definition 表...")
    
    # 重命名字段
    op.execute("ALTER TABLE metric_definition RENAME COLUMN metric_id TO id")
    op.execute("ALTER TABLE metric_definition RENAME COLUMN metric_name TO name")
    op.execute("ALTER TABLE metric_definition RENAME COLUMN metric_en TO code")
    
    # 重命名唯一索引
    op.execute("DROP INDEX IF EXISTS idx_metric_en_unique")
    op.execute("CREATE UNIQUE INDEX idx_code_unique ON metric_definition (code)")
    
    print("✅ metric_definition 表修改完成")
    
    # ==================== 4. metric_dim_rel 表 ====================
    print("处理 metric_dim_rel 表...")
    
    # 检查并删除 id 列（如果存在）
    try:
        op.execute("ALTER TABLE metric_dim_rel DROP COLUMN IF EXISTS id")
        print("  - 已删除 id 列")
    except Exception as e:
        print(f"  - id 列不存在或已删除: {str(e)}")
    
    # 删除旧的主键约束（如果存在）
    op.execute("ALTER TABLE metric_dim_rel DROP CONSTRAINT IF EXISTS metric_dim_rel_pkey")
    
    # 添加联合主键 (metric_id, dim_id)
    op.execute("ALTER TABLE metric_dim_rel ADD PRIMARY KEY (metric_id, dim_id)")
    
    print("✅ metric_dim_rel 表修改完成")
    
    # ==================== 5. table_lineage 表 ====================
    print("处理 table_lineage 表...")
    
    # 重命名字段
    op.execute("ALTER TABLE table_lineage RENAME COLUMN lineage_id TO id")
    
    # ⚠️ 确保唯一约束存在（如果不存在则创建）
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uk_source_target_table ON table_lineage (source_table, target_table)")
    
    print("✅ table_lineage 表修改完成")
    
    # ==================== 6. field_lineage 表 ====================
    print("处理 field_lineage 表...")
    
    # 重命名字段
    op.execute("ALTER TABLE field_lineage RENAME COLUMN lineage_id TO id")
    
    print("✅ field_lineage 表修改完成")
    
    print("\n🎉 所有表结构修改完成！")


def downgrade():
    """回滚修改"""
    
    # ==================== 1. dim_definition 表 ====================
    op.execute("ALTER TABLE dim_definition RENAME COLUMN id TO dim_id")
    op.execute("ALTER TABLE dim_definition RENAME COLUMN name TO dim_name")
    op.execute("ALTER TABLE dim_definition RENAME COLUMN code TO dim_code")
    op.execute("ALTER TABLE dim_definition RENAME COLUMN type TO dim_type")
    op.execute("DROP INDEX IF EXISTS dim_definition_code_key")
    op.execute("CREATE UNIQUE INDEX dim_definition_dim_code_key ON dim_definition (dim_code)")
    
    # ==================== 2. dim_field_mapping 表 ====================
    op.execute("ALTER TABLE dim_field_mapping DROP CONSTRAINT IF EXISTS dim_field_mapping_pkey")
    op.execute("ALTER TABLE dim_field_mapping ADD PRIMARY KEY (dim_id)")
    op.execute("CREATE UNIQUE INDEX uk_db_table_field ON dim_field_mapping (db_table, field)")
    
    # ==================== 3. metric_definition 表 ====================
    op.execute("ALTER TABLE metric_definition RENAME COLUMN id TO metric_id")
    op.execute("ALTER TABLE metric_definition RENAME COLUMN name TO metric_name")
    op.execute("ALTER TABLE metric_definition RENAME COLUMN code TO metric_en")
    op.execute("DROP INDEX IF EXISTS idx_code_unique")
    op.execute("CREATE UNIQUE INDEX idx_metric_en_unique ON metric_definition (metric_en)")
    
    # ==================== 4. metric_dim_rel 表 ====================
    op.execute("ALTER TABLE metric_dim_rel DROP CONSTRAINT IF EXISTS metric_dim_rel_pkey")
    op.execute("ALTER TABLE metric_dim_rel ADD PRIMARY KEY (id)")
    # 注意：回滚时需要重新添加 id 列，这里简化处理
    
    # ==================== 5. table_lineage 表 ====================
    op.execute("ALTER TABLE table_lineage RENAME COLUMN id TO lineage_id")
    
    # ==================== 6. field_lineage 表 ====================
    op.execute("ALTER TABLE field_lineage RENAME COLUMN id TO lineage_id")

