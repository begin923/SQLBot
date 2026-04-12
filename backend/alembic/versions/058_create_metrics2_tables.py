"""create metrics2 tables - version management and lineage

Revision ID: 058
Revises: 057
Create Date: 2026-04-07

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '058'
down_revision = '5755c0b95839'  # 053_update_chat.py
branch_labels = None
depends_on = None


def upgrade():
    """
    创建 metrics2 模块的11张表
    
    包括：
    1. 语义层（2张）：metric_definition, dim_definition
    2. 映射层（3张）：metric_dim_rel, metric_source_mapping, metric_compound_rel
    3. 版本管理层（2张）：metric_version, dim_version
    4. 双血缘层（4张）：table_lineage, field_lineage, metric_lineage, dim_field_mapping
    
    注意：如果表已存在，先删除再重建
    """
    
    # ==================== 清理旧表（如果存在）====================
    # 按依赖顺序删除
    try:
        op.drop_table('dim_field_mapping')
    except Exception:
        pass
    
    try:
        op.drop_table('metric_lineage')
    except Exception:
        pass
    
    try:
        op.drop_table('field_lineage')
    except Exception:
        pass
    
    try:
        op.drop_table('table_lineage')
    except Exception:
        pass
    
    try:
        op.drop_table('dim_version')
    except Exception:
        pass
    
    try:
        op.drop_table('metric_version')
    except Exception:
        pass
    
    try:
        op.drop_table('metric_compound_rel')
    except Exception:
        pass
    
    try:
        op.drop_table('metric_source_mapping')
    except Exception:
        pass
    
    try:
        op.drop_table('metric_dim_rel')
    except Exception:
        pass
    
    try:
        op.drop_table('dim_definition')
    except Exception:
        pass
    
    try:
        op.drop_table('metric_definition')
    except Exception:
        pass
    
    # ==================== 语义层 ====================
    
    # 1. 指标定义表
    op.create_table(
        'metric_definition',
        sa.Column('metric_id', sa.String(32), nullable=False, comment='指标ID(M000001)'),
        sa.Column('metric_name', sa.String(128), nullable=False, comment='指标名称'),
        sa.Column('metric_code', sa.String(64), nullable=False, comment='指标编码'),
        sa.Column('metric_type', sa.String(16), nullable=False, comment='指标类型(ATOMIC/COMPOUND)'),
        sa.Column('biz_domain', sa.String(32), nullable=False, comment='业务域'),
        sa.Column('cal_logic', sa.Text, nullable=True, comment='计算逻辑'),
        sa.Column('unit', sa.String(16), nullable=True, comment='单位'),
        sa.Column('status', sa.SmallInteger, nullable=False, server_default='1', comment='状态(0:禁用 1:启用)'),
        sa.Column('create_time', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), comment='创建时间'),
        sa.Column('modify_time', sa.DateTime, nullable=True, server_default=sa.text('CURRENT_TIMESTAMP'), comment='修改时间'),
        sa.PrimaryKeyConstraint('metric_id')
    )
    op.create_index('idx_metric_code', 'metric_definition', ['metric_code'], unique=True)
    op.create_index('idx_metric_name', 'metric_definition', ['metric_name'])
    op.create_index('idx_biz_domain', 'metric_definition', ['biz_domain'])
    
    # 2. 维度定义表
    op.create_table(
        'dim_definition',
        sa.Column('dim_id', sa.String(32), nullable=False, comment='维度ID(D000001)'),
        sa.Column('dim_name', sa.String(64), nullable=False, comment='维度名称'),
        sa.Column('dim_code', sa.String(64), nullable=False, comment='维度编码'),
        sa.Column('dim_type', sa.String(16), nullable=False, comment='维度类型'),
        sa.Column('is_valid', sa.SmallInteger, nullable=False, server_default='1', comment='是否启用(0:禁用 1:启用)'),
        sa.Column('create_time', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), comment='创建时间'),
        sa.Column('modify_time', sa.DateTime, nullable=True, server_default=sa.text('CURRENT_TIMESTAMP'), comment='修改时间'),
        sa.PrimaryKeyConstraint('dim_id')
    )
    op.create_index('idx_dim_code', 'dim_definition', ['dim_code'], unique=True)
    op.create_index('idx_dim_name', 'dim_definition', ['dim_name'])
    
    # ==================== 映射层 ====================
    
    # 3. 指标维度关联表
    op.create_table(
        'metric_dim_rel',
        sa.Column('id', sa.BigInteger, nullable=False, autoincrement=True, comment='主键ID'),
        sa.Column('metric_id', sa.String(32), nullable=False, comment='指标ID'),
        sa.Column('dim_id', sa.String(32), nullable=False, comment='维度ID'),
        sa.Column('is_required', sa.SmallInteger, nullable=False, server_default='0', comment='是否必选(0:否 1:是)'),
        sa.Column('create_time', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), comment='创建时间'),
        sa.Column('modify_time', sa.DateTime, nullable=True, server_default=sa.text('CURRENT_TIMESTAMP'), comment='修改时间'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('metric_id', 'dim_id', name='uq_metric_dim')
    )
    op.create_index('idx_metric_id', 'metric_dim_rel', ['metric_id'])
    op.create_index('idx_dim_id', 'metric_dim_rel', ['dim_id'])
    
    # 4. 指标多源映射表
    op.create_table(
        'metric_source_mapping',
        sa.Column('map_id', sa.String(32), nullable=False, comment='映射ID(MAP001)'),
        sa.Column('metric_id', sa.String(32), nullable=False, comment='指标ID'),
        sa.Column('source_type', sa.String(16), nullable=False, comment='数据源类型(OFFLINE/REAL_TIME)'),
        sa.Column('datasource', sa.String(64), nullable=False, comment='数据源标识'),
        sa.Column('db_table', sa.String(128), nullable=False, comment='物理表名'),
        sa.Column('metric_column', sa.String(64), nullable=True, comment='指标字段'),
        sa.Column('filter_condition', sa.Text, nullable=True, comment='过滤条件'),
        sa.Column('agg_func', sa.String(32), nullable=True, comment='聚合函数'),
        sa.Column('priority', sa.SmallInteger, nullable=False, comment='优先级'),
        sa.Column('is_valid', sa.SmallInteger, nullable=False, server_default='1', comment='是否启用(0:禁用 1:启用)'),
        sa.Column('source_level', sa.String(16), nullable=False, comment='源等级(AUTHORITY/STANDBY/DISCARD)'),
        sa.Column('create_time', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), comment='创建时间'),
        sa.Column('modify_time', sa.DateTime, nullable=True, server_default=sa.text('CURRENT_TIMESTAMP'), comment='修改时间'),
        sa.PrimaryKeyConstraint('map_id')
    )
    op.create_index('idx_metric_id_source', 'metric_source_mapping', ['metric_id'])
    op.create_index('idx_db_table', 'metric_source_mapping', ['db_table'])
    
    # 5. 复合指标关系表
    op.create_table(
        'metric_compound_rel',
        sa.Column('id', sa.BigInteger, nullable=False, autoincrement=True, comment='主键ID'),
        sa.Column('metric_id', sa.String(32), nullable=False, comment='复合指标ID'),
        sa.Column('sub_metric_id', sa.String(32), nullable=False, comment='子指标ID'),
        sa.Column('cal_operator', sa.String(8), nullable=False, comment='运算符'),
        sa.Column('sort', sa.SmallInteger, nullable=False, comment='计算顺序'),
        sa.Column('create_time', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), comment='创建时间'),
        sa.Column('modify_time', sa.DateTime, nullable=True, server_default=sa.text('CURRENT_TIMESTAMP'), comment='修改时间'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_metric_id_compound', 'metric_compound_rel', ['metric_id'])
    op.create_index('idx_sub_metric_id', 'metric_compound_rel', ['sub_metric_id'])
    
    # ==================== 版本管理层 ====================
    
    # 6. 指标版本管理表
    op.create_table(
        'metric_version',
        sa.Column('version_id', sa.String(32), nullable=False, comment='版本ID(V000001)'),
        sa.Column('metric_id', sa.String(32), nullable=False, comment='指标ID'),
        sa.Column('cal_logic', sa.Text, nullable=False, comment='历史口径'),
        sa.Column('version', sa.Integer, nullable=False, comment='版本号'),
        sa.Column('effective_time', sa.DateTime, nullable=False, comment='生效时间'),
        sa.Column('expire_time', sa.DateTime, nullable=True, comment='失效时间'),
        sa.Column('is_current', sa.SmallInteger, nullable=False, server_default='1', comment='当前版本'),
        sa.PrimaryKeyConstraint('version_id')
    )
    op.create_index('idx_metric_version', 'metric_version', ['metric_id'])
    op.create_index('idx_effective_time', 'metric_version', ['effective_time'])
    
    # 7. 维度版本管理表
    op.create_table(
        'dim_version',
        sa.Column('version_id', sa.String(32), nullable=False, comment='版本ID'),
        sa.Column('dim_id', sa.String(32), nullable=False, comment='维度ID'),
        sa.Column('dim_name', sa.String(64), nullable=False, comment='维度名称'),
        sa.Column('version', sa.Integer, nullable=False, comment='版本号'),
        sa.Column('effective_time', sa.DateTime, nullable=False, comment='生效时间'),
        sa.Column('is_current', sa.SmallInteger, nullable=False, server_default='1', comment='当前版本'),
        sa.PrimaryKeyConstraint('version_id')
    )
    
    # ==================== 双血缘层 ====================
    
    # 8. 表级血缘表
    op.create_table(
        'table_lineage',
        sa.Column('lineage_id', sa.String(32), nullable=False, comment='表血缘ID(L000001)'),
        sa.Column('source_table', sa.String(128), nullable=False, comment='上游明细表'),
        sa.Column('target_table', sa.String(128), nullable=False, comment='下游汇总表'),
        sa.Column('create_time', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), comment='创建时间'),
        sa.Column('modify_time', sa.DateTime, nullable=True, server_default=sa.text('CURRENT_TIMESTAMP'), comment='修改时间'),
        sa.PrimaryKeyConstraint('lineage_id')
    )
    op.create_index('idx_source_table', 'table_lineage', ['source_table'])
    op.create_index('idx_target_table', 'table_lineage', ['target_table'])
    
    # 9. 字段级血缘表
    op.create_table(
        'field_lineage',
        sa.Column('lineage_id', sa.String(32), nullable=False, comment='字段血缘ID(F000001)'),
        sa.Column('table_lineage_id', sa.String(32), nullable=False, comment='表血缘ID'),
        sa.Column('source_table', sa.String(128), nullable=False, comment='上游表'),
        sa.Column('source_field', sa.String(64), nullable=False, comment='上游字段'),
        sa.Column('target_table', sa.String(128), nullable=False, comment='下游表'),
        sa.Column('target_field', sa.String(64), nullable=False, comment='下游字段'),
        sa.Column('target_field_mark', sa.String(16), nullable=False, server_default='normal', comment='目标字段标记(public_dim/private_dim/metric/normal)'),
        sa.Column('dim_id', sa.String(32), nullable=True, comment='公共维度绑定ID'),
        sa.Column('create_time', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), comment='创建时间'),
        sa.Column('modify_time', sa.DateTime, nullable=True, server_default=sa.text('CURRENT_TIMESTAMP'), comment='修改时间'),
        sa.PrimaryKeyConstraint('lineage_id')
    )
    
    # 10. 指标血缘表
    op.create_table(
        'metric_lineage',
        sa.Column('metric_id', sa.String(32), nullable=False, comment='指标ID'),
        sa.Column('field_lineage_id', sa.String(32), nullable=False, comment='字段血缘ID'),
        sa.Column('create_time', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), comment='创建时间'),
        sa.Column('modify_time', sa.DateTime, nullable=True, server_default=sa.text('CURRENT_TIMESTAMP'), comment='修改时间'),
        sa.PrimaryKeyConstraint('metric_id', 'field_lineage_id')
    )
    
    # 11. 维度字段映射表
    op.create_table(
        'dim_field_mapping',
        sa.Column('dim_id', sa.String(32), nullable=False, comment='维度ID'),
        sa.Column('db_table', sa.String(128), nullable=False, comment='物理表'),
        sa.Column('dim_field', sa.String(64), nullable=False, comment='维度字段'),
        sa.Column('create_time', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), comment='创建时间'),
        sa.Column('modify_time', sa.DateTime, nullable=True, server_default=sa.text('CURRENT_TIMESTAMP'), comment='修改时间'),
        sa.PrimaryKeyConstraint('db_table', 'dim_field')
    )
    op.create_index('idx_dim_id_lineage', 'dim_field_mapping', ['dim_id'])


def downgrade():
    """删除 metrics2 模块的11张表"""
    
    # 删除双血缘层
    op.drop_table('dim_field_mapping')
    op.drop_table('metric_lineage')
    op.drop_table('field_lineage')
    op.drop_table('table_lineage')
    
    # 删除版本管理层
    op.drop_table('dim_version')
    op.drop_table('metric_version')
    
    # 删除映射层
    op.drop_table('metric_compound_rel')
    op.drop_table('metric_source_mapping')
    op.drop_table('metric_dim_rel')
    
    # 删除语义层
    op.drop_table('dim_definition')
    op.drop_table('metric_definition')
