
-- 2. 新建指标血缘表
DROP TABLE IF EXISTS metric_lineage2;
CREATE TABLE metric_lineage2 (
    metric_column varchar(100) NOT NULL,
    table_name varchar(100) NOT NULL,
    metric_name varchar(100) NOT NULL,
    synonyms text,
    upstream_table varchar(100),
    filter text,
    calc_logic text,
    dw_layer varchar(20),
    embedding_vector vector,
    create_time timestamp DEFAULT now(),
    PRIMARY KEY (metric_column, table_name)
);

-- 3. 添加表注释
COMMENT ON TABLE metric_lineage2 IS '智能问数指标元数据表（自动扩源+分层下钻+语义检索）';

-- 4. 添加字段注释（PostgreSQL 标准写法）
COMMENT ON COLUMN metric_lineage2.metric_column IS '指标字段名（如：sale_number）';
COMMENT ON COLUMN metric_lineage2.metric_name IS '指标名称（如：销售额）';
COMMENT ON COLUMN metric_lineage2.synonyms IS '指标同义词，逗号分隔（如：营收,卖钱额）';
COMMENT ON COLUMN metric_lineage2.table_name IS '指标所属的物理表名';
COMMENT ON COLUMN metric_lineage2.upstream_table IS '上游关联表名（数仓分层下钻使用）';
COMMENT ON COLUMN metric_lineage2.filter IS '指标核心字段，逗号分隔';
COMMENT ON COLUMN metric_lineage2.calc_logic IS '指标计算逻辑（如：sum(amt)/count(id)）';
COMMENT ON COLUMN metric_lineage2.dw_layer IS '数仓分层（ODS/DWD/DWS/ADS）';
COMMENT ON COLUMN metric_lineage2.embedding_vector IS '指标向量值，用于语义相似度检索';
COMMENT ON COLUMN metric_lineage2.create_time IS '记录创建时间';




