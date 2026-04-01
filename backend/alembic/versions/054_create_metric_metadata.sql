
-- 2. 新建指标元数据表（无注释，纯结构）
DROP TABLE IF EXISTS metric_metadata;
CREATE TABLE metric_metadata (
    id bigserial PRIMARY KEY,
    metric_column varchar(100) NOT NULL,
    metric_name varchar(100) NOT NULL,
    synonyms text,
    datasource_id int8 ,
    table_name varchar(100) NOT NULL,
    core_fields text,
    calc_logic text,
    upstream_table varchar(100),
    dw_layer varchar(20),
    embedding_vector vector,
    create_time timestamp DEFAULT now()
);

-- 3. 添加表注释
COMMENT ON TABLE metric_metadata IS '智能问数指标元数据表（自动扩源+分层下钻+语义检索）';

-- 4. 添加字段注释（PostgreSQL 标准写法）
COMMENT ON COLUMN metric_metadata.id IS '主键ID';
COMMENT ON COLUMN metric_metadata.metric_column IS '指标字段名（如：sale_number）';
COMMENT ON COLUMN metric_metadata.metric_name IS '指标名称（如：销售额）';
COMMENT ON COLUMN metric_metadata.synonyms IS '指标同义词，逗号分隔（如：营收,卖钱额）';
COMMENT ON COLUMN metric_metadata.datasource_id IS '对应SQLBot的数据源id';
COMMENT ON COLUMN metric_metadata.table_name IS '指标所属的物理表名';
COMMENT ON COLUMN metric_metadata.core_fields IS '指标核心字段，逗号分隔';
COMMENT ON COLUMN metric_metadata.calc_logic IS '指标计算逻辑（如：sum(amt)/count(id)）';
COMMENT ON COLUMN metric_metadata.upstream_table IS '上游关联表名（数仓分层下钻使用）';
COMMENT ON COLUMN metric_metadata.dw_layer IS '数仓分层（ODS/DWD/DWS/ADS）';
COMMENT ON COLUMN metric_metadata.embedding_vector IS '指标向量值，用于语义相似度检索';
COMMENT ON COLUMN metric_metadata.create_time IS '记录创建时间';

-- 5. 创建业务索引
CREATE UNIQUE INDEX idx_metric_table ON metric_metadata (metric_column,table_name);
CREATE INDEX idx_datasource ON metric_metadata (datasource_id);
CREATE INDEX idx_table_name ON metric_metadata (table_name);

