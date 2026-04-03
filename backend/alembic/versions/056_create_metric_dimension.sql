
-- 2. 新建指标元数据表
DROP TABLE IF EXISTS metric_dimension;
CREATE TABLE metric_dimension (
    table_name varchar(100) NOT NULL,
    dim_column varchar(100) NOT NULL,
    dim_name varchar(100),
    create_time timestamp DEFAULT now(),
    PRIMARY KEY (table_name, dim_column)
);

-- 3. 添加表注释
COMMENT ON TABLE metric_dimension IS '智能问数指标维度表）';

-- 4. 添加字段注释（PostgreSQL 标准写法）
COMMENT ON COLUMN metric_dimension.table_name IS '指标所属的物理表名';
COMMENT ON COLUMN metric_dimension.dim_column IS '指标维度字段';
COMMENT ON COLUMN metric_dimension.dim_name IS '指标维度字段名';
COMMENT ON COLUMN metric_dimension.create_time IS '记录创建时间';



