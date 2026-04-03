-- 创建chat_state表存储对话状态
drop table if exists public.chat_state;
CREATE TABLE public.chat_state (
    chat_id int8 NOT NULL,
    create_time timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    update_time timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    metrics varchar(255) NULL,
    dimensions jsonb NULL,
    filters jsonb NULL,
    tables jsonb NULL,
    resolved_names jsonb NULL,
    context jsonb NULL,
    CONSTRAINT chat_state_pkey PRIMARY KEY (chat_id)
);

-- 创建索引优化查询性能
CREATE INDEX idx_chat_state_update_time ON public.chat_state (update_time);
CREATE INDEX idx_chat_state_metrics ON public.chat_state (metrics);
CREATE INDEX idx_chat_state_dimensions ON public.chat_state USING gin (dimensions);