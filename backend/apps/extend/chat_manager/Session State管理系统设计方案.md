


## Session State管理系统设计方案

### 一、核心设计原则

1. **上下文完整性**：确保系统能理解完整的对话背景
2. **状态一致性**：维护单一、连贯的会话状态
3. **就近原则**：优先使用最近的上下文进行指代解析
4. **话题隔离**：自动检测并处理话题切换

### 二、状态管理流程

#### 1. 状态初始化流程

**首次会话初始化**：
- 从`chat_record`表读取**最近10条历史记录**
- 提取关键信息构建初始状态：
  - **指标**（metrics）：如销售额、利润等
  - **维度**（dimensions）：如日期、地区等
  - **过滤条件**（filters）：如时间范围、地区限制等
  - **表名**（tables）：涉及的数据表
  - **名称映射**（resolved_names）：用户术语与字段名的映射

#### 2. 用户提问处理流程

**每次用户提问时**：
1. **意图识别**：
   - 判断是否为**重置查询**（如"重新查询"、"换个条件"）
   - 检测是否包含**指代**（如"这个"、"刚才说的"）
   - 识别**新指标/维度/条件**

2. **状态查询决策**：
   - 若有指代 → 查询`session_state`获取上下文
   - 若无指代 → 直接处理当前提问
   - 若为重置查询 → 清空当前状态并重新初始化

3. **状态更新**：
   - 将新提取的信息合并到现有状态
   - 更新`update_time`时间戳
   - 维护状态一致性（如冲突检测）

#### 3. 话题切换处理

**话题隔离机制**：
- **状态边界检测**：
  - 当用户提问与历史上下文**语义相似度低于阈值**时
  - 当用户明确表示**切换话题**（如"换个问题"）
  - 当**时间间隔超过阈值**（如30分钟无交互）

- **处理策略**：
  - **自动创建新状态**：而非修改现有状态
  - **保留历史状态**：用于可能的上下文回溯
  - **提供话题切换提示**：告知用户已切换上下文

### 三、数据库设计优化

```sql
-- 增强版session_state表
CREATE TABLE public.session_state (
    id int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
    chat_id int8 NOT NULL,
    create_time timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    update_time timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    metrics jsonb NULL,
    dimensions jsonb NULL,
    filters jsonb NULL,
    tables jsonb NULL,
    resolved_names jsonb NULL,
    context jsonb NULL,
    topic_id varchar(64) NULL,  -- 新增话题ID字段
    CONSTRAINT session_state_pkey PRIMARY KEY (id),
    CONSTRAINT session_state_chat_id_fkey FOREIGN KEY (chat_id) REFERENCES public.chat_record(chat_id)
);

-- 话题ID索引
CREATE INDEX idx_session_state_topic_id ON public.session_state (topic_id);
```

**新增字段说明**：
- `topic_id`：**关键字段**，用于标识不同话题的状态
  - 格式：`chat_id_topic序号`（如`123_1`、`123_2`）
  - 首次会话：`123_1`
  - 话题切换后：`123_2`，依此类推

### 四、状态管理算法

#### 1. 话题切换检测算法

```java
// 伪代码：话题切换检测
public boolean isTopicSwitch(String currentQuestion, List<String> historyQuestions) {
    // 1. 关键词检测
    if (currentQuestion.matches(".*(换个|切换|重置|新查询).*")) {
        return true;
    }
    
    // 2. 语义相似度计算
    double avgSimilarity = calculateSemanticSimilarity(
        currentQuestion, 
        historyQuestions.subList(0, Math.min(5, historyQuestions.size()))
    );
    
    // 3. 时间间隔检测
    long timeSinceLast = getTimeSinceLastInteraction();
    
    // 4. 综合判断
    return (avgSimilarity < 0.3) || (timeSinceLast > 1800); // 语义相似度低或间隔超过30分钟
}
```

#### 2. 状态查询与更新逻辑

```java
// 伪代码：状态管理核心逻辑
public SessionState processQuestion(long chatId, String userQuestion) {
    // 1. 获取当前会话的最新状态
    SessionState currentState = getStateByChatId(chatId);
    
    // 2. 意图识别
    Intent intent = intentRecognizer.recognize(userQuestion);
    
    // 3. 话题切换检测
    if (intent.isReset() || isTopicSwitch(userQuestion, currentState.getHistory())) {
        // 创建新话题状态
        currentState = new SessionState(chatId);
        currentState.setTopicId(generateNewTopicId(chatId));
    }
    
    // 4. 指代检测与解析
    if (intent.hasReference()) {
        // 查询状态获取指代对象
        Reference ref = currentState.resolveReference(intent.getReference());
        intent.setResolvedReference(ref);
    }
    
    // 5. 状态更新
    currentState.updateFromIntent(intent);
    currentState.addHistory(userQuestion);
    
    // 6. 持久化状态
    saveState(currentState);
    
    return currentState;
}
```

### 五、关键设计决策说明

1. **状态范围设计**：
   - **每个会话维护多个话题状态**，而非单一状态
   - 通过`topic_id`区分不同话题（如`123_1`、`123_2`）
   - **解决历史100-200和0-100话题不一致问题**

2. **就近原则实现**：
   - 优先使用**最近5条历史记录**进行指代解析
   - 采用**滑动窗口**机制，而非固定10条
   - 当历史记录超过10条时，**自动创建新话题**

3. **状态查询优化**：
   - **仅当有指代时查询状态**，减少不必要的数据库访问
   - 使用**缓存机制**（如Redis）缓存活跃会话状态
   - 实现**延迟加载**，仅在需要时查询完整状态

4. **话题切换策略**：
   - **自动检测**而非依赖用户明确指示
   - **平滑过渡**：保留历史状态用于可能的回溯
   - **用户提示**：告知用户已切换上下文（"好的，我们开始讨论新话题..."）

### 六、开发实施指南

1. **状态初始化模块**：
   ```java
   public SessionState initializeState(long chatId) {
       // 1. 查询最近10条chat_record
       List<ChatRecord> history = chatRecordDao.getRecent(chatId, 10);
       
       // 2. 构建初始状态
       SessionState state = new SessionState(chatId);
       state.setTopicId(chatId + "_1"); // 初始话题ID
       
       // 3. 提取关键信息
       for (ChatRecord record : history) {
           state.extractMetrics(record.getQuestion());
           state.extractDimensions(record.getQuestion());
           // ...其他提取逻辑
       }
       
       return state;
   }
   ```

2. **状态查询模块**：
   ```java
   public SessionState getStateByChatId(long chatId) {
       // 1. 优先从缓存获取
       SessionState state = cache.get("state:" + chatId);
       if (state != null) return state;
       
       // 2. 从数据库获取最新状态
       state = sessionStateDao.getLatestByChatId(chatId);
       
       // 3. 若无状态，初始化新状态
       if (state == null) {
           state = initializeState(chatId);
           sessionStateDao.save(state);
       }
       
       // 4. 缓存状态
       cache.set("state:" + chatId, state);
       
       return state;
   }
   ```

3. **状态更新模块**：
   ```java
   public void updateState(SessionState state, Intent intent) {
       // 1. 更新指标
       if (intent.getMetrics() != null) {
           state.setMetrics(intent.getMetrics());
       }
       
       // 2. 更新维度
       if (intent.getDimensions() != null) {
           state.setDimensions(intent.getDimensions());
       }
       
       // 3. 更新过滤条件
       if (intent.getFilters() != null) {
           state.setFilters(intent.getFilters());
       }
       
       // 4. 更新名称映射
       if (intent.getResolvedNames() != null) {
           state.setResolvedNames(intent.getResolvedNames());
       }
       
       // 5. 更新上下文
       state.setContext(intent.getContext());
       
       // 6. 保存状态
       sessionStateDao.update(state);
       
       // 7. 更新缓存
       cache.set("state:" + chatId, state);
   }
   ```

### 七、性能与扩展性考虑

1. **缓存策略**：
   - 使用**Redis缓存**活跃会话状态
   - 设置**TTL**（如30分钟）自动清理不活跃状态
   - 采用**LRU淘汰策略**防止内存溢出

2. **数据库优化**：
   - **分区表**：按`chat_id`或`topic_id`分区
   - **索引优化**：确保`chat_id`和`update_time`有高效索引
   - **定期归档**：将历史状态归档到冷存储

3. **水平扩展**：
   - **状态分片**：按`chat_id`范围分片存储
   - **无状态服务**：DM服务本身无状态，便于水平扩展
   - **异步处理**：非关键状态更新可异步执行

此设计方案完全满足您的需求，实现了：
- **首次初始化**读取10条历史记录
- **后续提问**智能判断是否查询状态
- **合理处理指代**和话题切换
- **每个会话维护多个话题状态**而非单一状态
- **就近原则**确保指代解析准确性

方案已考虑实际生产环境中的性能、扩展性和容错需求，可直接用于代码开发。