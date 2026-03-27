import datetime
from pathlib import Path
from typing import List, Optional

from apps.ai_model.embedding import EmbeddingModelCache
from sqlalchemy import and_, or_, select, func, delete, update, text

from apps.extend.metric_metadata.models.metric_metadata_model import MetricMetadata, MetricMetadataInfo
from common.core.config import settings
from common.core.deps import SessionDep


def create_metric_metadata(session: SessionDep, info: MetricMetadataInfo, skip_embedding: bool = False):
    """
    创建单个指标元数据记录
    
    Args:
        session: 数据库会话
        info: 指标元数据信息对象
        skip_embedding: 是否跳过 embedding 处理（用于批量插入）
    
    Returns:
        创建的记录 ID
    """
    # ========== 步骤 1：基本验证 ==========
    if not info.metric_name or not info.metric_name.strip():
        raise Exception("指标名称不能为空")
    
    if not info.table_name or not info.table_name.strip():
        raise Exception("表名不能为空")
    
    create_time = datetime.datetime.now()
    
    # ========== 步骤 2：检查是否已存在 ==========
    exists_query = session.query(MetricMetadata).filter(
        and_(
            MetricMetadata.metric_name == info.metric_name.strip(),
            MetricMetadata.table_name == info.table_name.strip(),
            MetricMetadata.datasource_id == info.datasource_id
        )
    ).first()
    
    if exists_query:
        raise Exception(f"指标 {info.metric_name} 在表 {info.table_name} 中已存在")
    
    # ========== 步骤 3：创建记录 ==========
    metric = MetricMetadata(
        metric_name=info.metric_name.strip(),
        synonyms=info.synonyms.strip() if info.synonyms else None,
        datasource_id=info.datasource_id,
        table_name=info.table_name.strip(),
        core_fields=info.core_fields.strip() if info.core_fields else None,
        calc_logic=info.calc_logic.strip() if info.calc_logic else None,
        upstream_table=info.upstream_table.strip() if info.upstream_table else None,
        dw_layer=info.dw_layer.strip() if info.dw_layer else None,
        create_time=create_time
    )
    
    session.add(metric)
    session.flush()
    session.refresh(metric)  # 获取生成的 ID
    
    session.commit()
    
    # ========== 步骤 4：处理 Embedding ==========
    if not skip_embedding and settings.EMBEDDING_ENABLED:
        try:
            _save_metric_embeddings([metric.id])
        except Exception as e:
            print(f"Metric embedding processing failed: {str(e)}")
            # embedding 失败不影响主流程
    
    return metric.id


def batch_create_metric_metadata(session: SessionDep, info_list: List[MetricMetadataInfo]):
    """
    批量创建指标元数据记录
    
    Args:
        session: 数据库会话
        info_list: 指标元数据信息列表
    
    Returns:
        处理结果统计
    """
    if not info_list:
        return {
            'success_count': 0,
            'failed_records': [],
            'duplicate_count': 0,
            'original_count': 0
        }
    
    failed_records = []
    success_count = 0
    inserted_ids = []
    
    # 去重处理
    unique_key_set = set()
    deduplicated_list = []
    duplicate_count = 0
    
    for info in info_list:
        # 创建唯一标识
        unique_key = (
            info.metric_name.strip().lower() if info.metric_name else '',
            info.table_name.strip().lower() if info.table_name else '',
            info.datasource_id if info.datasource_id else 0
        )
        
        if unique_key in unique_key_set:
            duplicate_count += 1
            continue
        
        unique_key_set.add(unique_key)
        deduplicated_list.append(info)
    
    # 批量插入
    for info in deduplicated_list:
        try:
            metric_id = create_metric_metadata(session, info, skip_embedding=True)
            inserted_ids.append(metric_id)
            success_count += 1
        except Exception as e:
            failed_records.append({
                'data': info,
                'errors': [str(e)]
            })
    
    # 批量处理 embedding（只在最后执行一次）
    if success_count > 0 and inserted_ids and settings.EMBEDDING_ENABLED:
        try:
            _save_metric_embeddings(inserted_ids)
        except Exception as e:
            print(f"Batch metric embedding processing failed: {str(e)}")
    
    return {
        'success_count': success_count,
        'failed_records': failed_records,
        'duplicate_count': duplicate_count,
        'original_count': len(info_list),
        'deduplicated_count': len(deduplicated_list)
    }


def update_metric_metadata(session: SessionDep, info: MetricMetadataInfo):
    """
    更新指标元数据记录
    
    Args:
        session: 数据库会话
        info: 指标元数据信息对象
    
    Returns:
        更新的记录 ID
    """
    if not info.id:
        raise Exception("ID 不能为空")
    
    count = session.query(MetricMetadata).filter(
        MetricMetadata.id == info.id
    ).count()
    
    if count == 0:
        raise Exception("指标元数据不存在")
    
    stmt = update(MetricMetadata).where(
        MetricMetadata.id == info.id
    ).values(
        metric_name=info.metric_name.strip() if info.metric_name else None,
        synonyms=info.synonyms.strip() if info.synonyms else None,
        datasource_id=info.datasource_id,
        table_name=info.table_name.strip() if info.table_name else None,
        core_fields=info.core_fields.strip() if info.core_fields else None,
        calc_logic=info.calc_logic.strip() if info.calc_logic else None,
        upstream_table=info.upstream_table.strip() if info.upstream_table else None,
        dw_layer=info.dw_layer.strip() if info.dw_layer else None,
    )
    
    session.execute(stmt)
    session.commit()
    
    # 更新 embedding
    if settings.EMBEDDING_ENABLED:
        try:
            _save_metric_embeddings([info.id])
        except Exception as e:
            print(f"Update metric embedding processing failed: {str(e)}")
    
    return info.id


def delete_metric_metadata(session: SessionDep, ids: List[int]):
    """
    删除指标元数据记录
    
    Args:
        session: 数据库会话
        ids: 要删除的记录 ID 列表
    """
    stmt = delete(MetricMetadata).where(MetricMetadata.id.in_(ids))
    session.execute(stmt)
    session.commit()


def get_metric_metadata_by_id(session: SessionDep, id: int) -> Optional[MetricMetadataInfo]:
    """
    根据 ID 查询指标元数据
    
    Args:
        session: 数据库会话
        id: 记录 ID
    
    Returns:
        指标元数据信息对象
    """
    metric = session.query(MetricMetadata).filter(MetricMetadata.id == id).first()
    
    if not metric:
        return None
    
    return MetricMetadataInfo(
        id=metric.id,
        metric_name=metric.metric_name,
        synonyms=metric.synonyms,
        datasource_id=metric.datasource_id,
        table_name=metric.table_name,
        core_fields=metric.core_fields,
        calc_logic=metric.calc_logic,
        upstream_table=metric.upstream_table,
        dw_layer=metric.dw_layer,
        enabled=True
    )


def get_metric_metadata_by_names(session, metric_names: List[str], datasource_id: Optional[int] = None) -> List[MetricMetadataInfo]:
    """
    根据指标名称列表查询指标元数据（支持混合查询：模糊匹配 + 向量相似度）
    
    Args:
        session: 数据库会话
        metric_names: 指标名称列表
        datasource_id: 数据源 ID（可选，用于过滤）
    
    Returns:
        指标元数据信息对象列表
    """
    if not metric_names or len(metric_names) == 0:
        return []
    
    _list: List[MetricMetadata] = []
    matched_ids_set = set()  # 用于去重
    
    # ========== 步骤 1：模糊匹配（ILIKE） ==========
    for name in metric_names:
        if not name or not name.strip():
            continue
        
        # 构建模糊查询条件
        conditions = [MetricMetadata.metric_name.ilike(f"%{name.strip()}%")]
        if datasource_id is not None:
            conditions.append(MetricMetadata.datasource_id == datasource_id)
        
        results = session.query(MetricMetadata).filter(and_(*conditions)).all()

        like_metrics = []
        for metric in results:
            if metric.id not in matched_ids_set:
                _list.append(metric)
                like_metrics.append(metric.metric_name)
                matched_ids_set.add(metric.id)

    # ========== 步骤 2：向量相似度匹配（如果启用 EMBEDDING） ==========
    if settings.EMBEDDING_ENABLED and metric_names:
        try:
            from apps.ai_model.embedding import EmbeddingModelCache
            model = EmbeddingModelCache.get_model()
            
            # 为每个搜索词生成 embedding
            for search_name in metric_names:
                if not search_name or not search_name.strip():
                    continue
                
                embedding = model.embed_query(search_name.strip())
                
                # 使用余弦相似度查询相似的指标
                # 注意：这里需要使用 SQL 表达式计算向量相似度
                # PostgreSQL pgvector 扩展支持 <=> 操作符
                similarity_threshold = getattr(settings, 'EMBEDDING_METRIC_SIMILARITY', 0.75)
                top_count = getattr(settings, 'EMBEDDING_METRIC_TOP_COUNT', 5)

                # 构建向量相似度查询
                # 注意：必须使用 HAVING 子句来过滤计算出的相似度
                query = session.query(
                    MetricMetadata.id,
                    MetricMetadata.metric_name,
                    text(f"(1 - (embedding_vector <=> :embedding_array)) AS similarity")
                )
                
                # 添加数据源过滤条件（如果有）
                if datasource_id is not None:
                    query = query.filter(MetricMetadata.datasource_id == datasource_id)
                
                # 先执行查询获取所有结果
                all_results = query.params(embedding_array=str(embedding)).all()

                # 然后在 Python 中过滤相似度 > 阈值的记录
                similarity_results = [
                    row for row in all_results
                    if row[2] > similarity_threshold  # row[2] 是 similarity 列
                ]
                
                # 按相似度降序排序并取 top N
                similarity_results.sort(key=lambda x: x[2], reverse=True)
                similarity_results = similarity_results[:top_count]

                # 添加匹配的结果（去重）
                for row in similarity_results:
                    if row.id not in matched_ids_set:
                        metric = session.get(MetricMetadata, row.id)
                        if metric:
                            _list.append(metric)
                            matched_ids_set.add(metric.id)

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Metric embedding similarity search failed: {str(e)}")
            # 向量搜索失败不影响主流程，继续使用模糊匹配的结果
    
    # ========== 步骤 3：转换为返回格式 ==========
    result_list = []
    for metric in _list:
        result_list.append(MetricMetadataInfo(
            id=metric.id,
            metric_name=metric.metric_name,
            synonyms=metric.synonyms,
            datasource_id=metric.datasource_id,
            table_name=metric.table_name,
            core_fields=metric.core_fields,
            calc_logic=metric.calc_logic,
            upstream_table=metric.upstream_table,
            dw_layer=metric.dw_layer,
            enabled=True
        ))
    
    return result_list


def page_metric_metadata(session: SessionDep, current_page: int = 1, page_size: int = 10,
                         metric_name: Optional[str] = None, 
                         datasource_id: Optional[int] = None):
    """
    分页查询指标元数据
    
    Args:
        session: 数据库会话
        current_page: 当前页码
        page_size: 每页数量
        metric_name: 指标名称（支持模糊查询）
        datasource_id: 数据源 ID
    
    Returns:
        分页结果
    """
    # 构建查询条件
    conditions = []
    if metric_name and metric_name.strip():
        conditions.append(MetricMetadata.metric_name.ilike(f"%{metric_name.strip()}%"))
    if datasource_id is not None:
        conditions.append(MetricMetadata.datasource_id == datasource_id)
    
    # 查询总数
    if conditions:
        count_stmt = select(func.count()).select_from(MetricMetadata).where(and_(*conditions))
    else:
        count_stmt = select(func.count()).select_from(MetricMetadata)
    
    total_count = session.execute(count_stmt).scalar()
    
    # 分页处理
    page_size = max(10, page_size)
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
    current_page = max(1, min(current_page, total_pages)) if total_pages > 0 else 1
    
    # 查询数据
    stmt = select(MetricMetadata)
    if conditions:
        stmt = stmt.where(and_(*conditions))
    
    stmt = stmt.order_by(MetricMetadata.create_time.desc())
    stmt = stmt.offset((current_page - 1) * page_size).limit(page_size)
    
    results = session.execute(stmt).scalars().all()
    
    _list = []
    for metric in results:
        _list.append(MetricMetadataInfo(
            id=metric.id,
            metric_name=metric.metric_name,
            synonyms=metric.synonyms,
            datasource_id=metric.datasource_id,
            table_name=metric.table_name,
            core_fields=metric.core_fields,
            calc_logic=metric.calc_logic,
            upstream_table=metric.upstream_table,
            dw_layer=metric.dw_layer,
            enabled=True
        ))
    
    return current_page, page_size, total_count, total_pages, _list


def get_all_metric_metadata(session: SessionDep, 
                            metric_name: Optional[str] = None,
                            datasource_id: Optional[int] = None):
    """
    获取所有指标元数据（不分页）
    
    Args:
        session: 数据库会话
        metric_name: 指标名称（支持模糊查询）
        datasource_id: 数据源 ID
    
    Returns:
        指标元数据列表
    """
    conditions = []
    if metric_name and metric_name.strip():
        conditions.append(MetricMetadata.metric_name.ilike(f"%{metric_name.strip()}%"))
    if datasource_id is not None:
        conditions.append(MetricMetadata.datasource_id == datasource_id)
    
    stmt = select(MetricMetadata)
    if conditions:
        stmt = stmt.where(and_(*conditions))
    
    stmt = stmt.order_by(MetricMetadata.create_time.desc())
    
    results = session.execute(stmt).scalars().all()
    
    _list = []
    for metric in results:
        _list.append(MetricMetadataInfo(
            id=metric.id,
            metric_name=metric.metric_name,
            synonyms=metric.synonyms,
            datasource_id=metric.datasource_id,
            table_name=metric.table_name,
            core_fields=metric.core_fields,
            calc_logic=metric.calc_logic,
            upstream_table=metric.upstream_table,
            dw_layer=metric.dw_layer,
            enabled=True
        ))
    
    return _list


def _save_metric_embeddings(ids: List[int]):
    """
    为指标元数据计算并保存 embedding 向量
    参考 terminology 表的 save_embeddings 函数实现
    
    Args:
        ids: 指标 ID 列表
    """
    if not settings.EMBEDDING_ENABLED:
        print("ℹ️  EMBEDDING_ENABLED 未启用，跳过向量化")
        return
    
    if not ids or len(ids) == 0:
        print("ℹ️  没有需要处理的数据，跳过向量化")
        return
    
    # 创建独立的数据库会话
    session = _create_local_session()
    
    try:
        print(f"🔍 正在查询 {len(ids)} 条记录...")
        # 使用 ORM 查询需要处理的记录
        metrics = session.query(MetricMetadata).filter(
            MetricMetadata.id.in_(ids)
        ).all()
        
        print(f"✅ 查询到 {len(metrics)} 条记录")
        
        # 准备文本数据（使用指标名称 + 同义词）- 参考 terminology 的逻辑
        texts = []
        for metric in metrics:
            text = metric.metric_name
            if metric.synonyms:
                text += f", {metric.synonyms}"
            texts.append(text)
        
        print(f"📝 准备了 {len(texts)} 个文本用于向量化")
        print(f"   示例文本：{texts[0] if texts else '无'}")
        
        # 计算 embedding - 延迟导入 EmbeddingModelCache 避免循环依赖
        # 只在真正需要生成 embedding 时才导入
        try:
            print("🚀 正在加载 Embedding 模型...")
            model = EmbeddingModelCache.get_model()
            print("✅ Embedding 模型加载成功")
            
            print("⏳ 开始计算 embedding 向量（这可能需要一些时间）...")
            import time
            start_time = time.time()
            results = model.embed_documents(texts)
            end_time = time.time()
            
            print(f"✅ Embedding 计算完成！耗时：{end_time - start_time:.2f}秒")
            print(f"   生成了 {len(results)} 个向量")
            if results and len(results) > 0:
                print(f"   向量维度：{len(results[0]) if isinstance(results[0], (list, tuple)) else '未知'}")
            
            # 更新数据库
            print("💾 正在更新数据库...")
            for index in range(len(results)):
                stmt = update(MetricMetadata).where(
                    MetricMetadata.id == metrics[index].id
                ).values(embedding_vector=results[index])
                session.execute(stmt)
                if (index + 1) % 10 == 0 or index == len(results) - 1:
                    print(f"   已更新 {index + 1}/{len(results)} 条记录")
            
            session.commit()
            print("✅ 数据库更新完成！")
            
        except FileNotFoundError as e:
            # 模型文件不存在
            print(f"⚠️  Embedding 模型文件不存在：{e}")
            print(f"💡 提示：请检查 LOCAL_MODEL_PATH 配置")
            print(f"   当前路径：{settings.LOCAL_MODEL_PATH}")
            print(f"   如需配置，请在.env 文件中设置 LOCAL_MODEL_PATH")
            session.rollback()
            raise
        except ImportError as e:
            print(f"❌ Embedding 模块导入失败：{e}")
            print("💡 提示：请检查 EMBEDDING_ENABLED 配置和 EmbeddingModelCache 是否可用")
            session.rollback()
            raise
        except Exception as e:
            print(f"❌ Embedding 计算过程中出错：{e}")
            import traceback
            traceback.print_exc()
            session.rollback()
            raise
    
    except Exception as e:
        print(f"❌ 向量化处理失败：{e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        session.close()
        print("🔒 数据库会话已关闭")


def _create_local_session():
    """
    创建一个独立的数据库会话（用于本地测试或后台任务）
    不依赖 SessionDep，直接从.env 文件读取配置
    
    Returns:
        session: SQLAlchemy 会话对象
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import os
    from dotenv import load_dotenv
    
    # 加载根目录的.env 文件
    env_path = Path(__file__).parent.parent.parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    
    # 从环境变量读取数据库配置
    db_host = os.getenv("POSTGRES_SERVER", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_user = os.getenv("POSTGRES_USER", "sqlbot")
    db_password = os.getenv("POSTGRES_PASSWORD", "sqlbot")
    db_name = os.getenv("POSTGRES_DB", "sqlbot")
    
    database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    # 创建引擎和会话
    engine = create_engine(database_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    return session


def _init_local_config():
    """
    初始化本地配置（用于本地测试或后台任务）
    确保从.env 文件读取所有配置项
    """
    import os
    from dotenv import load_dotenv
    
    # 加载根目录的.env 文件
    env_path = Path(__file__).parent.parent.parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    
    # 确保 LOCAL_MODEL_PATH 被正确设置
    local_model_path = os.getenv("LOCAL_MODEL_PATH")
    if local_model_path:
        # 规范化路径（处理 Windows 和 Linux 路径分隔符）
        local_model_path = os.path.normpath(local_model_path)
        
        # 如果是相对路径，转换为绝对路径
        if not Path(local_model_path).is_absolute():
            project_root = Path(__file__).parent.parent.parent.parent.parent
            local_model_path = str(project_root / local_model_path)
            local_model_path = os.path.normpath(local_model_path)
        
        # 更新 settings 中的配置
        settings.LOCAL_MODEL_PATH = local_model_path
        print(f"📁 LOCAL_MODEL_PATH 已设置为：{local_model_path}")
        
        # 验证路径是否存在
        if not Path(local_model_path).exists():
            print(f"⚠️  警告：模型路径不存在：{local_model_path}")
            # 尝试常见的路径格式
            alt_path = str(Path(__file__).parent.parent.parent.parent / "models")
            alt_path = os.path.normpath(alt_path)
            if Path(alt_path).exists():
                print(f"✅ 找到备用路径：{alt_path}")
                settings.LOCAL_MODEL_PATH = alt_path


def fill_empty_embeddings():
    """
    填充所有缺失的 embedding 向量
    参考 terminology 表的 run_fill_empty_embeddings 函数实现
    """
    # 先初始化本地配置，确保 LOCAL_MODEL_PATH 等配置项正确
    _init_local_config()
    
    print("🔍 开始检查 EMBEDDING_ENABLED 配置...")
    if not settings.EMBEDDING_ENABLED:
        print("ℹ️  EMBEDDING_ENABLED 未启用，跳过向量化")
        return
    
    print("✅ EMBEDDING_ENABLED 已启用")
    
    # 使用 ORM 方式查询
    print("🔍 正在查询需要向量化的记录...")
    try:
        # 创建独立的数据库会话
        session = _create_local_session()
        
        try:
            # 使用 ORM 查询
            from sqlalchemy import text
            sql = text("SELECT id FROM metric_metadata WHERE embedding_vector IS NULL")
            print(f"📝 执行 SQL: SELECT id FROM metric_metadata WHERE embedding_vector IS NULL")
            result = session.execute(sql)
            results = [row[0] for row in result.fetchall()]
            print(f"✅ 查询执行成功，找到 {len(results)} 条记录")
            
            if not results or len(results) == 0:
                print("✅ 所有指标已向量化，无需处理")
                return
            
            print(f"📊 发现 {len(results)} 条记录需要向量化")
            print(f"   ID 列表：{results[:10]}{'...' if len(results) > 10 else ''}")
            
            if results:
                print(f"⏳ 开始调用 _save_metric_embeddings...")
                # 关闭当前会话，在新会话中处理向量化
                session.close()
                _save_metric_embeddings(list(results))
                print(f"✅ 向量化处理完成")
        
        finally:
            session.close()
            print("🔒 数据库会话已关闭")
    
    except Exception as query_error:
        print(f"❌ 查询失败：{query_error}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        session.close()
        print("🔒 数据库会话已关闭")
