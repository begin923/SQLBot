import datetime
import time
from typing import List, Optional

from apps import settings
from sqlalchemy import and_, select, func, delete, update
from apps.extend.metric_metadata.models.metric_lineage_model import MetricLineage, MetricDimension, MetricLineageInfo, MetricDimensionInfo
from apps.extend.utils.utils import Utils
from common.core.deps import SessionDep



# ========== MetricLineage CRUD 操作 ==========

def create_metric_lineage(session: SessionDep, info: MetricLineageInfo):
    """
    创建指标血缘记录
    
    Args:
        session: 数据库会话
        info: 指标血缘信息对象
    
    Returns:
        创建的记录
    """
    if not info.metric_column or not info.metric_column.strip():
        raise Exception("指标字段不能为空")
    
    if not info.table_name or not info.table_name.strip():
        raise Exception("表名不能为空")
    
    if not info.metric_name or not info.metric_name.strip():
        raise Exception("指标名称不能为空")
    
    create_time = datetime.datetime.now()
    
    # 检查是否已存在
    exists_query = session.query(MetricLineage).filter(
        and_(
            MetricLineage.metric_column == info.metric_column.strip(),
            MetricLineage.table_name == info.table_name.strip()
        )
    ).first()
    
    if exists_query:
        raise Exception(f"指标 {info.metric_column} 在表 {info.table_name} 中已存在")
    
    # 创建记录
    lineage = MetricLineage(
        metric_column=info.metric_column.strip(),
        table_name=info.table_name.strip(),
        metric_name=info.metric_name.strip(),
        synonyms=info.synonyms.strip() if info.synonyms else None,
        upstream_table=info.upstream_table.strip() if info.upstream_table else None,
        filter=info.filter.strip() if info.filter else None,
        calc_logic=info.calc_logic.strip() if info.calc_logic else None,
        dw_layer=info.dw_layer.strip() if info.dw_layer else None,
        create_time=create_time
    )
    
    session.add(lineage)
    session.commit()
    session.refresh(lineage)
    
    return lineage


def batch_create_metric_lineage(session: SessionDep, info_list: List[MetricLineageInfo]):
    """
    批量创建或更新指标血缘记录
    
    Args:
        session: 数据库会话
        info_list: 指标血缘信息列表
    
    Returns:
        处理结果统计
    """
    if not info_list:
        return {
            'success_count': 0,
            'failed_records': [],
            'duplicate_count': 0,
            'update_count': 0,
            'original_count': 0
        }
    
    failed_records = []
    success_count = 0
    update_count = 0
    
    # 去重处理
    unique_key_set = set()
    deduplicated_list = []
    duplicate_count = 0
    
    for info in info_list:
        unique_key = (
            info.metric_column.strip().lower() if info.metric_column else '',
            info.table_name.strip().lower() if info.table_name else ''
        )
        
        if unique_key in unique_key_set:
            duplicate_count += 1
            continue
        
        unique_key_set.add(unique_key)
        deduplicated_list.append(info)
    
    # 批量插入或更新
    for info in deduplicated_list:
        try:
            # 检查是否已存在
            exists_query = session.query(MetricLineage).filter(
                and_(
                    MetricLineage.metric_column == info.metric_column.strip(),
                    MetricLineage.table_name == info.table_name.strip()
                )
            ).first()
            
            if exists_query:
                # 已存在则更新
                stmt = update(MetricLineage).where(
                    and_(
                        MetricLineage.metric_column == info.metric_column.strip(),
                        MetricLineage.table_name == info.table_name.strip()
                    )
                ).values(
                    metric_name=info.metric_name.strip() if info.metric_name else None,
                    synonyms=info.synonyms.strip() if info.synonyms else None,
                    upstream_table=info.upstream_table.strip() if info.upstream_table else None,
                    filter=info.filter.strip() if info.filter else None,
                    calc_logic=info.calc_logic.strip() if info.calc_logic else None,
                    dw_layer=info.dw_layer.strip() if info.dw_layer else None,
                )
                session.execute(stmt)
                update_count += 1
            else:
                # 不存在则创建
                create_metric_lineage(session, info)
            
            success_count += 1
        except Exception as e:
            failed_records.append({
                'data': info,
                'errors': [str(e)]
            })
    
    session.commit()
    
    return {
        'success_count': success_count,
        'update_count': update_count,
        'failed_records': failed_records,
        'duplicate_count': duplicate_count,
        'original_count': len(info_list),
        'deduplicated_count': len(deduplicated_list)
    }


def update_metric_lineage(session: SessionDep, info: MetricLineageInfo):
    """
    更新指标血缘记录
    
    Args:
        session: 数据库会话
        info: 指标血缘信息对象
    
    Returns:
        更新的记录
    """
    if not info.metric_column or not info.table_name:
        raise Exception("指标字段和表名不能为空")
    
    count = session.query(MetricLineage).filter(
        and_(
            MetricLineage.metric_column == info.metric_column,
            MetricLineage.table_name == info.table_name
        )
    ).count()
    
    if count == 0:
        raise Exception("指标血缘不存在")
    
    stmt = update(MetricLineage).where(
        and_(
            MetricLineage.metric_column == info.metric_column,
            MetricLineage.table_name == info.table_name
        )
    ).values(
        metric_name=info.metric_name.strip() if info.metric_name else None,
        synonyms=info.synonyms.strip() if info.synonyms else None,
        upstream_table=info.upstream_table.strip() if info.upstream_table else None,
        filter=info.filter.strip() if info.filter else None,
        calc_logic=info.calc_logic.strip() if info.calc_logic else None,
        dw_layer=info.dw_layer.strip() if info.dw_layer else None,
    )
    
    session.execute(stmt)
    session.commit()
    
    return info


def delete_metric_lineage(session: SessionDep, keys: List[tuple]):
    """
    删除指标血缘记录
    
    Args:
        session: 数据库会话
        keys: 要删除的记录键列表 [(metric_column, table_name), ...]
    """
    from sqlalchemy import or_
    conditions = [
        and_(
            MetricLineage.metric_column == key[0],
            MetricLineage.table_name == key[1]
        )
        for key in keys
    ]
    
    stmt = delete(MetricLineage).where(or_(*conditions))
    session.execute(stmt)
    session.commit()


def delete_metric_lineage_by_table(session: SessionDep, table_name: str):
    """
    根据表名删除所有指标血缘记录
    
    Args:
        session: 数据库会话
        table_name: 表名
    """
    stmt = delete(MetricLineage).where(MetricLineage.table_name == table_name)
    session.execute(stmt)
    session.commit()


def get_metric_lineage_by_key(session: SessionDep, metric_column: str, table_name: str) -> Optional[MetricLineageInfo]:
    """
    根据键查询指标血缘
    
    Args:
        session: 数据库会话
        metric_column: 指标字段
        table_name: 表名
    
    Returns:
        指标血缘信息对象
    """
    lineage = session.query(MetricLineage).filter(
        and_(
            MetricLineage.metric_column == metric_column,
            MetricLineage.table_name == table_name
        )
    ).first()
    
    if not lineage:
        return None
    
    return MetricLineageInfo(
        metric_column=lineage.metric_column,
        table_name=lineage.table_name,
        metric_name=lineage.metric_name,
        synonyms=lineage.synonyms,
        upstream_table=lineage.upstream_table,
        filter=lineage.filter,
        calc_logic=lineage.calc_logic,
        dw_layer=lineage.dw_layer,
        enabled=True
    )


def get_metric_lineage_by_names(session, metric_names: List[str], datasource_id: Optional[int] = None) -> List[MetricLineageInfo]:
    """
    根据指标名称列表查询指标血缘
    
    Args:
        session: 数据库会话
        metric_names: 指标名称列表
        datasource_id: 数据源 ID（可选，用于过滤）
    
    Returns:
        指标血缘信息对象列表
    """
    if not metric_names or len(metric_names) == 0:
        return []
    
    _list: List[MetricLineage] = []
    
    # 构建查询条件
    conditions = [MetricLineage.metric_name.in_(metric_names)]
    
    results = session.query(MetricLineage).filter(and_(*conditions)).all()
    
    for lineage in results:
        _list.append(lineage)
    
    # 转换为返回格式
    return _convert_lineage_to_info_list(_list)


def page_metric_lineage(session: SessionDep, current_page: int = 1, page_size: int = 10,
                       metric_name: Optional[str] = None,
                       table_name: Optional[str] = None,
                       dw_layer: Optional[str] = None):
    """
    分页查询指标血缘
    
    Args:
        session: 数据库会话
        current_page: 当前页码
        page_size: 每页数量
        metric_name: 指标名称（支持模糊查询）
        table_name: 表名
        dw_layer: 数仓分层
    
    Returns:
        分页结果
    """
    # 构建查询条件
    conditions = []
    if metric_name and metric_name.strip():
        conditions.append(MetricLineage.metric_name.ilike(f"%{metric_name.strip()}%"))
    if table_name and table_name.strip():
        conditions.append(MetricLineage.table_name == table_name.strip())
    if dw_layer and dw_layer.strip():
        conditions.append(MetricLineage.dw_layer == dw_layer.strip())
    
    # 查询总数
    if conditions:
        count_stmt = select(func.count()).select_from(MetricLineage).where(and_(*conditions))
    else:
        count_stmt = select(func.count()).select_from(MetricLineage)
    
    total_count = session.execute(count_stmt).scalar()
    
    # 分页处理
    page_size = max(10, page_size)
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
    current_page = max(1, min(current_page, total_pages)) if total_pages > 0 else 1
    
    # 查询数据
    stmt = select(MetricLineage)
    if conditions:
        stmt = stmt.where(and_(*conditions))
    
    stmt = stmt.order_by(MetricLineage.create_time.desc())
    stmt = stmt.offset((current_page - 1) * page_size).limit(page_size)
    
    results = session.execute(stmt).scalars().all()
    
    _list = []
    for lineage in results:
        _list.append(MetricLineageInfo(
            metric_column=lineage.metric_column,
            table_name=lineage.table_name,
            metric_name=lineage.metric_name,
            synonyms=lineage.synonyms,
            upstream_table=lineage.upstream_table,
            filter=lineage.filter,
            calc_logic=lineage.calc_logic,
            dw_layer=lineage.dw_layer,
            enabled=True
        ))
    
    return current_page, page_size, total_count, total_pages, _list


def get_all_metric_lineage(session: SessionDep,
                           metric_name: Optional[str] = None,
                           table_name: Optional[str] = None,
                           dw_layer: Optional[str] = None):
    """
    获取所有指标血缘（不分页）
    
    Args:
        session: 数据库会话
        metric_name: 指标名称（支持模糊查询）
        table_name: 表名
        dw_layer: 数仓分层
    
    Returns:
        指标血缘列表
    """
    conditions = []
    if metric_name and metric_name.strip():
        conditions.append(MetricLineage.metric_name.ilike(f"%{metric_name.strip()}%"))
    if table_name and table_name.strip():
        conditions.append(MetricLineage.table_name == table_name.strip())
    if dw_layer and dw_layer.strip():
        conditions.append(MetricLineage.dw_layer == dw_layer.strip())
    
    stmt = select(MetricLineage)
    if conditions:
        stmt = stmt.where(and_(*conditions))
    
    stmt = stmt.order_by(MetricLineage.create_time.desc())
    
    results = session.execute(stmt).scalars().all()
    
    _list = []
    for lineage in results:
        _list.append(MetricLineageInfo(
            metric_column=lineage.metric_column,
            table_name=lineage.table_name,
            metric_name=lineage.metric_name,
            synonyms=lineage.synonyms,
            upstream_table=lineage.upstream_table,
            filter=lineage.filter,
            calc_logic=lineage.calc_logic,
            dw_layer=lineage.dw_layer,
            enabled=True
        ))
    
    return _list


def _convert_lineage_to_info_list(lineages: List[MetricLineage]) -> List[MetricLineageInfo]:
    """
    将 MetricLineage 对象列表转换为 MetricLineageInfo 对象列表
    
    Args:
        lineages: MetricLineage 对象列表
    
    Returns:
        MetricLineageInfo 对象列表
    """
    result_list = []
    for lineage in lineages:
        result_list.append(MetricLineageInfo(
            metric_column=lineage.metric_column,
            table_name=lineage.table_name,
            metric_name=lineage.metric_name,
            synonyms=lineage.synonyms,
            upstream_table=lineage.upstream_table,
            filter=lineage.filter,
            calc_logic=lineage.calc_logic,
            dw_layer=lineage.dw_layer,
            enabled=True
        ))
    
    return result_list


# ========== MetricDimension CRUD 操作 ==========

def create_metric_dimension(session: SessionDep, info: MetricDimensionInfo):
    """
    创建指标维度记录
    
    Args:
        session: 数据库会话
        info: 指标维度信息对象
    
    Returns:
        创建的记录
    """
    if not info.table_name or not info.table_name.strip():
        raise Exception("表名不能为空")
    
    if not info.dim_column or not info.dim_column.strip():
        raise Exception("维度字段不能为空")
    
    create_time = datetime.datetime.now()
    
    # 检查是否已存在
    exists_query = session.query(MetricDimension).filter(
        and_(
            MetricDimension.table_name == info.table_name.strip(),
            MetricDimension.dim_column == info.dim_column.strip()
        )
    ).first()
    
    if exists_query:
        raise Exception(f"维度 {info.dim_column} 在表 {info.table_name} 中已存在")
    
    # 创建记录
    dimension = MetricDimension(
        table_name=info.table_name.strip(),
        dim_column=info.dim_column.strip(),
        dim_name=info.dim_name.strip() if info.dim_name else None,
        create_time=create_time
    )
    
    session.add(dimension)
    session.commit()
    session.refresh(dimension)
    
    return dimension


def batch_create_metric_dimension(session: SessionDep, info_list: List[MetricDimensionInfo]):
    """
    批量创建或更新指标维度记录
    
    Args:
        session: 数据库会话
        info_list: 指标维度信息列表
    
    Returns:
        处理结果统计
    """
    if not info_list:
        return {
            'success_count': 0,
            'failed_records': [],
            'duplicate_count': 0,
            'update_count': 0,
            'original_count': 0
        }
    
    failed_records = []
    success_count = 0
    update_count = 0
    
    # 去重处理
    unique_key_set = set()
    deduplicated_list = []
    duplicate_count = 0
    
    for info in info_list:
        unique_key = (
            info.table_name.strip().lower() if info.table_name else '',
            info.dim_column.strip().lower() if info.dim_column else ''
        )
        
        if unique_key in unique_key_set:
            duplicate_count += 1
            continue
        
        unique_key_set.add(unique_key)
        deduplicated_list.append(info)
    
    # 批量插入或更新
    for info in deduplicated_list:
        try:
            # 检查是否已存在
            exists_query = session.query(MetricDimension).filter(
                and_(
                    MetricDimension.table_name == info.table_name.strip(),
                    MetricDimension.dim_column == info.dim_column.strip()
                )
            ).first()
            
            if exists_query:
                # 已存在则更新
                stmt = update(MetricDimension).where(
                    and_(
                        MetricDimension.table_name == info.table_name.strip(),
                        MetricDimension.dim_column == info.dim_column.strip()
                    )
                ).values(
                    dim_name=info.dim_name.strip() if info.dim_name else None,
                )
                session.execute(stmt)
                update_count += 1
            else:
                # 不存在则创建
                create_metric_dimension(session, info)
            
            success_count += 1
        except Exception as e:
            failed_records.append({
                'data': info,
                'errors': [str(e)]
            })
    
    session.commit()
    
    # 为新创建的维度计算 embedding
    if success_count > 0 and not failed_records:
        try:
            from apps.ai_model.embedding import EmbeddingModelCache
            embedding_model = EmbeddingModelCache.get_model()
            save_dimension_embeddings(session, embedding_model, keys=None)  # 更新所有 NULL 向量的记录
        except Exception as e:
            print(f"Warning: Failed to save dimension embeddings: {e}")
    
    return {
        'success_count': success_count,
        'update_count': update_count,
        'failed_records': failed_records,
        'duplicate_count': duplicate_count,
        'original_count': len(info_list),
        'deduplicated_count': len(deduplicated_list)
    }


def delete_metric_dimension(session: SessionDep, keys: List[tuple]):
    """
    删除指标维度记录
    
    Args:
        session: 数据库会话
        keys: 要删除的记录键列表 [(table_name, dim_column), ...]
    """
    from sqlalchemy import or_
    conditions = [
        and_(
            MetricDimension.table_name == key[0],
            MetricDimension.dim_column == key[1]
        )
        for key in keys
    ]
    
    stmt = delete(MetricDimension).where(or_(*conditions))
    session.execute(stmt)
    session.commit()


def delete_metric_dimension_by_table(session: SessionDep, table_name: str):
    """
    根据表名删除所有指标维度记录
    
    Args:
        session: 数据库会话
        table_name: 表名
    """
    stmt = delete(MetricDimension).where(MetricDimension.table_name == table_name)
    session.execute(stmt)
    session.commit()


def get_metric_dimension_by_key(session: SessionDep, table_name: str, dim_column: str) -> Optional[MetricDimensionInfo]:
    """
    根据键查询指标维度
    
    Args:
        session: 数据库会话
        table_name: 表名
        dim_column: 维度字段
    
    Returns:
        指标维度信息对象
    """
    dimension = session.query(MetricDimension).filter(
        and_(
            MetricDimension.table_name == table_name,
            MetricDimension.dim_column == dim_column
        )
    ).first()
    
    if not dimension:
        return None
    
    return MetricDimensionInfo(
        table_name=dimension.table_name,
        dim_column=dimension.dim_column,
        dim_name=dimension.dim_name,
        enabled=True
    )


def get_metric_dimensions_by_table(session: SessionDep, table_name: str) -> List[MetricDimensionInfo]:
    """
    根据表名查询所有维度
    
    Args:
        session: 数据库会话
        table_name: 表名
    
    Returns:
        指标维度信息对象列表
    """
    dimensions = session.query(MetricDimension).filter(
        MetricDimension.table_name == table_name
    ).all()
    
    return _convert_dimension_to_info_list(dimensions)


def page_metric_dimension(session: SessionDep, current_page: int = 1, page_size: int = 10,
                          table_name: Optional[str] = None):
    """
    分页查询指标维度
    
    Args:
        session: 数据库会话
        current_page: 当前页码
        page_size: 每页数量
        table_name: 表名
    
    Returns:
        分页结果
    """
    # 构建查询条件
    conditions = []
    if table_name and table_name.strip():
        conditions.append(MetricDimension.table_name == table_name.strip())
    
    # 查询总数
    if conditions:
        count_stmt = select(func.count()).select_from(MetricDimension).where(and_(*conditions))
    else:
        count_stmt = select(func.count()).select_from(MetricDimension)
    
    total_count = session.execute(count_stmt).scalar()
    
    # 分页处理
    page_size = max(10, page_size)
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
    current_page = max(1, min(current_page, total_pages)) if total_pages > 0 else 1
    
    # 查询数据
    stmt = select(MetricDimension)
    if conditions:
        stmt = stmt.where(and_(*conditions))
    
    stmt = stmt.order_by(MetricDimension.create_time.desc())
    stmt = stmt.offset((current_page - 1) * page_size).limit(page_size)
    
    results = session.execute(stmt).scalars().all()
    
    _list = []
    for dimension in results:
        _list.append(MetricDimensionInfo(
            table_name=dimension.table_name,
            dim_column=dimension.dim_column,
            dim_name=dimension.dim_name,
            enabled=True
        ))
    
    return current_page, page_size, total_count, total_pages, _list


def get_all_metric_dimension(session: SessionDep,
                             table_name: Optional[str] = None):
    """
    获取所有指标维度（不分页）
    
    Args:
        session: 数据库会话
        table_name: 表名
    
    Returns:
        指标维度列表
    """
    conditions = []
    if table_name and table_name.strip():
        conditions.append(MetricDimension.table_name == table_name.strip())
    
    stmt = select(MetricDimension)
    if conditions:
        stmt = stmt.where(and_(*conditions))
    
    stmt = stmt.order_by(MetricDimension.create_time.desc())
    
    results = session.execute(stmt).scalars().all()
    
    _list = []
    for dimension in results:
        _list.append(MetricDimensionInfo(
            table_name=dimension.table_name,
            dim_column=dimension.dim_column,
            dim_name=dimension.dim_name,
            enabled=True
        ))
    
    return _list


def _convert_dimension_to_info_list(dimensions: List[MetricDimension]) -> List[MetricDimensionInfo]:
    """
    将 MetricDimension 对象列表转换为 MetricDimensionInfo 对象列表
    
    Args:
        dimensions: MetricDimension 对象列表
    
    Returns:
        MetricDimensionInfo 对象列表
    """
    result_list = []
    for dimension in dimensions:
        result_list.append(MetricDimensionInfo(
            table_name=dimension.table_name,
            dim_column=dimension.dim_column,
            dim_name=dimension.dim_name,
            enabled=True
        ))
    
    return result_list


# ========== MetricDimension 向量检索功能 ==========

def _search_by_exact(session: SessionDep, search_text: str, table_name: Optional[str] = None) -> List[MetricDimension]:
    """
    精准匹配搜索
    
    Args:
        session: 数据库会话
        search_text: 搜索文本
        table_name: 表名（可选，用于过滤）
    
    Returns:
        匹配的维度列表
    """
    import time
    start_time = time.time()
    
    try:
        # 先拼接 table_name 条件，再拼接 dim_name 条件（优化查询性能）
        conditions = []
        if table_name:
            conditions.append(MetricDimension.table_name == table_name)
        conditions.append(MetricDimension.dim_name == search_text)
        
        # 构建查询并执行
        results = session.query(MetricDimension).filter(and_(*conditions)).all()
        
        elapsed_time = time.time() - start_time
        print(f"✅ [Exact] 精准匹配搜索完成，找到 {len(results)} 条结果，耗时: {elapsed_time:.3f}s")
        return results
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"❌ [Exact] 精准匹配搜索失败，耗时: {elapsed_time:.3f}s, 错误: {str(e)}")
        raise


def _search_by_fuzzy(session: SessionDep, search_text: str, table_name: Optional[str] = None) -> List[MetricDimension]:
    """
    模糊匹配搜索
    
    Args:
        session: 数据库会话
        search_text: 搜索文本
        table_name: 表名（可选，用于过滤）
    
    Returns:
        匹配的维度列表
    """
    import time
    start_time = time.time()
    
    try:
        conditions = [MetricDimension.dim_name.ilike(f"%{search_text}%")]
        if table_name:
            conditions.append(MetricDimension.table_name == table_name)
        
        # 构建查询并执行
        results = session.query(MetricDimension).filter(and_(*conditions)).all()
        
        elapsed_time = time.time() - start_time
        print(f"✅ [Fuzzy] 模糊匹配搜索完成，找到 {len(results)} 条结果，耗时: {elapsed_time:.3f}s")
        return results
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"❌ [Fuzzy] 模糊匹配搜索失败，耗时: {elapsed_time:.3f}s, 错误: {str(e)}")
        raise


def _search_by_vector(session: SessionDep, search_text: str, embedding_model, top_k: int = 1, table_name: Optional[str] = None) -> List[MetricDimensionInfo]:
    """
    向量语义搜索（直接返回 MetricDimensionInfo，不包含 embedding_vector）
    
    Args:
        session: 数据库会话
        search_text: 搜索文本
        embedding_model: 向量模型（必选）
        top_k: 返回结果数量限制
        table_name: 表名（可选，用于过滤）
    
    Returns:
        指标维度信息对象列表（不含 embedding_vector）
    """
    import time
    start_time = time.time()
    
    try:
        from sqlalchemy import text
        
        # 使用传入的向量模型
        embedding = embedding_model.embed_query(search_text)
        
        similarity_threshold = getattr(settings, 'EMBEDDING_DIMENSION_SIMILARITY', 0.75)
        vector_top_k = getattr(settings, 'EMBEDDING_DIMENSION_TOP_COUNT', top_k)
        
        # 使用 CTE + JOIN 一次性获取完整对象并保持排序（排除 embedding_vector 字段）
        if table_name:
            # 有表名过滤：在 CTE 和主查询中都添加过滤
            cte_sql = text("""
                WITH vector_matches AS (
                    SELECT 
                        table_name,
                        dim_column,
                        (1 - (embedding_vector <=> :embedding_array)) AS similarity
                    FROM metric_dimension
                    WHERE table_name = :table_name
                      AND embedding_vector IS NOT NULL
                      AND (1 - (embedding_vector <=> :embedding_array)) > :threshold
                    ORDER BY similarity DESC
                    LIMIT :limit
                )
                SELECT md.table_name, md.dim_column, md.dim_name, md.create_time
                FROM metric_dimension md
                INNER JOIN vector_matches vm 
                    ON md.table_name = vm.table_name 
                    AND md.dim_column = vm.dim_column
                WHERE md.table_name = :table_name
                ORDER BY vm.similarity DESC
            """)
        else:
            # 无表名过滤
            cte_sql = text("""
                WITH vector_matches AS (
                    SELECT 
                        table_name,
                        dim_column,
                        (1 - (embedding_vector <=> :embedding_array)) AS similarity
                    FROM metric_dimension
                    WHERE embedding_vector IS NOT NULL
                      AND (1 - (embedding_vector <=> :embedding_array)) > :threshold
                    ORDER BY similarity DESC
                    LIMIT :limit
                )
                SELECT md.table_name, md.dim_column, md.dim_name, md.create_time
                FROM metric_dimension md
                INNER JOIN vector_matches vm 
                    ON md.table_name = vm.table_name 
                    AND md.dim_column = vm.dim_column
                ORDER BY vm.similarity DESC
            """)
        
        # 执行查询
        params = {
            'embedding_array': str(embedding),
            'threshold': similarity_threshold,
            'limit': vector_top_k
        }
        
        # 添加表名参数
        if table_name:
            params['table_name'] = table_name
        
        # 执行查询
        results = session.execute(cte_sql, params).fetchall()
        
        # 转换为 MetricDimensionInfo 对象
        dimensions_info = [
            MetricDimensionInfo(
                table_name=row.table_name,
                dim_column=row.dim_column,
                dim_name=row.dim_name,
                enabled=True
            )
            for row in results
        ]
        
        elapsed_time = time.time() - start_time
        print(f"✅ [Vector] 向量语义搜索完成，找到 {len(dimensions_info)} 条结果，耗时: {elapsed_time:.3f}s")
        return dimensions_info
    
    except Exception as e:
        elapsed_time = time.time() - start_time
        import traceback
        traceback.print_exc()
        print(f"❌ [Vector] 向量语义搜索失败，耗时: {elapsed_time:.3f}s, 错误: {str(e)}")
        return []

def search_metric_dimensions(session: SessionDep, 
                             search_text: str, 
                             embedding_model,
                             table_name: Optional[str] = None,
                             datasource_id: Optional[int] = None,
                             search_mode: str = 'hybrid',
                             top_k: int = 1) -> List[MetricDimensionInfo]:
    """
    搜索指标维度（支持精准、模糊、向量检索）
    
    Args:
        session: 数据库会话
        search_text: 搜索文本
        embedding_model: 向量模型（必选）
        table_name: 表名（可选，用于过滤）
        datasource_id: 数据源 ID（可选）
        search_mode: 搜索模式 - 'exact'(精准), 'fuzzy'(模糊), 'vector'(向量), 'hybrid'(混合，默认)
        top_k: 返回结果数量限制
    
    Returns:
        指标维度信息对象列表
    """
    import time
    start_time = time.time()
    print(f"\n{'='*60}")
    print(f"🔎 [Search] 开始搜索维度")
    print(f"   - 搜索文本: '{search_text}'")
    print(f"   - 表名过滤: {table_name if table_name else '无'}")
    print(f"   - 搜索模式: {search_mode}")
    print(f"   - 返回数量: top_k={top_k}")
    print(f"{'='*60}")
    
    if not search_text or not search_text.strip():
        print("⚠️  [Search] 搜索文本为空，返回空列表")
        return []
    
    search_text = search_text.strip()
    
    # Hybrid 模式（默认）：三级检索，匹配到即返回
    if search_mode == 'hybrid':
        # ========== Hybrid 模式：三级检索，匹配到即返回 ==========
        
        # 第 1 级：精准匹配
        results = _search_by_exact(session, search_text, table_name)
        if results:
            total_time = time.time() - start_time
            print(f"\n{'='*60}")
            print(f"🏁 [Search] 搜索完成（精准匹配），总耗时: {total_time:.3f}s")
            print(f"{'='*60}\n")
            return _convert_dimension_to_info_list(results)
        
        # 第 2 级：模糊匹配
        results = _search_by_fuzzy(session, search_text, table_name)
        if results:
            total_time = time.time() - start_time
            print(f"\n{'='*60}")
            print(f"🏁 [Search] 搜索完成（模糊匹配），总耗时: {total_time:.3f}s")
            print(f"{'='*60}\n")
            return _convert_dimension_to_info_list(results)
        
        # 第 3 级：向量语义搜索
        results = _search_by_vector(session, search_text, embedding_model, top_k, table_name)  # 直接返回 MetricDimensionInfo
        
        total_time = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"🏁 [Search] 搜索完成（向量匹配），总耗时: {total_time:.3f}s")
        print(f"{'='*60}\n")
        return results  # 直接返回，不需要转换
    
    elif search_mode == 'exact':
        # ========== 精准匹配模式 ==========
        results = _search_by_exact(session, search_text, table_name)
        total_time = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"🏁 [Search] 搜索完成（精准模式），总耗时: {total_time:.3f}s")
        print(f"{'='*60}\n")
        return _convert_dimension_to_info_list(results)
    
    elif search_mode == 'fuzzy':
        # ========== 模糊匹配模式 ==========
        results = _search_by_fuzzy(session, search_text, table_name)
        total_time = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"🏁 [Search] 搜索完成（模糊模式），总耗时: {total_time:.3f}s")
        print(f"{'='*60}\n")
        return _convert_dimension_to_info_list(results)
    
    elif search_mode == 'vector':
        # ========== 向量检索模式 ==========
        results = _search_by_vector(session, search_text, embedding_model, top_k, table_name)  # 直接返回 MetricDimensionInfo
        total_time = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"🏁 [Search] 搜索完成，总耗时: {total_time:.3f}s")
        print(f"{'='*60}\n")
        return results  # 直接返回，不需要转换
    
    else:
        # 无效的搜索模式，返回空列表
        print(f"⚠️  无效的搜索模式 '{search_mode}'，支持的模式: hybrid, exact, fuzzy, vector")
        elapsed_time = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"🏁 [Search] 搜索完成（无效模式），总耗时: {elapsed_time:.3f}s")
        print(f"{'='*60}\n")
        return []


def save_dimension_embeddings(session: SessionDep, embedding_model, keys: List[tuple] = None):
    """
    为指标维度计算并保存 embedding 向量
    
    Args:
        session: 数据库会话
        embedding_model: 向量模型（必选）
        keys: 要更新向量的键列表 [(table_name, dim_column), ...]，如果为 None 则更新所有 NULL 向量的记录
    """
    try:
        # 确定要更新的记录
        if keys:
            # 更新指定的记录
            from sqlalchemy import or_
            conditions = [
                and_(
                    MetricDimension.table_name == key[0],
                    MetricDimension.dim_column == key[1]
                )
                for key in keys
            ]
            stmt = select(MetricDimension).where(or_(*conditions))
        else:
            # 更新所有 embedding_vector 为 NULL 的记录
            stmt = select(MetricDimension).where(MetricDimension.embedding_vector.is_(None))
        
        dimensions = session.execute(stmt).scalars().all()
        
        if not dimensions:
            print("No dimensions need embedding update")
            return
        
        print(f"Starting to embed {len(dimensions)} dimensions...")
        
        # 批量计算 embedding
        texts_to_embed = []
        dimension_map = {}  # (table_name, dim_column) -> dimension object
        
        for dim in dimensions:
            # 使用 dim_name 作为 embedding 的文本，如果没有 dim_name 则使用 dim_column
            text_to_embed = dim.dim_name if dim.dim_name and dim.dim_name.strip() else dim.dim_column
            texts_to_embed.append(text_to_embed)
            dimension_map[(dim.table_name, dim.dim_column)] = dim
        
        # 批量生成 embeddings
        if texts_to_embed:
            embeddings = embedding_model.embed_documents(texts_to_embed)
            
            # 更新数据库
            for idx, (key, dim) in enumerate(dimension_map.items()):
                if idx < len(embeddings):
                    embedding_vector = embeddings[idx]
                    
                    # 更新记录
                    update_stmt = update(MetricDimension).where(
                        and_(
                            MetricDimension.table_name == key[0],
                            MetricDimension.dim_column == key[1]
                        )
                    ).values(
                        embedding_vector=embedding_vector
                    )
                    session.execute(update_stmt)
            
            session.commit()
            print(f"✅ Successfully embedded {len(embeddings)} dimensions")
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Failed to save dimension embeddings: {str(e)}")
        session.rollback()


if __name__ == '__main__':
    # 获取向量模型
    from apps.ai_model.embedding import EmbeddingModelCache
    embedding_model = EmbeddingModelCache.get_model()
    session = Utils.create_local_session()
    # save_dimension_embeddings(session, embedding_model)
    start_time = time.time()
    res = search_metric_dimensions(session, '断奶时间', embedding_model, 'yz_datawarehouse_ads.ads_anc_idx_female_wean_info', 'hybrid')
    end_time = time.time()
    print(f"耗时: {end_time - start_time:.2f}s")
    print(res)
