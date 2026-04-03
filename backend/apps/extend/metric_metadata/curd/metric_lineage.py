import datetime
from typing import List, Optional

from sqlalchemy import and_, select, func, delete, update
from apps.extend.metric_metadata.models.metric_lineage_model import MetricLineage, MetricDimension, MetricLineageInfo, MetricDimensionInfo
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
