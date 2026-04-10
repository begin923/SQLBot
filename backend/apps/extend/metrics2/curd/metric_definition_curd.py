from datetime import datetime
from typing import List, Optional
from sqlalchemy import and_, or_, select, insert, update, delete, text, func
from sqlmodel import Session

from apps.extend.metrics2.models.metric_definition_model import MetricDefinition, MetricDefinitionInfo


def create_metric_definition(session: Session, info: MetricDefinitionInfo):
    """
    创建单个指标定义记录

    Args:
        session: 数据库会话
        info: 指标定义信息对象

    Returns:
        创建的记录 ID
    """
    # 基本验证
    if not info.metric_name or not info.metric_name.strip():
        raise Exception("指标名称不能为空")

    if not info.metric_code or not info.metric_code.strip():
        raise Exception("指标编码不能为空")

    if not info.metric_type or not info.metric_type.strip():
        raise Exception("指标类型不能为空")

    if not info.biz_domain or not info.biz_domain.strip():
        raise Exception("业务域不能为空")

    # 检查是否已存在
    exists_query = session.query(MetricDefinition).filter(
        and_(
            MetricDefinition.metric_code == info.metric_code.strip(),
            MetricDefinition.biz_domain == info.biz_domain.strip()
        )
    ).first()

    if exists_query:
        raise Exception(f"指标 {info.metric_name} 已存在")

    # 创建记录
    metric_def = MetricDefinition(
        metric_id=info.metric_id.strip() if info.metric_id else None,
        metric_name=info.metric_name.strip(),
        metric_code=info.metric_code.strip(),
        metric_type=info.metric_type.strip(),
        biz_domain=info.biz_domain.strip(),
        cal_logic=info.cal_logic.strip() if info.cal_logic else None,
        unit=info.unit.strip() if info.unit else None,
        status=1 if info.status else 0
        # owner 字段已注释
    )

    session.add(metric_def)
    session.flush()
    session.refresh(metric_def)

    session.commit()

    return metric_def.metric_id


def batch_create_metric_definition(session: Session, info_list: List[MetricDefinitionInfo]):
    """
    批量创建指标定义记录

    Args:
        session: 数据库会话
        info_list: 指标定义信息列表

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
            info.metric_code.strip().lower() if info.metric_code else '',
            info.biz_domain.strip().lower() if info.biz_domain else ''
        )

        if unique_key in unique_key_set:
            duplicate_count += 1
            continue

        unique_key_set.add(unique_key)
        deduplicated_list.append(info)

    # 批量插入
    for info in deduplicated_list:
        try:
            metric_id = create_metric_definition(session, info)
            inserted_ids.append(metric_id)
            success_count += 1
        except Exception as e:
            failed_records.append({
                'data': info,
                'errors': [str(e)]
            })

    return {
        'success_count': success_count,
        'failed_records': failed_records,
        'duplicate_count': duplicate_count,
        'original_count': len(info_list),
        'deduplicated_count': len(deduplicated_list)
    }


def update_metric_definition(session: Session, info: MetricDefinitionInfo):
    """
    更新指标定义记录

    Args:
        session: 数据库会话
        info: 指标定义信息对象

    Returns:
        更新的记录 ID
    """
    if not info.metric_id:
        raise Exception("ID 不能为空")

    count = session.query(MetricDefinition).filter(
        MetricDefinition.metric_id == info.metric_id
    ).count()

    if count == 0:
        raise Exception("指标定义不存在")

    stmt = update(MetricDefinition).where(
        MetricDefinition.metric_id == info.metric_id
    ).values(
        metric_name=info.metric_name.strip() if info.metric_name else None,
        metric_code=info.metric_code.strip() if info.metric_code else None,
        metric_type=info.metric_type.strip() if info.metric_type else None,
        biz_domain=info.biz_domain.strip() if info.biz_domain else None,
        cal_logic=info.cal_logic.strip() if info.cal_logic else None,
        unit=info.unit.strip() if info.unit else None,
        status=1 if info.status else 0
        # owner 字段已注释
    )

    session.execute(stmt)
    session.commit()

    return info.metric_id


def delete_metric_definition(session: Session, metric_ids: List[str]):
    """
    删除指标定义记录

    Args:
        session: 数据库会话
        metric_ids: 要删除的指标ID列表
    """
    stmt = delete(MetricDefinition).where(MetricDefinition.metric_id.in_(metric_ids))
    session.execute(stmt)
    session.commit()


def get_metric_definition_by_id(session: Session, metric_id: str) -> Optional[MetricDefinitionInfo]:
    """
    根据ID查询指标定义

    Args:
        session: 数据库会话
        metric_id: 指标ID

    Returns:
        指标定义信息对象
    """
    metric_def = session.query(MetricDefinition).filter(MetricDefinition.metric_id == metric_id).first()

    if not metric_def:
        return None

    # owner 字段已注释，不再返回
    return MetricDefinitionInfo(
        metric_id=metric_def.metric_id,
        metric_name=metric_def.metric_name,
        metric_code=metric_def.metric_code,
        metric_type=metric_def.metric_type,
        biz_domain=metric_def.biz_domain,
        cal_logic=metric_def.cal_logic,
        unit=metric_def.unit,
        status=bool(metric_def.status)
    )


def get_metric_definition_by_code(session: Session, metric_code: str) -> Optional[MetricDefinitionInfo]:
    """
    根据编码查询指标定义

    Args:
        session: 数据库会话
        metric_code: 指标编码

    Returns:
        指标定义信息对象
    """
    metric_def = session.query(MetricDefinition).filter(MetricDefinition.metric_code == metric_code).first()

    if not metric_def:
        return None

    # owner 字段已注释，不再返回
    return MetricDefinitionInfo(
        metric_id=metric_def.metric_id,
        metric_name=metric_def.metric_name,
        metric_code=metric_def.metric_code,
        metric_type=metric_def.metric_type,
        biz_domain=metric_def.biz_domain,
        cal_logic=metric_def.cal_logic,
        unit=metric_def.unit,
        status=bool(metric_def.status)
    )


def page_metric_definition(session: Session, current_page: int = 1, page_size: int = 10,
                        metric_name: Optional[str] = None,
                        metric_code: Optional[str] = None,
                        metric_type: Optional[str] = None,
                        biz_domain: Optional[str] = None):
    """
    分页查询指标定义

    Args:
        session: 数据库会话
        current_page: 当前页码
        page_size: 每页数量
        metric_name: 指标名称（支持模糊查询）
        metric_code: 指标编码（支持模糊查询）
        metric_type: 指标类型
        biz_domain: 业务域

    Returns:
        分页结果
    """
    # 构建查询条件
    conditions = []
    if metric_name and metric_name.strip():
        conditions.append(MetricDefinition.metric_name.ilike(f"%{metric_name.strip()}%"))
    if metric_code and metric_code.strip():
        conditions.append(MetricDefinition.metric_code.ilike(f"%{metric_code.strip()}%"))
    if metric_type and metric_type.strip():
        conditions.append(MetricDefinition.metric_type == metric_type.strip())
    if biz_domain and biz_domain.strip():
        conditions.append(MetricDefinition.biz_domain == biz_domain.strip())

    # 查询总数
    if conditions:
        count_stmt = select(func.count()).select_from(MetricDefinition).where(and_(*conditions))
    else:
        count_stmt = select(func.count()).select_from(MetricDefinition)

    total_count = session.execute(count_stmt).scalar()

    # 分页处理
    page_size = max(10, page_size)
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
    current_page = max(1, min(current_page, total_pages)) if total_pages > 0 else 1

    # 查询数据
    stmt = select(MetricDefinition)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.order_by(MetricDefinition.create_time.desc())
    stmt = stmt.offset((current_page - 1) * page_size).limit(page_size)

    results = session.execute(stmt).scalars().all()

    _list = []
    for metric_def in results:
        # owner 字段已注释，不再返回
        _list.append(MetricDefinitionInfo(
            metric_id=metric_def.metric_id,
            metric_name=metric_def.metric_name,
            metric_code=metric_def.metric_code,
            metric_type=metric_def.metric_type,
            biz_domain=metric_def.biz_domain,
            cal_logic=metric_def.cal_logic,
            unit=metric_def.unit,
            status=bool(metric_def.status)
        ))

    return current_page, page_size, total_count, total_pages, _list


def get_all_metric_definition(session: Session,
                           metric_name: Optional[str] = None,
                           metric_code: Optional[str] = None,
                           metric_type: Optional[str] = None,
                           biz_domain: Optional[str] = None):
    """
    获取所有指标定义（不分页）

    Args:
        session: 数据库会话
        metric_name: 指标名称（支持模糊查询）
        metric_code: 指标编码（支持模糊查询）
        metric_type: 指标类型
        biz_domain: 业务域

    Returns:
        指标定义列表
    """
    conditions = []
    if metric_name and metric_name.strip():
        conditions.append(MetricDefinition.metric_name.ilike(f"%{metric_name.strip()}%"))
    if metric_code and metric_code.strip():
        conditions.append(MetricDefinition.metric_code.ilike(f"%{metric_code.strip()}%"))
    if metric_type and metric_type.strip():
        conditions.append(MetricDefinition.metric_type == metric_type.strip())
    if biz_domain and biz_domain.strip():
        conditions.append(MetricDefinition.biz_domain == biz_domain.strip())

    stmt = select(MetricDefinition)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.order_by(MetricDefinition.create_time.desc())

    results = session.execute(stmt).scalars().all()

    _list = []
    for metric_def in results:
        # owner 字段已注释，不再返回
        _list.append(MetricDefinitionInfo(
            metric_id=metric_def.metric_id,
            metric_name=metric_def.metric_name,
            metric_code=metric_def.metric_code,
            metric_type=metric_def.metric_type,
            biz_domain=metric_def.biz_domain,
            cal_logic=metric_def.cal_logic,
            unit=metric_def.unit,
            status=bool(metric_def.status)
        ))

    return _list