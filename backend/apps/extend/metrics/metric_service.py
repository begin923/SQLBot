from typing import List, Optional
from sqlalchemy import and_, or_, select, text
from apps.extend.metrics.models.metric_source_mapping_model import MetricSourceMapping, MetricSourceMappingInfo
from apps.ai_model.embedding import EmbeddingModelCache
from apps.extend.utils.utils import DBUtils
from common.core.config import settings
import logging

logger = logging.getLogger("MetricsService")


class MetricsService:
    def __init__(self, session):
        self.session = session

    def search_metrics(
        self,
        metric_names: List[str],
        datasource_id: Optional[int] = None
    ) -> List[MetricSourceMappingInfo]:
        """
        根据指标名称列表查询指标源映射数据（支持混合查询：精准匹配 + 模糊匹配 + 向量相似度）
        匹配优先级：精准匹配 > 模糊匹配 > 向量相似度
        一旦高优先级匹配命中，则不再继续低优先级匹配
        
        Args:
            metric_names: 指标名称列表
            datasource_id: 数据源 ID（可选，用于过滤）
        
        Returns:
            指标源映射信息对象列表
        """
        if not metric_names or len(metric_names) == 0:
            return []
        
        _list: List[MetricSourceMapping] = []
        matched_ids_set = set()  # 用于去重
        
        # 清理并过滤空名称
        cleaned_names = [name.strip() for name in metric_names if name and name.strip()]
        if not cleaned_names:
            return []
        
        # ========== 步骤 1：精准匹配（最高优先级） ==========
        # ⚠️ 优化：使用 IN 查询，一次性获取所有精准匹配结果
        results = self.session.query(MetricSourceMapping).filter(
            MetricSourceMapping.metric_name.in_(cleaned_names)
        ).all()
        
        for mapping in results:
            if mapping.id not in matched_ids_set:
                _list.append(mapping)
                matched_ids_set.add(mapping.id)
        
        # 如果精准匹配已有结果，直接返回，不再继续模糊匹配和向量匹配
        if _list:
            logger.info(f"✅ 精准匹配命中 {len(_list)} 条记录，跳过后续匹配")
            return self._convert_to_info_list(_list)
        
        # ========== 步骤 2：模糊匹配（metric_name，次优先级） ==========
        # ⚠️ 优化：使用 OR 条件，一次性查询所有模糊匹配
        conditions = [
            MetricSourceMapping.metric_name.ilike(f"%{name}%")
            for name in cleaned_names
        ]
        
        if conditions:
            results = self.session.query(MetricSourceMapping).filter(
                or_(*conditions)
            ).all()
            
            for mapping in results:
                if mapping.id not in matched_ids_set:
                    _list.append(mapping)
                    matched_ids_set.add(mapping.id)
        
        # 如果模糊匹配已有结果，直接返回，不再继续向量匹配
        if _list:
            logger.info(f"✅ 模糊匹配命中 {len(_list)} 条记录，跳过向量匹配")
            return self._convert_to_info_list(_list)
        
        # ========== 步骤 3：向量相似度匹配（最低优先级） ==========
        if settings.EMBEDDING_ENABLED and cleaned_names:
            try:
                model = EmbeddingModelCache.get_model()
                
                # ⚠️ 优化：批量计算 embedding
                embeddings = model.embed_documents(cleaned_names)
                
                # 为每个搜索词生成 embedding 并查询
                for search_name, embedding in zip(cleaned_names, embeddings):
                    # 使用余弦相似度查询相似的指标（在数据库层面完成过滤和排序）
                    similarity_threshold = getattr(settings, 'EMBEDDING_METRIC_SIMILARITY', 0.75)
                    top_count = getattr(settings, 'EMBEDDING_METRIC_TOP_COUNT', 5)
                    
                    # 构建 CTE 查询：计算相似度、过滤 NULL 值、过滤阈值、排序、LIMIT
                    cte_query = select(
                        MetricSourceMapping.id,
                        MetricSourceMapping.metric_name,
                        text(f"(1 - (embedding_vector <=> :embedding_array)) AS similarity")
                    ).select_from(MetricSourceMapping)
                    
                    cte_query = cte_query.where(MetricSourceMapping.embedding_vector.isnot(None))
                    cte_query = cte_query.where(
                        text(f"(1 - (embedding_vector <=> :embedding_array)) > {similarity_threshold}")
                    )
                    cte_query = cte_query.order_by(text('similarity DESC'))
                    cte_query = cte_query.limit(top_count)
                    
                    # 执行查询
                    similarity_results = self.session.execute(
                        cte_query.params(embedding_array=str(embedding))
                    ).all()
                    
                    logger.debug(f"Similarity results for '{search_name}': {len(similarity_results)} records")
                    
                    # ⚠️ 优化：批量查询，避免 N+1 问题
                    if similarity_results:
                        ids_to_fetch = [
                            row.id for row in similarity_results 
                            if row.id not in matched_ids_set
                        ]
                        
                        if ids_to_fetch:
                            # 一次性查询所有需要的记录
                            mappings = self.session.query(MetricSourceMapping).filter(
                                MetricSourceMapping.id.in_(ids_to_fetch)
                            ).all()
                            
                            for mapping in mappings:
                                _list.append(mapping)
                                matched_ids_set.add(mapping.id)
            
            except Exception as e:
                import traceback
                traceback.print_exc()
                logger.error(f"Metric source mapping embedding similarity search failed: {str(e)}")
                # 向量搜索失败不影响主流程，继续使用模糊匹配的结果
        
        # 如果没有匹配到任何结果，返回空列表
        if not _list:
            logger.warning("⚠️  未匹配到任何结果")
            return []
        
        # 转换为返回格式
        return self._convert_to_info_list(_list)
    
    def _convert_to_info_list(
        self,
        mappings: List[MetricSourceMapping]
    ) -> List[MetricSourceMappingInfo]:
        """
        将 MetricSourceMapping 对象列表转换为 MetricSourceMappingInfo 对象列表
        
        Args:
            mappings: MetricSourceMapping 对象列表
        
        Returns:
            MetricSourceMappingInfo 对象列表
        """
        result_list = []
        for mapping in mappings:
            result_list.append(MetricSourceMappingInfo(
                id=mapping.id,
                metric_id=mapping.metric_id,
                metric_name=mapping.metric_name,  # ⚠️ 新增：指标中文名称
                source_type=mapping.source_type,
                datasource=mapping.datasource,
                db_table=mapping.db_table,
                metric_column=mapping.metric_column,
                filter_condition=mapping.filter_condition,
                agg_func=mapping.agg_func,
                priority=mapping.priority,
                is_valid=bool(mapping.is_valid),
                source_level=mapping.source_level,
                biz_domain=mapping.biz_domain,
                cal_logic=mapping.cal_logic,
                unit=mapping.unit,
                create_time=mapping.create_time,
                modify_time=mapping.modify_time
            ))
        
        return result_list


if __name__ == '__main__':
    session = DBUtils.create_local_session()
    metric_service = MetricsService(session)
    res= metric_service.search_metrics(["采集数"])
    for item in res:
        print(item)
