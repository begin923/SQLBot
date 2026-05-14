import datetime
import time
from pathlib import Path
from typing import List, Optional

from apps.ai_model.embedding import EmbeddingModelCache
from sqlalchemy import and_, select, func, delete, update, text

from apps.extend.metrics.models.metric_source_mapping_model import MetricSourceMapping, MetricSourceMappingInfo
from apps.extend.utils.utils import DBUtils
from common.core.config import settings
from common.core.deps import SessionDep


class MetricSourceMappingCRUD:
    """指标源映射 CRUD 操作类"""

    def __init__(self, session: SessionDep):
        self.session = session
        # 在初始化时加载 embedding 模型，避免每次查询时重复加载
        self.embedding_model = None
        if settings.EMBEDDING_ENABLED:
            try:
                from apps.ai_model.embedding import EmbeddingModelCache
                self.embedding_model = EmbeddingModelCache.get_model()
                print("✅ Embedding 模型加载成功")
            except Exception as e:
                print(f"⚠️  Embedding 模型加载失败: {e}")
                self.embedding_model = None


    def create(self, session: SessionDep, info: MetricSourceMappingInfo, skip_embedding: bool = False):
        """
        创建单个指标源映射记录
        
        Args:
            session: 数据库会话
            info: 指标源映射信息对象
            skip_embedding: 是否跳过 embedding 处理（用于批量插入）
        
        Returns:
            创建的记录 ID
        """
        # ========== 步骤 1：基本验证 ==========
        if not info.metric_id or not info.metric_id.strip():
            raise Exception("指标ID不能为空")
        
        # if not info.source_type or not info.source_type.strip():
        #     raise Exception("数据源类型不能为空")
        
        # if not info.datasource or not info.datasource.strip():
        #     raise Exception("数据源标识不能为空")
        
        if not info.db_table or not info.db_table.strip():
            raise Exception("物理库表名不能为空")
        
        # if info.priority is None:
        #     raise Exception("优先级不能为空")
        
        if not info.source_level or not info.source_level.strip():
            raise Exception("源等级不能为空")
        
        create_time = datetime.datetime.now()
        
        # ========== 步骤 2：检查是否已存在（基于唯一约束） ==========
        exists_query = session.query(MetricSourceMapping).filter(
            and_(
                MetricSourceMapping.metric_id == info.metric_id.strip(),
                MetricSourceMapping.db_table == info.db_table.strip(),
                MetricSourceMapping.metric_column == (info.metric_column.strip() if info.metric_column else None)
            )
        ).first()
        
        if exists_query:
            raise Exception(f"指标 {info.metric_id} 在表 {info.db_table} 字段 {info.metric_column} 的映射已存在")
        
        # ========== 步骤 3：生成 ID（格式：S+6位数字） ==========
        # 查询当前最大的 ID
        max_id_query = session.query(func.max(MetricSourceMapping.id)).scalar()
        if max_id_query:
            # 提取数字部分并加1
            try:
                num_part = int(max_id_query[1:])  # 去掉 'S' 前缀
                new_num = num_part + 1
            except:
                new_num = 1
        else:
            new_num = 1
        
        new_id = f"S{new_num:06d}"  # 格式化为 S000001
        
        # ========== 步骤 4：创建记录 ==========
        mapping = MetricSourceMapping(
            id=new_id,
            metric_id=info.metric_id.strip(),
            metric_name=info.metric_name.strip() if info.metric_name else None,
            source_type=info.source_type.strip(),
            datasource=info.datasource.strip(),
            db_table=info.db_table.strip(),
            metric_column=info.metric_column.strip() if info.metric_column else None,
            filter_condition=info.filter_condition.strip() if info.filter_condition else None,
            agg_func=info.agg_func.strip() if info.agg_func else None,
            priority=info.priority,
            is_valid=1 if info.is_valid else 0,
            source_level=info.source_level.strip(),
            biz_domain=info.biz_domain.strip() if info.biz_domain else None,
            cal_logic=info.cal_logic.strip() if info.cal_logic else None,
            unit=info.unit.strip() if info.unit else None,
            create_time=create_time
        )
        
        session.add(mapping)
        session.flush()
        session.refresh(mapping)
        
        session.commit()
        
        # ========== 步骤 5：处理 Embedding ==========
        if not skip_embedding and settings.EMBEDDING_ENABLED:
            try:
                self._save_mapping_embeddings([mapping.id], session)
            except Exception as e:
                print(f"Metric source mapping embedding processing failed: {str(e)}")
                # embedding 失败不影响主流程
        
        return mapping.id

    def batch_create(self, session: SessionDep, info_list: List[MetricSourceMappingInfo]):
        """
        批量创建指标源映射记录
        
        Args:
            session: 数据库会话
            info_list: 指标源映射信息列表
        
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
                info.metric_id.strip().lower() if info.metric_id else '',
                info.db_table.strip().lower() if info.db_table else '',
                info.metric_column.strip().lower() if info.metric_column else ''
            )
            
            if unique_key in unique_key_set:
                duplicate_count += 1
                continue
            
            unique_key_set.add(unique_key)
            deduplicated_list.append(info)
        
        # 批量插入
        for info in deduplicated_list:
            try:
                mapping_id = self.create(session, info, skip_embedding=True)
                inserted_ids.append(mapping_id)
                success_count += 1
            except Exception as e:
                failed_records.append({
                    'data': info,
                    'errors': [str(e)]
                })
        
        # 批量处理 embedding（只在最后执行一次）
        if success_count > 0 and inserted_ids and settings.EMBEDDING_ENABLED:
            try:
                self._save_mapping_embeddings(inserted_ids, session)
            except Exception as e:
                print(f"Batch metric source mapping embedding processing failed: {str(e)}")
        
        return {
            'success_count': success_count,
            'failed_records': failed_records,
            'duplicate_count': duplicate_count,
            'original_count': len(info_list),
            'deduplicated_count': len(deduplicated_list)
        }

    def update(self, session: SessionDep, info: MetricSourceMappingInfo):
        """
        更新指标源映射记录
        
        Args:
            session: 数据库会话
            info: 指标源映射信息对象
        
        Returns:
            更新的记录 ID
        """
        if not info.id:
            raise Exception("ID 不能为空")
        
        count = session.query(MetricSourceMapping).filter(
            MetricSourceMapping.id == info.id
        ).count()
        
        if count == 0:
            raise Exception("指标源映射不存在")
        
        stmt = update(MetricSourceMapping).where(
            MetricSourceMapping.id == info.id
        ).values(
            metric_id=info.metric_id.strip() if info.metric_id else None,
            metric_name=info.metric_name.strip() if info.metric_name else None,
            source_type=info.source_type.strip() if info.source_type else None,
            datasource=info.datasource.strip() if info.datasource else None,
            db_table=info.db_table.strip() if info.db_table else None,
            metric_column=info.metric_column.strip() if info.metric_column else None,
            filter_condition=info.filter_condition.strip() if info.filter_condition else None,
            agg_func=info.agg_func.strip() if info.agg_func else None,
            priority=info.priority,
            is_valid=1 if info.is_valid else 0,
            source_level=info.source_level.strip() if info.source_level else None,
            biz_domain=info.biz_domain.strip() if info.biz_domain else None,
            cal_logic=info.cal_logic.strip() if info.cal_logic else None,
            unit=info.unit.strip() if info.unit else None,
        )
        
        session.execute(stmt)
        session.commit()
        
        # 更新 embedding
        if settings.EMBEDDING_ENABLED:
            try:
                self._save_mapping_embeddings([info.id], session)
            except Exception as e:
                print(f"Update metric source mapping embedding processing failed: {str(e)}")
        
        return info.id

    def delete(self, session: SessionDep, ids: List[str]):
        """
        删除指标源映射记录
        
        Args:
            session: 数据库会话
            ids: 要删除的记录 ID 列表
        """
        stmt = delete(MetricSourceMapping).where(MetricSourceMapping.id.in_(ids))
        session.execute(stmt)
        session.commit()

    def get_by_id(self, session: SessionDep, id: str) -> Optional[MetricSourceMappingInfo]:
        """
        根据 ID 查询指标源映射
        
        Args:
            session: 数据库会话
            id: 记录 ID
        
        Returns:
            指标源映射信息对象
        """
        mapping = session.query(MetricSourceMapping).filter(MetricSourceMapping.id == id).first()
        
        if not mapping:
            return None
        
        return MetricSourceMappingInfo(
            id=mapping.id,
            metric_id=mapping.metric_id,
            metric_name=mapping.metric_name,
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
            unit=mapping.unit
        )

    def get_by_metric_id(self, session: SessionDep, metric_id: str, 
                       datasource: Optional[str] = None,
                       source_type: Optional[str] = None) -> List[MetricSourceMappingInfo]:
        """
        根据指标 ID 查询源映射列表
        
        Args:
            session: 数据库会话
            metric_id: 指标 ID
            datasource: 数据源标识（可选）
            source_type: 数据源类型（可选）
        
        Returns:
            指标源映射信息对象列表
        """
        if not metric_id or not metric_id.strip():
            return []
        
        conditions = [MetricSourceMapping.metric_id == metric_id.strip()]
        
        if datasource and datasource.strip():
            conditions.append(MetricSourceMapping.datasource == datasource.strip())
        
        if source_type and source_type.strip():
            conditions.append(MetricSourceMapping.source_type == source_type.strip())
        
        # 只查询启用的记录，按优先级排序
        conditions.append(MetricSourceMapping.is_valid == 1)
        
        results = session.query(MetricSourceMapping).filter(
            and_(*conditions)
        ).order_by(
            MetricSourceMapping.priority.asc()
        ).all()
        
        return self._convert_to_info_list(results)

    def page(self, session: SessionDep, current_page: int = 1, page_size: int = 10,
             metric_id: Optional[str] = None,
             metric_name: Optional[str] = None,
             datasource: Optional[str] = None,
             source_type: Optional[str] = None,
             source_level: Optional[str] = None,
             is_valid: Optional[bool] = None):
        """
        分页查询指标源映射
        
        Args:
            session: 数据库会话
            current_page: 当前页码
            page_size: 每页数量
            metric_id: 指标 ID
            metric_name: 指标名称（支持模糊查询）
            datasource: 数据源标识
            source_type: 数据源类型
            source_level: 源等级
            is_valid: 是否启用
        
        Returns:
            分页结果
        """
        # 构建查询条件
        conditions = []
        if metric_id and metric_id.strip():
            conditions.append(MetricSourceMapping.metric_id == metric_id.strip())
        if metric_name and metric_name.strip():
            conditions.append(MetricSourceMapping.metric_name.ilike(f"%{metric_name.strip()}%"))
        if datasource and datasource.strip():
            conditions.append(MetricSourceMapping.datasource == datasource.strip())
        if source_type and source_type.strip():
            conditions.append(MetricSourceMapping.source_type == source_type.strip())
        if source_level and source_level.strip():
            conditions.append(MetricSourceMapping.source_level == source_level.strip())
        if is_valid is not None:
            conditions.append(MetricSourceMapping.is_valid == (1 if is_valid else 0))
        
        # 查询总数
        if conditions:
            count_stmt = select(func.count()).select_from(MetricSourceMapping).where(and_(*conditions))
        else:
            count_stmt = select(func.count()).select_from(MetricSourceMapping)
        
        total_count = session.execute(count_stmt).scalar()
        
        # 分页处理
        page_size = max(10, page_size)
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        current_page = max(1, min(current_page, total_pages)) if total_pages > 0 else 1
        
        # 查询数据
        stmt = select(MetricSourceMapping)
        if conditions:
            stmt = stmt.where(and_(*conditions))
        
        stmt = stmt.order_by(MetricSourceMapping.create_time.desc())
        stmt = stmt.offset((current_page - 1) * page_size).limit(page_size)
        
        results = session.execute(stmt).scalars().all()
        
        _list = []
        for mapping in results:
            _list.append(MetricSourceMappingInfo(
                id=mapping.id,
                metric_id=mapping.metric_id,
                metric_name=mapping.metric_name,
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
                unit=mapping.unit
            ))
        
        return current_page, page_size, total_count, total_pages, _list

    def get_all(self, session: SessionDep, 
                metric_id: Optional[str] = None,
                datasource: Optional[str] = None,
                source_type: Optional[str] = None):
        """
        获取所有指标源映射（不分页）
        
        Args:
            session: 数据库会话
            metric_id: 指标 ID
            datasource: 数据源标识
            source_type: 数据源类型
        
        Returns:
            指标源映射列表
        """
        conditions = []
        if metric_id and metric_id.strip():
            conditions.append(MetricSourceMapping.metric_id == metric_id.strip())
        if datasource and datasource.strip():
            conditions.append(MetricSourceMapping.datasource == datasource.strip())
        if source_type and source_type.strip():
            conditions.append(MetricSourceMapping.source_type == source_type.strip())
        
        stmt = select(MetricSourceMapping)
        if conditions:
            stmt = stmt.where(and_(*conditions))
        
        stmt = stmt.order_by(MetricSourceMapping.priority.asc(), MetricSourceMapping.create_time.desc())
        
        results = session.execute(stmt).scalars().all()
        
        return self._convert_to_info_list(results)

    def _convert_to_info_list(self, mappings: List[MetricSourceMapping]) -> List[MetricSourceMappingInfo]:
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
                metric_name=mapping.metric_name,
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
                unit=mapping.unit
            ))
        
        return result_list

    def _save_mapping_embeddings(self, ids: List[str], session: SessionDep):
        """
        为指标源映射计算并保存 embedding 向量
        
        Args:
            ids: 指标源映射 ID 列表
            session: 数据库会话（由调用方传入）
        """
        if not settings.EMBEDDING_ENABLED:
            print("ℹ️  EMBEDDING_ENABLED 未启用，跳过向量化")
            return
        
        if not ids or len(ids) == 0:
            print("ℹ️  没有需要处理的数据，跳过向量化")
            return
        
        try:
            print(f"🔍 正在查询 {len(ids)} 条记录...")
            # 使用 ORM 查询需要处理的记录
            mappings = session.query(MetricSourceMapping).filter(
                MetricSourceMapping.id.in_(ids)
            ).all()
            
            print(f"✅ 查询到 {len(mappings)} 条记录")
            
            # 准备文本数据（使用指标名称 + 业务域 + 计算逻辑）
            texts = []
            for mapping in mappings:
                text_parts = []
                if mapping.metric_name:
                    text_parts.append(mapping.metric_name)
                if mapping.biz_domain:
                    text_parts.append(mapping.biz_domain)
                if mapping.cal_logic:
                    text_parts.append(mapping.cal_logic)
                if mapping.db_table:
                    text_parts.append(mapping.db_table)
                if mapping.metric_column:
                    text_parts.append(mapping.metric_column)
                
                text = ", ".join(text_parts) if text_parts else ""
                texts.append(text)
            
            print(f"📝 准备了 {len(texts)} 个文本用于向量化")
            print(f"   示例文本：{texts[0] if texts else '无'}")
            
            # 计算 embedding
            try:
                print("🚀 正在加载 Embedding 模型...")
                model = EmbeddingModelCache.get_model()
                print("✅ Embedding 模型加载成功")
                
                print("⏳ 开始计算 embedding 向量（这可能需要一些时间）...")
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
                    stmt = update(MetricSourceMapping).where(
                        MetricSourceMapping.id == mappings[index].id
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
            print("🔒 数据库会话已关闭")

    def fill_empty_embeddings(self, session: SessionDep):
        """
        填充所有缺失的 embedding 向量
        
        Args:
            session: 数据库会话（由调用方传入）
        """
        # 先初始化本地配置，确保 LOCAL_MODEL_PATH 等配置项正确
        self._init_local_config()
        
        print("🔍 开始检查 EMBEDDING_ENABLED 配置...")
        if not settings.EMBEDDING_ENABLED:
            print("ℹ️  EMBEDDING_ENABLED 未启用，跳过向量化")
            return
        
        print("✅ EMBEDDING_ENABLED 已启用")
        
        # 使用 ORM 方式查询
        print("🔍 正在查询需要向量化的记录...")
        try:
            # 使用 ORM 查询
            select_null_vector = "SELECT id FROM metric_source_mapping WHERE embedding_vector IS NULL"
            sql = text(select_null_vector)
            print(f"📝 执行 SQL: {select_null_vector}")
            result = session.execute(sql)
            results = [row[0] for row in result.fetchall()]
            print(f"✅ 查询执行成功，找到 {len(results)} 条记录")
            
            if not results or len(results) == 0:
                print("✅ 所有指标源映射已向量化，无需处理")
                return
            
            print(f"📊 发现 {len(results)} 条记录需要向量化")
            print(f"   ID 列表：{results[:10]}{'...' if len(results) > 10 else ''}")
            
            if results:
                print(f"⏳ 开始调用 _save_mapping_embeddings...")
                self._save_mapping_embeddings(list(results), session)
                print(f"✅ 向量化处理完成")
        
        except Exception as query_error:
            print(f"❌ 查询失败：{query_error}")
            import traceback
            traceback.print_exc()
            raise


    def _init_local_config(self):
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

    def _search_by_exact(self, search_texts: List[str], datasource: Optional[str] = None,
                        source_type: Optional[str] = None) -> List[MetricSourceMapping]:
        """
        精准匹配搜索
        
        Args:
            search_texts: 搜索文本列表
            datasource: 数据源标识（可选）
            source_type: 数据源类型（可选）
        
        Returns:
            匹配的 MetricSourceMapping 对象列表
        """
        results = []
        matched_ids = set()
        
        for name in search_texts:
            if not name or not name.strip():
                continue
            
            conditions = [MetricSourceMapping.metric_name == name.strip()]
            if datasource is not None:
                conditions.append(MetricSourceMapping.datasource == datasource.strip())
            if source_type is not None:
                conditions.append(MetricSourceMapping.source_type == source_type.strip())
            
            query_results = self.session.query(MetricSourceMapping).filter(and_(*conditions)).all()
            
            for mapping in query_results:
                if mapping.id not in matched_ids:
                    results.append(mapping)
                    matched_ids.add(mapping.id)
        
        return results

    def _search_by_fuzzy(self, search_texts: List[str], datasource: Optional[str] = None,
                        source_type: Optional[str] = None, exclude_ids: set = None) -> List[MetricSourceMapping]:
        """
        模糊匹配搜索
        
        Args:
            search_texts: 搜索文本列表
            datasource: 数据源标识（可选）
            source_type: 数据源类型（可选）
            exclude_ids: 需要排除的 ID 集合（已匹配的）
        
        Returns:
            匹配的 MetricSourceMapping 对象列表
        """
        if exclude_ids is None:
            exclude_ids = set()
        
        results = []
        
        for name in search_texts:
            if not name or not name.strip():
                continue
            
            name_trimmed = name.strip()
            conditions = [
                MetricSourceMapping.metric_name.ilike(f"%{name_trimmed}%")
            ]
            if datasource is not None:
                conditions.append(MetricSourceMapping.datasource == datasource.strip())
            if source_type is not None:
                conditions.append(MetricSourceMapping.source_type == source_type.strip())
            
            query_results = self.session.query(MetricSourceMapping).filter(and_(*conditions)).all()
            
            for mapping in query_results:
                if mapping.id not in exclude_ids:
                    results.append(mapping)
                    exclude_ids.add(mapping.id)
        
        return results

    def _search_by_vector(self, search_texts: List[str], embedding_model,
                         datasource: Optional[str] = None, source_type: Optional[str] = None,
                         exclude_ids: set = None) -> tuple[List[MetricSourceMapping], set]:
        """
        向量相似度搜索
        
        Args:
            search_texts: 搜索文本列表
            embedding_model: 向量模型
            datasource: 数据源标识（可选）
            source_type: 数据源类型（可选）
            exclude_ids: 需要排除的 ID 集合（已匹配的）
        
        Returns:
            (匹配的 MetricSourceMapping 对象列表, 匹配的关键词集合)
        """
        if exclude_ids is None:
            exclude_ids = set()
        
        results = []
        matched_keywords = set()
        
        try:
            # 为每个搜索词生成 embedding
            for search_name in search_texts:
                if not search_name or not search_name.strip():
                    continue
                
                embedding = embedding_model.embed_query(search_name.strip())
                
                # 使用余弦相似度查询相似的指标
                similarity_threshold = getattr(settings, 'EMBEDDING_METRIC_SIMILARITY', 0.75)
                top_count = getattr(settings, 'EMBEDDING_METRIC_TOP_COUNT', 5)

                # 构建查询
                cte_query = select(
                    MetricSourceMapping.id,
                    MetricSourceMapping.metric_name,
                    text(f"(1 - (embedding_vector <=> :embedding_array)) AS similarity")
                ).select_from(MetricSourceMapping)
                
                if datasource is not None:
                    cte_query = cte_query.where(MetricSourceMapping.datasource == datasource.strip())
                if source_type is not None:
                    cte_query = cte_query.where(MetricSourceMapping.source_type == source_type.strip())
                
                cte_query = cte_query.where(MetricSourceMapping.embedding_vector.isnot(None))
                cte_query = cte_query.where(text(f"(1 - (embedding_vector <=> :embedding_array)) > {similarity_threshold}"))
                cte_query = cte_query.order_by(text('similarity DESC'))
                cte_query = cte_query.limit(top_count)
                
                # 执行查询
                similarity_results = self.session.execute(cte_query.params(embedding_array=str(embedding))).all()

                print(f"Similarity results for '{search_name}': {similarity_results}")

                # 添加匹配的结果（去重）
                for row in similarity_results:
                    if row.id not in exclude_ids:
                        mapping = self.session.get(MetricSourceMapping, row.id)
                        if mapping:
                            results.append(mapping)
                            exclude_ids.add(mapping.id)
                            matched_keywords.add(search_name)

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Metric source mapping embedding similarity search failed: {str(e)}")
            # 向量搜索失败不影响主流程
        
        return results, matched_keywords

    def search_metrics(self, search_texts: List[str],
                      datasource: Optional[str] = None,
                      source_type: Optional[str] = None) -> List[MetricSourceMappingInfo]:
        """
        根据指标名称列表查询指标源映射（支持混合查询：精准匹配 + 模糊匹配 + 向量相似度）
        匹配优先级：精准匹配 > 模糊匹配 > 向量相似度
        一旦高优先级匹配命中，则不再继续低优先级匹配
        
        Args:
            search_texts: 搜索文本列表（指标名称）
            datasource: 数据源标识（可选，用于过滤）
            source_type: 数据源类型（可选，用于过滤）
        
        Returns:
            指标源映射信息对象列表
        """
        if not search_texts or len(search_texts) == 0:
            return []
        
        _list: List[MetricSourceMapping] = []
        matched_ids_set = set()  # 用于去重
        
        # ========== 步骤 1：精准匹配（最高优先级） ==========
        _list = self._search_by_exact(search_texts, datasource, source_type)
        matched_ids_set = {mapping.id for mapping in _list}
        
        # 如果精准匹配已有结果，直接返回，不再继续模糊匹配和向量匹配
        if _list:
            print(f"✅ 精准匹配命中 {len(_list)} 条记录，跳过后续匹配")
            return self._convert_to_info_list(_list)
        
        # ========== 步骤 2：模糊匹配（次优先级） ==========
        _list = self._search_by_fuzzy(search_texts, datasource, source_type, matched_ids_set)
        matched_ids_set = {mapping.id for mapping in _list}
        
        # 如果模糊匹配已有结果，直接返回，不再继续向量匹配
        if _list:
            print(f"✅ 模糊匹配命中 {len(_list)} 条记录，跳过向量匹配")
            return self._convert_to_info_list(_list)
        
        # ========== 步骤 3：向量相似度匹配（最低优先级） ==========
        if settings.EMBEDDING_ENABLED and search_texts and self.embedding_model:
            try:
                vector_results, _ = self._search_by_vector(
                    search_texts, self.embedding_model, datasource, source_type, matched_ids_set
                )
                _list.extend(vector_results)
            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"Metric source mapping embedding similarity search failed: {str(e)}")
                # 向量搜索失败不影响主流程
        
        # 如果没有匹配到任何结果，返回空列表
        if not _list:
            print("⚠️  未匹配到任何结果")
            return []
        
        # 转换为返回格式
        return self._convert_to_info_list(_list)

if __name__ == '__main__':
    # 获取向量模型
    session = DBUtils.create_local_session()
    metric_source_mapping = MetricSourceMappingCRUD(session)
    start_time = time.time()
    results = metric_source_mapping.search_metrics(
        ['饲喂头数']  # 多个搜索文本
    )
    end_time = time.time()
    print(f"耗时: {end_time - start_time:.2f}s")
    print(f"结果数量: {len(results)}")
    print(f"结果列表: {results}")