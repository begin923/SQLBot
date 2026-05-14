import datetime
import time
import uuid
from pathlib import Path
from typing import List, Optional

from apps.ai_model.embedding import EmbeddingModelCache
from sqlalchemy import and_, or_, select, func, delete, update, text

from apps.extend.metrics.models.column_metadata_model import ColumnMetadata, ColumnMetadataInfo
from common.core.config import settings
from common.core.deps import SessionDep


class ColumnMetadataService:
    """列元数据服务类"""
    
    def __init__(self, session: SessionDep):
        """
        初始化服务
        
        Args:
            session: 数据库会话
        """
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
    
    def create(self, info: ColumnMetadataInfo, skip_embedding: bool = False) -> str:
        """
        创建单个列元数据记录
        
        Args:
            info: 列元数据信息对象
            skip_embedding: 是否跳过 embedding 处理（用于批量插入）
        
        Returns:
            创建的记录 column_id
        """
        # ========== 步骤 1：基本验证 ==========
        if not info.column_name or not info.column_name.strip():
            raise Exception("列名不能为空")
        
        if not info.table_id or not info.table_id.strip():
            raise Exception("表ID不能为空")
        
        # 生成 column_id (如果未提供)
        column_id = info.column_id if info.column_id else str(uuid.uuid4()).replace('-', '')[:32]
        
        create_time = datetime.datetime.now()
        
        # ========== 步骤 2：检查是否已存在 ==========
        exists_query = self.session.query(ColumnMetadata).filter(
            and_(
                ColumnMetadata.table_id == info.table_id.strip(),
                ColumnMetadata.column_name == info.column_name.strip()
            )
        ).first()
        
        if exists_query:
            raise Exception(f"列 {info.column_name} 在表 {info.table_id} 中已存在")
        
        # ========== 步骤 3：创建记录 ==========
        column = ColumnMetadata(
            column_id=column_id,
            table_id=info.table_id.strip(),
            table_name=info.table_name.strip() if info.table_name else None,
            table_comment=info.table_comment.strip() if info.table_comment else None,
            column_name=info.column_name.strip(),
            column_comment=info.column_comment.strip() if info.column_comment else None,
            column_type=info.column_type.strip() if info.column_type else None,
            column_length=info.column_length,
            column_precision=info.column_precision,
            is_nullable=info.is_nullable if info.is_nullable is not None else 1,
            is_primary_key=info.is_primary_key if info.is_primary_key is not None else 0,
            column_biz_type=info.column_biz_type.strip() if info.column_biz_type else None,
            dim_id=info.dim_id.strip() if info.dim_id else None,
            metric_id=info.metric_id.strip() if info.metric_id else None,
            create_time=create_time,
            modify_time=create_time
        )
        
        self.session.add(column)
        self.session.flush()
        self.session.refresh(column)  # 获取生成的 ID
        
        self.session.commit()
        
        # ========== 步骤 4：处理 Embedding ==========
        if not skip_embedding and settings.EMBEDDING_ENABLED:
            try:
                self._save_column_embeddings([column.column_id])
            except Exception as e:
                print(f"Column embedding processing failed: {str(e)}")
                # embedding 失败不影响主流程
        
        return column.column_id
    
    def batch_create(self, info_list: List[ColumnMetadataInfo]) -> dict:
        """
        批量创建列元数据记录
        
        Args:
            info_list: 列元数据信息列表
        
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
                info.table_id.strip().lower() if info.table_id else '',
                info.column_name.strip().lower() if info.column_name else ''
            )
            
            if unique_key in unique_key_set:
                duplicate_count += 1
                continue
            
            unique_key_set.add(unique_key)
            deduplicated_list.append(info)
        
        # 批量插入
        for info in deduplicated_list:
            try:
                column_id = self.create(info, skip_embedding=True)
                inserted_ids.append(column_id)
                success_count += 1
            except Exception as e:
                failed_records.append({
                    'data': info,
                    'errors': [str(e)]
                })
        
        # 批量处理 embedding（只在最后执行一次）
        if success_count > 0 and inserted_ids and settings.EMBEDDING_ENABLED:
            try:
                self._save_column_embeddings(inserted_ids)
            except Exception as e:
                print(f"Batch column embedding processing failed: {str(e)}")
        
        return {
            'success_count': success_count,
            'failed_records': failed_records,
            'duplicate_count': duplicate_count,
            'original_count': len(info_list),
            'deduplicated_count': len(deduplicated_list)
        }
    
    def update(self, info: ColumnMetadataInfo) -> str:
        """
        更新列元数据记录
        
        Args:
            info: 列元数据信息对象
        
        Returns:
            更新的记录 column_id
        """
        if not info.column_id:
            raise Exception("Column ID 不能为空")
        
        count = self.session.query(ColumnMetadata).filter(
            ColumnMetadata.column_id == info.column_id
        ).count()
        
        if count == 0:
            raise Exception("列元数据不存在")
        
        stmt = update(ColumnMetadata).where(
            ColumnMetadata.column_id == info.column_id
        ).values(
            table_id=info.table_id.strip() if info.table_id else None,
            table_name=info.table_name.strip() if info.table_name else None,
            table_comment=info.table_comment.strip() if info.table_comment else None,
            column_name=info.column_name.strip() if info.column_name else None,
            column_comment=info.column_comment.strip() if info.column_comment else None,
            column_type=info.column_type.strip() if info.column_type else None,
            column_length=info.column_length,
            column_precision=info.column_precision,
            is_nullable=info.is_nullable if info.is_nullable is not None else 1,
            is_primary_key=info.is_primary_key if info.is_primary_key is not None else 0,
            column_biz_type=info.column_biz_type.strip() if info.column_biz_type else None,
            dim_id=info.dim_id.strip() if info.dim_id else None,
            metric_id=info.metric_id.strip() if info.metric_id else None,
            modify_time=datetime.datetime.now()
        )
        
        self.session.execute(stmt)
        self.session.commit()
        
        # 更新 embedding
        if settings.EMBEDDING_ENABLED:
            try:
                self._save_column_embeddings([info.column_id])
            except Exception as e:
                print(f"Update column embedding processing failed: {str(e)}")
        
        return info.column_id
    
    def delete(self, column_ids: List[str]):
        """
        删除列元数据记录
        
        Args:
            column_ids: 要删除的记录 column_id 列表
        """
        stmt = delete(ColumnMetadata).where(ColumnMetadata.column_id.in_(column_ids))
        self.session.execute(stmt)
        self.session.commit()
    
    def get_by_id(self, column_id: str) -> Optional[ColumnMetadataInfo]:
        """
        根据 column_id 查询列元数据
        
        Args:
            column_id: 记录 column_id
        
        Returns:
            列元数据信息对象
        """
        column = self.session.query(ColumnMetadata).filter(ColumnMetadata.column_id == column_id).first()
        
        if not column:
            return None
        
        return ColumnMetadataInfo(
            column_id=column.column_id,
            table_id=column.table_id,
            table_name=column.table_name,
            table_comment=column.table_comment,
            column_name=column.column_name,
            column_comment=column.column_comment,
            column_type=column.column_type,
            column_length=column.column_length,
            column_precision=column.column_precision,
            is_nullable=column.is_nullable,
            is_primary_key=column.is_primary_key,
            column_biz_type=column.column_biz_type,
            dim_id=column.dim_id,
            metric_id=column.metric_id,
            enabled=True
        )
    
    def get_by_table(self, table_id: str) -> List[ColumnMetadataInfo]:
        """
        根据表ID查询所有列元数据
        
        Args:
            table_id: 表ID
        
        Returns:
            列元数据信息对象列表
        """
        columns = self.session.query(ColumnMetadata).filter(
            ColumnMetadata.table_id == table_id
        ).order_by(ColumnMetadata.column_name).all()
        
        result_list = []
        for column in columns:
            result_list.append(ColumnMetadataInfo(
                column_id=column.column_id,
                table_id=column.table_id,
                table_name=column.table_name,
                table_comment=column.table_comment,
                column_name=column.column_name,
                column_comment=column.column_comment,
                column_type=column.column_type,
                column_length=column.column_length,
                column_precision=column.column_precision,
                is_nullable=column.is_nullable,
                is_primary_key=column.is_primary_key,
                column_biz_type=column.column_biz_type,
                dim_id=column.dim_id,
                metric_id=column.metric_id,
                enabled=True
            ))
        
        return result_list
    
    def page(self, current_page: int = 1, page_size: int = 10,
             column_name: Optional[str] = None, 
             table_id: Optional[str] = None,
             column_biz_type: Optional[str] = None):
        """
        分页查询列元数据
        
        Args:
            current_page: 当前页码
            page_size: 每页数量
            column_name: 列名（支持模糊查询）
            table_id: 表ID
            column_biz_type: 列业务类型
        
        Returns:
            分页结果
        """
        # 构建查询条件
        conditions = []
        if column_name and column_name.strip():
            conditions.append(ColumnMetadata.column_name.ilike(f"%{column_name.strip()}%"))
        if table_id and table_id.strip():
            conditions.append(ColumnMetadata.table_id == table_id.strip())
        if column_biz_type and column_biz_type.strip():
            conditions.append(ColumnMetadata.column_biz_type == column_biz_type.strip())
        
        # 查询总数
        if conditions:
            count_stmt = select(func.count()).select_from(ColumnMetadata).where(and_(*conditions))
        else:
            count_stmt = select(func.count()).select_from(ColumnMetadata)
        
        total_count = self.session.execute(count_stmt).scalar()
        
        # 分页处理
        page_size = max(10, page_size)
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        current_page = max(1, min(current_page, total_pages)) if total_pages > 0 else 1
        
        # 查询数据
        stmt = select(ColumnMetadata)
        if conditions:
            stmt = stmt.where(and_(*conditions))
        
        stmt = stmt.order_by(ColumnMetadata.create_time.desc())
        stmt = stmt.offset((current_page - 1) * page_size).limit(page_size)
        
        results = self.session.execute(stmt).scalars().all()
        
        _list = []
        for column in results:
            _list.append(ColumnMetadataInfo(
                column_id=column.column_id,
                table_id=column.table_id,
                table_name=column.table_name,
                table_comment=column.table_comment,
                column_name=column.column_name,
                column_comment=column.column_comment,
                column_type=column.column_type,
                column_length=column.column_length,
                column_precision=column.column_precision,
                is_nullable=column.is_nullable,
                is_primary_key=column.is_primary_key,
                column_biz_type=column.column_biz_type,
                dim_id=column.dim_id,
                metric_id=column.metric_id,
                enabled=True
            ))
        
        return current_page, page_size, total_count, total_pages, _list
    
    def get_all(self, column_name: Optional[str] = None,
                table_id: Optional[str] = None,
                column_biz_type: Optional[str] = None):
        """
        获取所有列元数据（不分页）
        
        Args:
            column_name: 列名（支持模糊查询）
            table_id: 表ID
            column_biz_type: 列业务类型
        
        Returns:
            列元数据列表
        """
        conditions = []
        if column_name and column_name.strip():
            conditions.append(ColumnMetadata.column_name.ilike(f"%{column_name.strip()}%"))
        if table_id and table_id.strip():
            conditions.append(ColumnMetadata.table_id == table_id.strip())
        if column_biz_type and column_biz_type.strip():
            conditions.append(ColumnMetadata.column_biz_type == column_biz_type.strip())
        
        stmt = select(ColumnMetadata)
        if conditions:
            stmt = stmt.where(and_(*conditions))
        
        stmt = stmt.order_by(ColumnMetadata.create_time.desc())
        
        results = self.session.execute(stmt).scalars().all()
        
        _list = []
        for column in results:
            _list.append(ColumnMetadataInfo(
                column_id=column.column_id,
                table_id=column.table_id,
                table_name=column.table_name,
                table_comment=column.table_comment,
                column_name=column.column_name,
                column_comment=column.column_comment,
                column_type=column.column_type,
                column_length=column.column_length,
                column_precision=column.column_precision,
                is_nullable=column.is_nullable,
                is_primary_key=column.is_primary_key,
                column_biz_type=column.column_biz_type,
                dim_id=column.dim_id,
                metric_id=column.metric_id,
                enabled=True
            ))
        
        return _list
    
    def _save_column_embeddings(self, column_ids: List[str]):
        """
        为列元数据计算并保存 embedding 向量
        参考 terminology 表的 save_embeddings 函数实现
        
        Args:
            column_ids: 列ID列表
        """
        if not settings.EMBEDDING_ENABLED:
            print("ℹ️  EMBEDDING_ENABLED 未启用，跳过向量化")
            return
        
        if not column_ids or len(column_ids) == 0:
            print("ℹ️  没有需要处理的数据，跳过向量化")
            return
        
        try:
            print(f"🔍 正在查询 {len(column_ids)} 条记录...")
            # 使用 ORM 查询需要处理的记录
            columns = self.session.query(ColumnMetadata).filter(
                ColumnMetadata.column_id.in_(column_ids)
            ).all()
            
            print(f"✅ 查询到 {len(columns)} 条记录")
            
            # 准备文本数据（使用列名 + 列注释）- 参考 terminology 的逻辑
            texts = []
            for column in columns:
                text = column.column_name or ""
                if column.column_comment:
                    text += f", {column.column_comment}"
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
                    stmt = update(ColumnMetadata).where(
                        ColumnMetadata.column_id == columns[index].column_id
                    ).values(embedding_vector=results[index])
                    self.session.execute(stmt)
                    if (index + 1) % 10 == 0 or index == len(results) - 1:
                        print(f"   已更新 {index + 1}/{len(results)} 条记录")
                
                self.session.commit()
                print("✅ 数据库更新完成！")
                
            except FileNotFoundError as e:
                # 模型文件不存在
                print(f"⚠️  Embedding 模型文件不存在：{e}")
                print(f"💡 提示：请检查 LOCAL_MODEL_PATH 配置")
                print(f"   当前路径：{settings.LOCAL_MODEL_PATH}")
                print(f"   如需配置，请在.env 文件中设置 LOCAL_MODEL_PATH")
                self.session.rollback()
                raise
            except ImportError as e:
                print(f"❌ Embedding 模块导入失败：{e}")
                print("💡 提示：请检查 EMBEDDING_ENABLED 配置和 EmbeddingModelCache 是否可用")
                self.session.rollback()
                raise
            except Exception as e:
                print(f"❌ Embedding 计算过程中出错：{e}")
                import traceback
                traceback.print_exc()
                self.session.rollback()
                raise
        
        except Exception as e:
            print(f"❌ 向量化处理失败：{e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            print("🔒 数据库会话已关闭")
    
    @staticmethod
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
    
    def fill_empty_embeddings(self):
        """
        填充所有缺失的 embedding 向量
        参考 terminology 表的 run_fill_empty_embeddings 函数实现
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
            select_null_vector = "SELECT column_id FROM column_metadata WHERE embedding_vector IS NULL"
            sql = text(select_null_vector)
            print(f"📝 执行 SQL: {select_null_vector}")
            result = self.session.execute(sql)
            results = [row[0] for row in result.fetchall()]
            print(f"✅ 查询执行成功，找到 {len(results)} 条记录")
            
            if not results or len(results) == 0:
                print("✅ 所有列已向量化，无需处理")
                return
            
            print(f"📊 发现 {len(results)} 条记录需要向量化")
            print(f"   ID 列表：{results[:10]}{'...' if len(results) > 10 else ''}")
            
            if results:
                print(f"⏳ 开始调用 _save_column_embeddings...")
                self._save_column_embeddings(list(results))
                print(f"✅ 向量化处理完成")
        
        except Exception as query_error:
            print(f"❌ 查询失败：{query_error}")
            import traceback
            traceback.print_exc()
            raise
    
    def _search_by_exact_concurrent(self, search_list: List[dict]) -> dict:
        """
        精准匹配搜索（并发版本）- 返回字典便于快速查找
        
        Args:
            search_list: 搜索列表，每个元素包含 {table_name, column_comments}
        
        Returns:
            字典，key为 (table_name, column_comment)，value为 ColumnMetadata 对象
        """
        results = {}
        
        for item in search_list:
            table_name = item.get('table_name', '').strip()
            column_comments = item.get('column_comments', [])
            
            if not table_name or not column_comments:
                continue
            
            # 遍历该表的所有字段注释进行精准匹配
            for column_comment in column_comments:
                column_comment = column_comment.strip()
                if not column_comment:
                    continue
                
                key = (table_name, column_comment)
                    
                conditions = [
                    ColumnMetadata.table_name == table_name,
                    ColumnMetadata.column_comment == column_comment
                ]
                
                query_result = self.session.query(ColumnMetadata).filter(and_(*conditions)).first()
                
                if query_result:
                    results[key] = query_result
        
        return results
    
    def _search_by_fuzzy_concurrent(self, search_list: List[dict]) -> dict:
        """
        模糊匹配搜索（并发版本）- 返回字典便于快速查找
        
        Args:
            search_list: 搜索列表，每个元素包含 {table_name, column_comments}
        
        Returns:
            字典，key为 (table_name, column_comment)，value为 ColumnMetadata 对象
        """
        results = {}
        
        for item in search_list:
            table_name = item.get('table_name', '').strip()
            column_comments = item.get('column_comments', [])
            
            if not table_name or not column_comments:
                continue
            
            # 遍历该表的所有字段注释进行模糊匹配
            for column_comment in column_comments:
                column_comment = column_comment.strip()
                if not column_comment:
                    continue
                
                key = (table_name, column_comment)
                
                # 构建模糊查询条件：表名精确匹配 + 字段注释模糊匹配
                conditions = [
                    ColumnMetadata.table_name == table_name,
                    ColumnMetadata.column_comment.ilike(f"%{column_comment}%")
                ]
                
                query_result = self.session.query(ColumnMetadata).filter(and_(*conditions)).first()
                
                if query_result:
                    results[key] = query_result
        
        return results
    
    def _search_by_vector_concurrent(self, search_list: List[dict]) -> dict:
        """
        向量相似度搜索（并发版本）- 返回字典便于快速查找
        
        Args:
            search_list: 搜索列表，每个元素包含 {table_name, column_comments}
        
        Returns:
            字典，key为 (table_name, column_comment)，value为 ColumnMetadata 对象
        """
        results = {}
        
        if not self.embedding_model:
            print("⚠️  Embedding 模型未加载，跳过向量搜索")
            return results
        
        try:
            # 为每个搜索词生成 embedding
            for item in search_list:
                table_name = item.get('table_name', '').strip()
                column_comments = item.get('column_comments', [])
                
                if not table_name or not column_comments:
                    continue
                
                # 遍历该表的所有字段注释进行向量搜索
                for column_comment in column_comments:
                    column_comment = column_comment.strip()
                    if not column_comment:
                        continue
                    
                    key = (table_name, column_comment)
                    
                    # 使用字段注释生成 embedding
                    embedding = self.embedding_model.embed_query(column_comment)
                    
                    # 使用余弦相似度查询相似的列
                    similarity_threshold = getattr(settings, 'EMBEDDING_COLUMN_SIMILARITY', 0.75)
                    top_count = getattr(settings, 'EMBEDDING_COLUMN_TOP_COUNT', 1)  # 只取最相似的一个

                    # 构建查询
                    cte_query = select(
                        ColumnMetadata.column_id,
                        ColumnMetadata.column_name,
                        ColumnMetadata.column_comment,
                        text(f"(1 - (embedding_vector <=> :embedding_array)) AS similarity")
                    ).select_from(ColumnMetadata)
                    
                    # 表名精确匹配
                    cte_query = cte_query.where(ColumnMetadata.table_name == table_name)
                    cte_query = cte_query.where(ColumnMetadata.embedding_vector.isnot(None))
                    cte_query = cte_query.where(text(f"(1 - (embedding_vector <=> :embedding_array)) > {similarity_threshold}"))
                    cte_query = cte_query.order_by(text('similarity DESC'))
                    cte_query = cte_query.limit(top_count)
                    
                    # 执行查询
                    similarity_results = self.session.execute(cte_query.params(embedding_array=str(embedding))).all()

                    # 添加匹配的结果（只取最相似的一个）
                    if similarity_results:
                        row = similarity_results[0]
                        column = self.session.get(ColumnMetadata, row.column_id)
                        if column:
                            results[key] = column
                            print(f"✅ 向量匹配: table='{table_name}', comment='{column_comment}' → found '{column.column_name}'")

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Column metadata embedding similarity search failed: {str(e)}")
            # 向量搜索失败不影响主流程
        
        return results
    
    def search_columns(self, search_list: List[dict]) -> dict:
        """
        根据表名和字段注释列表查询列元数据（支持混合查询：精准匹配 + 模糊匹配 + 向量相似度）
        三种匹配方式并发执行，然后按优先级合并结果
        匹配优先级：精准匹配 > 模糊匹配 > 向量相似度
        
        Args:
            search_list: 搜索列表，每个元素包含 {table_name: 表名, column_comments: 字段注释列表}
                       例如：[
                           {"table_name": "users", "column_comments": ["用户名", "邮箱"]},
                           {"table_name": "orders", "column_comments": ["订单金额", "创建时间"]}
                       ]
        
        Returns:
            字典，包含：
            - found: 是否所有字段都查到数据 (bool)
            - columns: 列元数据信息对象列表
            - total_searched: 搜索的总数量（表*字段注释的组合数）
            - total_found: 找到的总数量
            - details: 详细匹配状态列表，每个元素包含：
                - table_name: 表名
                - column_comment: 字段注释
                - found: 是否找到 (bool)
                - match_type: 匹配类型 (exact/fuzzy/vector)
                - column_info: 如果找到，包含列的详细信息
        """
        if not search_list or len(search_list) == 0:
            return {
                'found': False,
                'columns': [],
                'total_searched': 0,
                'total_found': 0,
                'details': []
            }
        
        # 构建详细的搜索计划
        search_details = []
        search_keys = set()  # 用于快速查找
        
        for item in search_list:
            table_name = item.get('table_name', '').strip()
            column_comments = item.get('column_comments', [])
            
            if not table_name or not column_comments:
                continue
            
            for column_comment in column_comments:
                column_comment = column_comment.strip()
                if not column_comment:
                    continue
                
                key = (table_name, column_comment)
                search_keys.add(key)
                
                search_details.append({
                    'table_name': table_name,
                    'column_comment': column_comment,
                    'found': False,
                    'match_type': None,  # exact, fuzzy, vector
                    'column_info': None
                })
        
        # 计算总搜索数量
        total_searched = len(search_details)
        
        print(f"\n🔍 开始并发搜索 {total_searched} 个字段...")
        
        # ========== 并发执行三种匹配方式 ==========
        print("  ├─ 执行精准匹配...")
        exact_results = self._search_by_exact_concurrent(search_list)
        print(f"  │  ✅ 精准匹配找到 {len(exact_results)} 条")
        
        print("  ├─ 执行模糊匹配...")
        fuzzy_results = self._search_by_fuzzy_concurrent(search_list)
        print(f"  │  ✅ 模糊匹配找到 {len(fuzzy_results)} 条")
        
        print("  └─ 执行向量匹配...")
        vector_results = self._search_by_vector_concurrent(search_list)
        print(f"     ✅ 向量匹配找到 {len(vector_results)} 条")
        
        # ========== 按优先级合并结果 ==========
        # 优先级：精准匹配 > 模糊匹配 > 向量匹配
        merged_results = {}
        
        # 1. 先加入精准匹配结果（最高优先级）
        for key, column in exact_results.items():
            merged_results[key] = {
                'column': column,
                'match_type': 'exact'
            }
        
        # 2. 加入模糊匹配结果（仅当精准匹配没有时）
        for key, column in fuzzy_results.items():
            if key not in merged_results:
                merged_results[key] = {
                    'column': column,
                    'match_type': 'fuzzy'
                }
        
        # 3. 加入向量匹配结果（仅当前两者都没有时）
        for key, column in vector_results.items():
            if key not in merged_results:
                merged_results[key] = {
                    'column': column,
                    'match_type': 'vector'
                }
        
        # ========== 更新 search_details ==========
        for detail in search_details:
            key = (detail['table_name'], detail['column_comment'])
            if key in merged_results:
                detail['found'] = True
                detail['match_type'] = merged_results[key]['match_type']
                column = merged_results[key]['column']
                detail['column_info'] = ColumnMetadataInfo(
                    column_id=column.column_id,
                    table_id=column.table_id,
                    table_name=column.table_name,
                    table_comment=column.table_comment,
                    column_name=column.column_name,
                    column_comment=column.column_comment,
                    column_type=column.column_type,
                    column_length=column.column_length,
                    column_precision=column.column_precision,
                    is_nullable=column.is_nullable,
                    is_primary_key=column.is_primary_key,
                    column_biz_type=column.column_biz_type,
                    dim_id=column.dim_id,
                    metric_id=column.metric_id,
                    enabled=True
                )
        
        # 统计最终结果
        total_found = sum(1 for d in search_details if d['found'])
        
        # 提取所有找到的列
        all_columns = [item['column'] for item in merged_results.values()]
        
        print(f"\n✅ 搜索完成！总计: {total_searched}, 找到: {total_found}, 未找到: {total_searched - total_found}")
        
        # 转换为返回格式
        return {
            'found': total_found == total_searched,
            'columns': self._convert_to_info_list(all_columns),
            'total_searched': total_searched,
            'total_found': total_found,
            'details': search_details
        }
    
    def _convert_to_info_list(self, columns: List[ColumnMetadata]) -> List[ColumnMetadataInfo]:
        """
        将 ColumnMetadata 对象列表转换为 ColumnMetadataInfo 对象列表
        
        Args:
            columns: ColumnMetadata 对象列表
        
        Returns:
            ColumnMetadataInfo 对象列表
        """
        result_list = []
        for column in columns:
            result_list.append(ColumnMetadataInfo(
                column_id=column.column_id,
                table_id=column.table_id,
                table_name=column.table_name,
                table_comment=column.table_comment,
                column_name=column.column_name,
                column_comment=column.column_comment,
                column_type=column.column_type,
                column_length=column.column_length,
                column_precision=column.column_precision,
                is_nullable=column.is_nullable,
                is_primary_key=column.is_primary_key,
                column_biz_type=column.column_biz_type,
                dim_id=column.dim_id,
                metric_id=column.metric_id,
                enabled=True
            ))
        
        return result_list


if __name__ == '__main__':
    # 测试 search_columns
    from apps.extend.utils.utils import DBUtils
    session = DBUtils.create_local_session()
    service = ColumnMetadataService(session)
    
    result = service.search_columns([
        {
            "table_name": "yz_datawarehouse_ads.ads_idx_female_wean28adjweight_month",
            "column_comments": ["校正断奶重", "断奶窝数"]
        },
        {
            "table_name": "yz_datawarehouse_ads.ads_pig_feed_detail",
            "column_comments": ["日期", "猪场", "栏位"]
        }
    ])
    
    print(f"\n搜索结果:{result}")
    print(f"  是否全部找到: {result['found']}")
    print(f"  搜索总数: {result['total_searched']}")
    print(f"  找到数量: {result['total_found']}")
    
    print(f"\n  详细匹配状态:")
    for detail in result['details']:
        status = "✅ 找到" if detail['found'] else "❌ 未找到"
        match_type_map = {
            'exact': '精准匹配',
            'fuzzy': '模糊匹配',
            'vector': '向量匹配'
        }
        match_type_str = match_type_map.get(detail['match_type'], '-') if detail['match_type'] else '-'
        
        # 显示查询项
        print(f"    {status} [{match_type_str}] - 表: {detail['table_name']}, 查询项: {detail['column_comment']}")
        
        # 如果找到，显示实际的字段信息
        if detail['found'] and detail['column_info']:
            actual_comment = detail['column_info'].column_comment or '(无注释)'
            print(f"           → 字段名: {detail['column_info'].column_name}, 列注释: {actual_comment}, 类型: {detail['column_info'].column_type}")
    
    print(f"\n  找到的列信息列表:")
    for col in result['columns']:
        print(f"    - 表: {col.table_name}, 字段: {col.column_name}, 注释: {col.column_comment}, 类型: {col.column_type}")